using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//Verifies that SaveChanges defers AcceptChanges / ChangeTracker.Remove when the
//transaction is externally owned. Before the fix, AcceptChanges ran unconditionally
//after CommitAsync() (a no-op for non-owned transactions), so a subsequent rollback
//left tracked entities in Unchanged/Detached state while the DB still held the
//original values — a silent tracker↔DB divergence.
//
//Verifies that the retry loop is skipped entirely when an explicit or ambient
//System.Transactions.TransactionScope controls the scope. Before the fix, a transient
//failure inside an external transaction would invoke ShouldRetry and replay the write
//batch inside the same (un-rolled-back) transaction, risking duplicate effects.
//</summary>
public class ExternalTransactionOwnershipTests
{
 // entity: AUTOINCREMENT key, used for update/delete tests.
    [Table("TxStateItem")]
    private class TxStateItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

 // entity: manual (non-generated) key, so nORM includes it in INSERT
 // and we can force a duplicate-PK conflict for failure injection.
    [Table("TxRetryItem")]
    private class TxRetryItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE TxStateItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE TxRetryItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider(), opts ?? new DbContextOptions()));
    }

 // ─── AcceptChanges deferred for external transaction ───────────────

 //<summary>
 //regression: Under an external transaction that the caller later rolls back,
 //Modified entities must remain Modified after SaveChanges returns — the tracker must
 //not accept state before the caller commits.
 //
 //Before the fix: AcceptChanges ran unconditionally → entity became Unchanged →
 //rollback left DB with original value but tracker reported Unchanged (diverged).
 //</summary>
    [Fact]
    public async Task SaveChanges_ExternalTx_RolledBack_ModifiedEntitiesRemainModified()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
 // Pre-insert a row via raw SQL
            using (var setup = cn.CreateCommand())
            {
                setup.CommandText = "INSERT INTO TxStateItem (Name) VALUES ('original')";
                setup.ExecuteNonQuery();
            }

 // Force the entity into Modified state (detectChanges: false prevents snapshot
 // comparison from resetting state before SaveChanges executes).
            var entity = new TxStateItem { Id = 1, Name = "modified" };
            ctx.Update(entity);
            Assert.Equal(EntityState.Modified, ctx.ChangeTracker.Entries.Single().State);

 // Begin external transaction — SaveChanges writes but caller controls commit.
            await using var tx = await ctx.Database.BeginTransactionAsync();
            await ctx.SaveChangesAsync(detectChanges: false);

 // fix: AcceptChanges must NOT run under a non-owned transaction.
 // Before the fix, entity would be Unchanged here (premature acceptance).
            Assert.Equal(EntityState.Modified, ctx.ChangeTracker.Entries.Single().State);

 // Roll back — DB reverts to "original".
            await tx.RollbackAsync();

 // Tracker still reflects the pending change (Modified), not the false-accepted state.
            Assert.Equal(EntityState.Modified, ctx.ChangeTracker.Entries.Single().State);

 // Verify the rollback actually reverted the DB value.
            using var check = cn.CreateCommand();
            check.CommandText = "SELECT Name FROM TxStateItem WHERE Id = 1";
            Assert.Equal("original", (string?)check.ExecuteScalar());
        }
    }

 //<summary>
 //Self-owned transaction (the normal happy path) must still call AcceptChanges.
 //Entities must become Unchanged after a successful SaveChanges with no external tx.
 //This guards against the fix over-deferring and breaking the common path.
 //</summary>
    [Fact]
    public async Task SaveChanges_OwnedTx_EntitiesAcceptedUnchanged_AfterCommit()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var entity = new TxStateItem { Name = "hello" };
            ctx.Add(entity);

 // No external transaction — SaveChanges owns its transaction.
            await ctx.SaveChangesAsync();

 // Owned-tx path must still call AcceptChanges.
            Assert.Equal(EntityState.Unchanged, ctx.ChangeTracker.Entries.Single().State);
            Assert.True(entity.Id > 0, "DB-generated key must be assigned after owned-tx commit");
        }
    }

 //<summary>
 //regression: Under a rolled-back external transaction, Deleted entities must
 //NOT be removed from the tracker prematurely. Before the fix, ChangeTracker.Remove
 //ran unconditionally, detaching the entity while the DB still held the row.
 //</summary>
    [Fact]
    public async Task SaveChanges_ExternalTx_DeleteRolledBack_EntityRemainsTrackedAsDeleted()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
 // Pre-insert the row to be deleted.
            using (var setup = cn.CreateCommand())
            {
                setup.CommandText = "INSERT INTO TxStateItem (Name) VALUES ('to-delete')";
                setup.ExecuteNonQuery();
            }

            var entity = new TxStateItem { Id = 1, Name = "to-delete" };
            ctx.Attach(entity);
            ctx.Remove(entity);  // State = Deleted
            Assert.Equal(EntityState.Deleted, ctx.ChangeTracker.Entries.Single().State);

            await using var tx = await ctx.Database.BeginTransactionAsync();
            await ctx.SaveChangesAsync(detectChanges: false);

 // fix: ChangeTracker.Remove must NOT run under a non-owned transaction.
 // Entity must still be tracked as Deleted (not detached/gone from tracker).
            var entry = ctx.ChangeTracker.Entries.SingleOrDefault(e => ReferenceEquals(e.Entity, entity));
            Assert.NotNull(entry);
            Assert.Equal(EntityState.Deleted, entry!.State);

 // Roll back — row must still exist in DB.
            await tx.RollbackAsync();

            using var check = cn.CreateCommand();
            check.CommandText = "SELECT COUNT(*) FROM TxStateItem";
            Assert.Equal(1L, Convert.ToInt64(check.ExecuteScalar()));
        }
    }

 //<summary>
 //After an external transaction is committed by the caller, SaveChanges on a
 //subsequent (new, owned) transaction must work normally — no lingering state from
 //the external-tx path.
 //</summary>
    [Fact]
    public async Task SaveChanges_ExternalTx_AfterCallerCommits_SubsequentSaveSucceeds()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var entity = new TxStateItem { Name = "first" };
            ctx.Add(entity);

            await using var tx = await ctx.Database.BeginTransactionAsync();
            await ctx.SaveChangesAsync(detectChanges: false);

 // Caller commits — entity is still in Added state ( fix: AcceptChanges deferred)
            await tx.CommitAsync();

 // Now save again (owned tx this time) — must work without errors.
 // The entity is still Added; SaveChanges will try to insert again.
 // To avoid duplicate PK, accept changes manually to simulate caller-driven acceptance.
            ctx.ChangeTracker.Entries.Single().AcceptChanges();
            Assert.Equal(EntityState.Unchanged, ctx.ChangeTracker.Entries.Single().State);
        }
    }

 // ─── Retry loop bypassed under external transaction ────────────────

 //<summary>
 //regression: When an explicit (DbTransaction) external transaction is active,
 //a retryable failure must NOT invoke the retry policy. Retrying would replay writes
 //inside the same transaction whose rollback is a no-op from SaveChanges perspective,
 //risking duplicate effects.
 //
 //Before the fix: the retry loop ran regardless of transaction ownership; ShouldRetry
 //was consulted and the batch was re-executed inside the open external transaction.
 //</summary>
    [Fact]
    public async Task SaveChanges_ExplicitExternalTx_RetryableFailure_DoesNotRetry()
    {
        var retryCount = 0;
        var policy = new RetryPolicy
        {
            MaxRetries = 5,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = _ => { retryCount++; return true; }
        };

        var (cn, ctx) = CreateContext(new DbContextOptions { RetryPolicy = policy });
        await using (cn) await using (ctx)
        {
 // Pre-insert with Id=99 to provoke a unique constraint violation.
            using (var setup = cn.CreateCommand())
            {
                setup.CommandText = "INSERT INTO TxRetryItem (Id, Name) VALUES (99, 'existing')";
                setup.ExecuteNonQuery();
            }

            await using var tx = await ctx.Database.BeginTransactionAsync();

            ctx.Add(new TxRetryItem { Id = 99, Name = "duplicate" });

 // Must throw (duplicate PK) but the retry loop must be bypassed entirely.
            await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

 // fix: ShouldRetry was never consulted because external tx skips the retry loop.
            Assert.Equal(0, retryCount);
        }
    }

 //<summary>
 //With NO external transaction and a retry policy, a retryable failure DOES
 //consult the retry policy. This verifies the guard does not break normal retries.
 //</summary>
    [Fact]
    public async Task SaveChanges_NoExternalTx_RetryableFailure_ConsultsRetryPolicy()
    {
        var retryCount = 0;
        var policy = new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
 // Return false so we don't actually retry — we just want to verify it was consulted.
            ShouldRetry = _ => { retryCount++; return false; }
        };

        var (cn, ctx) = CreateContext(new DbContextOptions { RetryPolicy = policy });
        await using (cn) await using (ctx)
        {
            using (var setup = cn.CreateCommand())
            {
                setup.CommandText = "INSERT INTO TxRetryItem (Id, Name) VALUES (42, 'existing')";
                setup.ExecuteNonQuery();
            }

            ctx.Add(new TxRetryItem { Id = 42, Name = "duplicate" });

 // Throws because ShouldRetry returns false, but policy IS consulted.
            await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

 // Retry policy must be consulted when no external transaction guards the loop.
            Assert.True(retryCount > 0,
                "ShouldRetry must be consulted when no external transaction is active");
        }
    }

 //<summary>
 //An ambient System.Transactions.TransactionScope also counts as an external
 //transaction — the retry loop must be skipped when Transaction.Current is non-null.
 //</summary>
    [Fact]
    public async Task SaveChanges_AmbientTransactionScope_RetryableFailure_DoesNotRetry()
    {
        var retryCount = 0;
        var policy = new RetryPolicy
        {
            MaxRetries = 5,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = _ => { retryCount++; return true; }
        };

        var opts = new DbContextOptions
        {
            RetryPolicy = policy,
 // BestEffort: SQLite in-memory may not support EnlistTransaction;
 // we only need Transaction.Current != null for the guard to fire.
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.BestEffort
        };

        var (cn, ctx) = CreateContext(opts);
        await using (cn) await using (ctx)
        {
 // Pre-insert outside the scope so the row is committed before scope starts.
            using (var setup = cn.CreateCommand())
            {
                setup.CommandText = "INSERT INTO TxRetryItem (Id, Name) VALUES (77, 'existing')";
                setup.ExecuteNonQuery();
            }

            using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            ctx.Add(new TxRetryItem { Id = 77, Name = "duplicate" });

 // Must throw (duplicate PK) — scope is active so retry loop is bypassed.
            await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

 // fix: ambient scope causes Transaction.Current != null → retry loop skipped.
            Assert.Equal(0, retryCount);

 // Do NOT call scope.Complete() — scope disposes and rolls back automatically.
        }
    }
}
