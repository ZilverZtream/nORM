using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// X1 regression — tracker AcceptChanges must fire under ambient non-owned paths
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Regression tests for X1: SaveChangesAsync must call AcceptChanges on tracked
/// entities whenever the DB writes committed independently — i.e., when the ambient
/// policy is Ignore (enlistment skipped) or BestEffort (enlistment failed, e.g.
/// SQLite which cannot participate in DTC TransactionScope).
///
/// Before the fix, entities remained in Added/Modified state after save in these
/// scenarios, permitting duplicate re-execution on the next SaveChanges call.
///
/// The "correctly enlisted" path (FailFast/BestEffort where enlistment succeeds)
/// should still defer AcceptChanges to the caller. For SQLite this path always
/// fails enlistment, so we test FailFast by observing the NormConfigurationException.
/// </summary>
public class AmbientTransactionTrackerTests
{
    [Table("TrackerAmbient")]
    private class TrackerAmbient
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Counter { get; set; }
    }

    private static (SqliteConnection cn, DbContext ctx) Build(
        AmbientTransactionEnlistmentPolicy policy = AmbientTransactionEnlistmentPolicy.BestEffort)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TrackerAmbient (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Counter INTEGER NOT NULL DEFAULT 0);";
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions { AmbientTransactionPolicy = policy };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    // ── Ignore policy: writes commit independently, tracker must accept ─────────

    [Fact]
    public async Task IgnorePolicy_AfterSave_EntityIsUnchanged()
    {
        var (cn, ctx) = Build(AmbientTransactionEnlistmentPolicy.Ignore);
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 99;

        // Act: SaveChanges inside ambient scope (Ignore = enlistment skipped).
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await ctx.SaveChangesAsync();
            // Deliberately do NOT call scope.Complete() — simulating a rollback scenario.
            // Under Ignore policy the write already committed independently regardless.
        }

        // X1 fix: entity must be Unchanged after save (writes went through independently).
        var entry = ctx.ChangeTracker.GetEntryOrDefault(rows[0]);
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Unchanged, entry!.State);
    }

    [Fact]
    public async Task IgnorePolicy_RepeatedSaveChanges_SecondCallIsNoOp()
    {
        var (cn, ctx) = Build(AmbientTransactionEnlistmentPolicy.Ignore);
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 77;

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            var affected1 = await ctx.SaveChangesAsync();
            Assert.Equal(1, affected1);

            // Second call in same ambient scope: no Modified/Added/Deleted entities remain.
            var affected2 = await ctx.SaveChangesAsync();
            Assert.Equal(0, affected2);
        }

        // Verify the DB row was updated exactly once (Counter=77, not 0 or 154).
        await using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT Counter FROM TrackerAmbient WHERE Id = @id";
        var p = verifyCmd.CreateParameter(); p.ParameterName = "@id"; p.Value = rows[0].Id;
        verifyCmd.Parameters.Add(p);
        Assert.Equal(77L, Convert.ToInt64(await verifyCmd.ExecuteScalarAsync()));
    }

    // ── BestEffort policy: SQLite can't enlist in DTC, so enlistment fails ──────

    [Fact]
    public async Task BestEffortPolicy_EnlistmentFails_AfterSave_EntityIsUnchanged()
    {
        // SQLite cannot participate in a DTC TransactionScope; EnlistTransaction throws.
        // Under BestEffort the exception is swallowed, the write commits independently.
        // X1 fix: tracker must accept the change despite the failed enlistment.
        var (cn, ctx) = Build(AmbientTransactionEnlistmentPolicy.BestEffort);
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 42;

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await ctx.SaveChangesAsync();
            // Do NOT complete — write was already committed independently by SQLite.
        }

        var entry = ctx.ChangeTracker.GetEntryOrDefault(rows[0]);
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Unchanged, entry!.State);
    }

    [Fact]
    public async Task BestEffortPolicy_RepeatedSaveChanges_SecondCallIsNoOp()
    {
        var (cn, ctx) = Build(AmbientTransactionEnlistmentPolicy.BestEffort);
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 55;

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            var first  = await ctx.SaveChangesAsync();
            var second = await ctx.SaveChangesAsync();
            Assert.Equal(1, first);
            Assert.Equal(0, second);
        }
    }

    // ── No ambient scope: baseline owned-transaction path ─────────────────────

    [Fact]
    public async Task NoAmbientScope_AfterSave_EntityIsUnchanged()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 11;

        await ctx.SaveChangesAsync();

        var entry = ctx.ChangeTracker.GetEntryOrDefault(rows[0]);
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Unchanged, entry!.State);
    }

    [Fact]
    public async Task NoAmbientScope_RepeatedSaveChanges_SecondCallIsNoOp()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 22;

        Assert.Equal(1, await ctx.SaveChangesAsync());
        Assert.Equal(0, await ctx.SaveChangesAsync());
    }

    // ── FailFast policy: SQLite can't enlist → NormConfigurationException ──────

    [Fact]
    public async Task FailFastPolicy_SqliteCannotEnlist_ThrowsNormConfigurationException()
    {
        var (cn, ctx) = Build(AmbientTransactionEnlistmentPolicy.FailFast);
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 1;

        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.NotNull(ex);
        Assert.IsType<NormConfigurationException>(ex);
    }

    // ── Explicit external transaction: caller controls, no AcceptChanges ───────

    [Fact]
    public async Task ExplicitExternalTransaction_AfterSave_EntityRemainsModified()
    {
        // When an explicit DbTransaction is passed (external), the caller controls commit.
        // SaveChanges must NOT call AcceptChanges; the entity should remain Modified
        // so that rollback leaves the tracker in the correct state.
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new TrackerAmbient { Name = "seed", Counter = 0 });
        var rows = (await ctx.Query<TrackerAmbient>().ToListAsync()).ToList();
        rows[0].Counter = 999;

        // Begin an explicit transaction that the test controls.
        var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.SaveChangesAsync();
        // Do NOT commit — roll back instead.
        await tx.RollbackAsync();

        // Entity must still be Modified because we rolled back.
        var entry = ctx.ChangeTracker.GetEntryOrDefault(rows[0]);
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Modified, entry!.State);
    }

    // ── Ignore policy: added entity ───────────────────────────────────────────

    [Fact]
    public async Task IgnorePolicy_AddedEntity_AfterSave_RemovedFromAdded()
    {
        // Entities in Added state should be transitioned to Unchanged (not remain Added).
        var (cn, ctx) = Build(AmbientTransactionEnlistmentPolicy.Ignore);
        await using var _ = ctx; using var __ = cn;

        var entity = new TrackerAmbient { Name = "new", Counter = 0 };
        ctx.ChangeTracker.Track(entity, EntityState.Added, ctx.GetMapping(typeof(TrackerAmbient)));

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await ctx.SaveChangesAsync();
        }

        var entry = ctx.ChangeTracker.GetEntryOrDefault(entity);
        // After save the entity should be Unchanged (no longer Added).
        Assert.NotNull(entry);
        Assert.Equal(EntityState.Unchanged, entry!.State);
    }
}
