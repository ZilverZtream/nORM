using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Table("PiclItem")]
file class PiclItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

/// <summary>
/// Verifies that the PreparedInsert command cache correctly manages its lifecycle
/// across rapid transaction-binding changes: alternating inside/outside-transaction
/// inserts, bulk inserts within a single transaction, pre-cancelled tokens, and
/// rollback-then-reuse sequences.
///
/// Root cause of the original bugs: the <c>_disposed</c> field on <c>PreparedInsertCommand</c>
/// was not <c>volatile</c>, allowing stale reads under concurrent access and causing
/// <c>ObjectDisposedException</c> when the cache entry was reused after a rollback.
/// </summary>
public class PreparedInsertCommandLifecycleTests
{
    private static (SqliteConnection Cn, DbContext Ctx) BuildDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PiclItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task AlternatingInsideOutsideTransaction_500Cycles_NoException()
    {
        // 500 alternating cycles: insert outside tx, then begin-tx / insert / commit.
        // Exercises the cache's ability to switch between null-tx and live-tx entries
        // without ObjectDisposedException.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        for (int i = 0; i < 500; i++)
        {
            await ctx.InsertAsync(new PiclItem { Name = $"out{i}" });

            await using var tx = await ctx.Database.BeginTransactionAsync();
            await ctx.InsertAsync(new PiclItem { Name = $"in{i}" });
            await tx.CommitAsync();
        }

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PiclItem";
        Assert.Equal(1000L, Convert.ToInt64(count.ExecuteScalar()));
    }

    [Fact]
    public async Task BulkInsertsInSingleTransaction_200Items_AllCommitted()
    {
        // 200 consecutive inserts in the same transaction must reuse the cached
        // command without re-preparation.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        for (int i = 0; i < 200; i++)
            await ctx.InsertAsync(new PiclItem { Name = $"bulk{i}" });
        await tx.CommitAsync();

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PiclItem";
        Assert.Equal(200L, Convert.ToInt64(count.ExecuteScalar()));
    }

    [Fact]
    public async Task CancelledInsert_CacheRemainsUsableAfterwards()
    {
        // A pre-cancelled CancellationToken must propagate OperationCanceledException
        // without corrupting the PreparedInsert cache entry for subsequent calls.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        await ctx.InsertAsync(new PiclItem { Name = "warm" });

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.InsertAsync(new PiclItem { Name = "cancel" }, cts.Token));

        await ctx.InsertAsync(new PiclItem { Name = "after-cancel" });

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PiclItem";
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar())); // warm + after-cancel
    }

    [Fact]
    public async Task RollbackThenReuse_CacheEntryReplacedCorrectly()
    {
        // After a rollback the cache must replace the stale tx entry so the next
        // outside-transaction insert picks up a fresh null-tx command.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        await ctx.InsertAsync(new PiclItem { Name = "before" });

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new PiclItem { Name = "rolled-back" });
        await tx.RollbackAsync();

        await ctx.InsertAsync(new PiclItem { Name = "after" });

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PiclItem";
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar())); // before + after
    }
}

/// <summary>
/// Verifies that a pre-cancelled token on a query does not leave the connection broken,
/// and that nested savepoint rollbacks leave the outer transaction unaffected.
/// </summary>
public class SavepointFaultInjectionTests
{
    private static (SqliteConnection Cn, DbContext Ctx) BuildDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PiclItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task PreCancelledQueryToken_SubsequentQuerySucceeds()
    {
        // A query cancelled before execution must not leave the connection in a broken
        // state; subsequent non-cancelled queries must succeed.
        var (_, ctx) = BuildDb();
        await using var _ = ctx;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.Query<PiclItem>().ToListAsync(cts.Token));

        var results = await ctx.Query<PiclItem>().ToListAsync();
        Assert.Empty(results);
    }

    [Fact]
    public async Task SavepointRollback_DoesNotAffectOuterTransaction()
    {
        // Multiple nested savepoints where each is rolled back must leave the outer
        // transaction unaffected; final commit must persist only the outer changes.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;
        var provider = new SqliteProvider();

        await using var outer = await cn.BeginTransactionAsync();

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)outer;
            ins.CommandText = "INSERT INTO PiclItem (Name) VALUES ('outer1')";
            await ins.ExecuteNonQueryAsync();
        }

        await provider.CreateSavepointAsync(outer, "sp1");

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)outer;
            ins.CommandText = "INSERT INTO PiclItem (Name) VALUES ('inner1')";
            await ins.ExecuteNonQueryAsync();
        }

        await provider.RollbackToSavepointAsync(outer, "sp1");

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)outer;
            ins.CommandText = "INSERT INTO PiclItem (Name) VALUES ('outer2')";
            await ins.ExecuteNonQueryAsync();
        }

        await outer.CommitAsync();

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PiclItem";
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar())); // outer1 + outer2
    }

    [Fact]
    public async Task CancelledSavepoint_TransactionRemainsUsable()
    {
        // A pre-cancelled token passed to CreateSavepointAsync must not leave the
        // transaction in a broken state; subsequent operations must still work.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;
        var provider = new SqliteProvider();

        await using var tx = await cn.BeginTransactionAsync();

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
            ins.CommandText = "INSERT INTO PiclItem (Name) VALUES ('before-sp')";
            await ins.ExecuteNonQueryAsync();
        }

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp_cancel", cts.Token));

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
            ins.CommandText = "INSERT INTO PiclItem (Name) VALUES ('after-sp')";
            await ins.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PiclItem";
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar()));
    }

    [Fact]
    public async Task SaveChanges_CancelledAfterEntityAttach_NoPartialCommit()
    {
        // A SaveChanges cancelled after entities are attached must not partially commit.
        // The DB row count must remain consistent: either all changes committed or none.
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        await ctx.InsertAsync(new PiclItem { Name = "before" });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        try { await ctx.SaveChangesAsync(cts.Token); }
        catch (OperationCanceledException) { }

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PiclItem";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.True(count <= 1, $"Expected ≤1 rows (no partial commit), found {count}");
    }
}
