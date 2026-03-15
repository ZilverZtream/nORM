using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that a pre-canceled CancellationToken causes OperationCanceledException across
/// all major query types on both sync-execution (SQLite) and async-execution provider variants.
/// </summary>
public class QueryCancellationMatrixTests
{
    [Table("QcmItem")]
    private sealed class QcmItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Active { get; set; }
    }

    // ── SQLite sync provider (PrefersSyncExecution = true) ────────────────

    [Fact]
    public async Task ToListAsync_SyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().ToListAsync(cts.Token));
    }

    [Fact]
    public async Task CountAsync_SyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().CountAsync(cts.Token));
    }

    [Fact]
    public async Task CountAsync_WithPredicate_SyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().Where(x => x.Active).CountAsync(cts.Token));
    }

    [Fact]
    public async Task WhereToListAsync_SyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().Where(x => x.Id == 1).ToListAsync(cts.Token));
    }

    [Fact]
    public async Task TakeToListAsync_SyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().Take(2).ToListAsync(cts.Token));
    }

    // ── Async provider (PrefersSyncExecution = false) ─────────────────────

    [Fact]
    public async Task ToListAsync_AsyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().ToListAsync(cts.Token));
    }

    [Fact]
    public async Task CountAsync_AsyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().CountAsync(cts.Token));
    }

    [Fact]
    public async Task WhereToListAsync_AsyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().Where(x => x.Id == 1).ToListAsync(cts.Token));
    }

    [Fact]
    public async Task TakeToListAsync_AsyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().Take(2).ToListAsync(cts.Token));
    }

    // ── Retry + cancellation interaction ─────────────────────────────────

    /// <summary>
    /// A pre-canceled token on a non-fast-path query (predicate that falls back to the
    /// full query translator) must still throw OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task ComplexWhere_SyncProvider_PreCanceled_Throws()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Use a StartsWith predicate to force the full query translator path
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<QcmItem>().Where(x => x.Name.StartsWith("O")).ToListAsync(cts.Token));
    }

    /// <summary>
    /// A non-canceled token that gets canceled AFTER a successful operation must not
    /// affect the already-completed result.
    /// </summary>
    [Fact]
    public async Task CanceledAfterSuccess_DoesNotAffectCompletedResult()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();

        // Not canceled yet
        var count = await ctx.Query<QcmItem>().CountAsync(cts.Token);

        // Cancel after completion
        cts.Cancel();

        // The result is already materialized; cancel after the fact is a no-op
        Assert.Equal(3, count);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE QcmItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Active INTEGER NOT NULL);" +
            "INSERT INTO QcmItem VALUES (1, 'One', 1);" +
            "INSERT INTO QcmItem VALUES (2, 'Two', 0);" +
            "INSERT INTO QcmItem VALUES (3, 'Three', 1);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }
}
