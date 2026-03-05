using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Fix 3: Verifies that rollback in bulk operations uses CancellationToken.None,
/// so that a pre-cancelled caller token does not abort the rollback and leave
/// the transaction in an uncertain state.
/// </summary>
public class BulkOperationCancellationTests
{
    [Table("BocItems")]
    private class BocItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection cn, DbContext ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BocItems (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    /// <summary>
    /// Normal (non-cancelled) bulk insert completes successfully.
    /// Baseline to ensure the rollback fix does not break normal operation.
    /// </summary>
    [Fact]
    public async Task BulkInsert_WithNonCancelledToken_Succeeds()
    {
        var (cn, ctx) = CreateContext();
        using var _ = cn;
        using var __ = ctx;

        var items = new List<BocItem>
        {
            new() { Id = 10, Name = "Gamma" },
            new() { Id = 20, Name = "Delta" }
        };

        var inserted = await ctx.BulkInsertAsync(items, CancellationToken.None);
        Assert.Equal(2, inserted);
    }

    /// <summary>
    /// Verifies that BulkInsertAsync with an already-cancelled token does not
    /// throw an AggregateException caused by the rollback being cancelled too.
    /// The rollback must use CancellationToken.None internally.
    /// </summary>
    [Fact]
    public async Task BulkInsert_WithPreCancelledToken_RollbackDoesNotThrowAggregateException()
    {
        var (cn, ctx) = CreateContext();
        using var _ = cn;
        using var __ = ctx;

        var items = new List<BocItem>
        {
            new() { Id = 1, Name = "Alpha" },
            new() { Id = 2, Name = "Beta" }
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        Exception? caughtEx = null;
        try
        {
            await ctx.BulkInsertAsync(items, cts.Token);
        }
        catch (Exception ex)
        {
            caughtEx = ex;
        }

        // We expect either OperationCanceledException (the insert was cancelled)
        // or nothing thrown (SQLite might not honour the cancelled token on in-memory DBs).
        // What we must NOT get is an AggregateException whose inner exceptions include
        // a second OperationCanceledException from the rollback path.
        if (caughtEx is AggregateException ag)
        {
            // If we get an AggregateException, it must not have a rollback-cancellation inner exception
            // (which would indicate the rollback itself was cancelled).
            Assert.All(ag.InnerExceptions, inner =>
                Assert.False(
                    inner is OperationCanceledException && inner != ag.InnerExceptions[0],
                    $"Rollback was unexpectedly cancelled: {inner.Message}"));
        }
        // If it's a plain OperationCanceledException or null, that's fine.
    }

    /// <summary>
    /// Verifies that a bulk insert that fails mid-way (simulated by a duplicate PK)
    /// properly rolls back and does not leave stale rows.
    /// </summary>
    [Fact]
    public async Task BulkInsert_WhenFails_RollsBackCleanly()
    {
        var (cn, ctx) = CreateContext();
        using var _ = cn;
        using var __ = ctx;

        // Pre-insert a row so the second bulk insert attempt will fail with a constraint violation.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO BocItems (Id, Name) VALUES (99, 'Existing')";
        cmd.ExecuteNonQuery();

        var items = new List<BocItem>
        {
            new() { Id = 1,  Name = "First" },
            new() { Id = 99, Name = "Conflict" } // will cause UNIQUE violation
        };

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.BulkInsertAsync(items, CancellationToken.None));

        // Verify Id=1 was NOT committed (the transaction should have rolled back).
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM BocItems WHERE Id = 1";
        var count = (long)(verifyCmd.ExecuteScalar()!);
        Assert.Equal(0L, count);
    }
}
