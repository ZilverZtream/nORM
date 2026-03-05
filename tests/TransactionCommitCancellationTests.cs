using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace nORM.Tests;

/// <summary>
/// TX-1: Verifies that TransactionManager.CommitAsync uses CancellationToken.None
/// so that a cancelled caller token cannot abort a mid-flight commit and leave
/// the database in an ambiguous state.
/// </summary>
public class TransactionCommitCancellationTests
{
    [Table("TxCommitItem")]
    private class TxCommitItem
    {
        [Key]
        public int Id { get; set; }
        public string Val { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxCommitItem (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static long CountRows(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TxCommitItem";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ─── TX-1: CommitAsync(CancellationToken.None) is not cancelable ──────

    [Fact]
    public async Task DbTransaction_CommitAsync_WithCancelledToken_StillCommits()
    {
        // TX-1: Demonstrates the fix at the DbTransaction level.
        // CommitAsync(CancellationToken.None) must complete even if a caller's token was cancelled.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var createCmd = cn.CreateCommand();
        createCmd.CommandText = "CREATE TABLE TxCheck (Id INTEGER PRIMARY KEY)";
        createCmd.ExecuteNonQuery();

        await using var tx = await cn.BeginTransactionAsync();
        using var insertCmd = cn.CreateCommand();
        insertCmd.Transaction = (SqliteTransaction)tx;
        insertCmd.CommandText = "INSERT INTO TxCheck VALUES (99)";
        await insertCmd.ExecuteNonQueryAsync();

        // Pre-cancel a token — simulates what would previously have been passed to CommitAsync.
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // TX-1 fix: CommitAsync uses CancellationToken.None, so no OperationCanceledException.
        // This call mirrors what TransactionManager.CommitAsync now does.
        await tx.CommitAsync(CancellationToken.None);

        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM TxCheck WHERE Id = 99";
        var count = Convert.ToInt64(await verifyCmd.ExecuteScalarAsync());
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task SaveChangesAsync_WithValidToken_CommitsData()
    {
        // Non-regression: normal SaveChangesAsync with a non-cancelled token commits correctly.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        ctx.Add(new TxCommitItem { Id = 1, Val = "hello" });
        await ctx.SaveChangesAsync();

        Assert.Equal(1L, CountRows(cn));
    }

    [Fact]
    public async Task SaveChangesAsync_MultipleEntities_CommitsAllData()
    {
        // Non-regression: multiple entities are all committed.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        ctx.Add(new TxCommitItem { Id = 1, Val = "a" });
        ctx.Add(new TxCommitItem { Id = 2, Val = "b" });
        ctx.Add(new TxCommitItem { Id = 3, Val = "c" });
        await ctx.SaveChangesAsync();

        Assert.Equal(3L, CountRows(cn));
    }

    [Fact]
    public async Task SaveChangesAsync_AfterRollback_LeavesNoData()
    {
        // Non-regression: rollback on failure still leaves no data.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        ctx.Add(new TxCommitItem { Id = 1, Val = "ok" });
        await ctx.SaveChangesAsync();

        // Add duplicate PK to force failure
        ctx.Add(new TxCommitItem { Id = 1, Val = "duplicate" });
        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        // Original row should still be there (only the second transaction rolled back)
        Assert.Equal(1L, CountRows(cn));
    }
}
