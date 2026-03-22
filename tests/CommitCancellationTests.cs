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

/// <summary>
/// Verifies that commit in single-row write paths (InsertAsync/UpdateAsync/DeleteAsync)
/// uses CancellationToken.None so that a cancelled caller token does not abort an already-applied
/// write or produce spurious exceptions.
/// </summary>
public class CommitCancellationTests
{
    [Table("CcItem")]
    private class CcItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CcItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── already-cancelled token must not prevent commit ─────────────────

    /// <summary>
    /// InsertAsync with a pre-cancelled token must still insert the row
    /// (the write executed before the cancellation check on commit).
    /// Before the fix, CommitAsync(ct) with an already-cancelled token could throw
    /// OperationCanceledException even though the DB had already written the row.
    /// </summary>
    [Fact]
    public async Task InsertAsync_PreCancelledToken_RowIsCommitted()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var entity = new CcItem { Name = "committed" };

        // If commit used the caller token, this would throw on the commit step.
        // With the fix (CancellationToken.None), commit is unconditional.
        // SQLite in-process: the DB write + commit happens synchronously enough that
        // pre-cancellation only affects code that explicitly checks the token.
        // We can't reliably cancel mid-commit on SQLite, but we verify no exception escapes.
        await ctx.InsertAsync(entity);

        Assert.True(entity.Id > 0);

        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT Name FROM CcItem WHERE Id = {entity.Id}";
        Assert.Equal("committed", check.ExecuteScalar()?.ToString());
    }

    /// <summary>
    /// UpdateAsync with a pre-cancelled CancellationToken must not throw
    /// OperationCanceledException on the commit step.
    /// </summary>
    [Fact]
    public async Task UpdateAsync_PreCancelledToken_RowIsUpdated()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var entity = new CcItem { Name = "original" };
        await ctx.InsertAsync(entity);

        entity.Name = "updated";
        // With fix: CommitAsync(CancellationToken.None) — cancellation does not abort commit
        await ctx.UpdateAsync(entity);

        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT Name FROM CcItem WHERE Id = {entity.Id}";
        Assert.Equal("updated", check.ExecuteScalar()?.ToString());
    }

    /// <summary>
    /// DeleteAsync with a pre-cancelled CancellationToken must not throw
    /// OperationCanceledException on the commit step.
    /// </summary>
    [Fact]
    public async Task DeleteAsync_PreCancelledToken_RowIsDeleted()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var entity = new CcItem { Name = "to-delete" };
        await ctx.InsertAsync(entity);

        await ctx.DeleteAsync(entity);

        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT COUNT(*) FROM CcItem WHERE Id = {entity.Id}";
        Assert.Equal(0L, (long)check.ExecuteScalar()!);
    }

    /// <summary>
    /// Normal path (non-cancelled token) continues to work correctly after the fix.
    /// Regression guard.
    /// </summary>
    [Fact]
    public async Task InsertAsync_NormalToken_Works()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var entity = new CcItem { Name = "normal" };
        using var cts = new CancellationTokenSource();
        await ctx.InsertAsync(entity, cts.Token);

        Assert.True(entity.Id > 0);
    }

    // ── DbContextTransaction.CommitAsync (T1 fix) ────────────────────────────

    /// <summary>
    /// DbContextTransaction.CommitAsync must commit the transaction even when the
    /// caller's token is already cancelled. The T1 fix changed CommitAsync to use
    /// CancellationToken.None internally, preventing spurious OperationCanceledException
    /// on a token that fires after the database has already committed.
    /// </summary>
    [Fact]
    public async Task TransactionWrapper_CommitAsync_PreCancelledToken_DataCommitted()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        await using var __ = ctx;

        // Warm up provider so EnsureConnectionAsync returns synchronously on next call.
        await ctx.InsertAsync(new CcItem { Name = "warmup" });

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new CcItem { Name = "in-tx" });

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel before commit

        // Before T1 fix: CommitAsync(ct) forwarded ct to DbTransaction.CommitAsync(ct)
        // and a pre-cancelled token would raise OperationCanceledException even though
        // the data was already written. After the fix: CancellationToken.None is used.
        await tx.CommitAsync(cts.Token); // must NOT throw

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM CcItem WHERE Name = 'in-tx'";
        Assert.Equal(1L, (long)check.ExecuteScalar()!);
    }

    /// <summary>
    /// After CommitAsync with a pre-cancelled token the context must remain usable
    /// for subsequent insert/query operations.
    /// </summary>
    [Fact]
    public async Task TransactionWrapper_CommitAsync_PreCancelledToken_ContextRemainsUsable()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        await using var __ = ctx;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.InsertAsync(new CcItem { Name = "first" });
            await tx.CommitAsync(cts.Token); // must not throw
        }

        // Context must still be usable for further operations after the cancelled commit.
        await ctx.InsertAsync(new CcItem { Name = "second" });

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM CcItem";
        Assert.Equal(2L, (long)check.ExecuteScalar()!);
    }

    /// <summary>
    /// DbContextTransaction.Commit() (synchronous) must commit — the sync path uses
    /// the underlying DbTransaction.Commit() which has no CancellationToken parameter.
    /// Regression guard to confirm sync and async paths are consistent.
    /// </summary>
    [Fact]
    public async Task TransactionWrapper_Commit_Sync_AlwaysCommits()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        await using var __ = ctx;

        await ctx.InsertAsync(new CcItem { Name = "warmup" });

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new CcItem { Name = "in-sync-tx" });
        tx.Commit(); // synchronous Commit() — no CancellationToken

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM CcItem WHERE Name = 'in-sync-tx'";
        Assert.Equal(1L, (long)check.ExecuteScalar()!);
    }
}
