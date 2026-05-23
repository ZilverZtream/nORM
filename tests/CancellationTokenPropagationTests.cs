using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that a pre-cancelled CancellationToken causes OperationCanceledException
/// (not a hang) for the primary async entry points: query execution, SaveChangesAsync,
/// and BulkInsertAsync.
///
/// The token must be propagated — not silently swallowed, not treated as default,
/// and not result in a hung or indefinitely-blocked call.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class CancellationTokenPropagationTests
{
    [Table("CtpEntity")]
    private class CtpEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) BuildContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CtpEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static CancellationToken PreCancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts.Token;
    }

    private static void AssertCancelled(Exception? ex)
    {
        Assert.NotNull(ex);
        Assert.True(
            ex is OperationCanceledException ||
            (ex is AggregateException agg && agg.InnerException is OperationCanceledException),
            $"Expected OperationCanceledException but got {ex?.GetType().Name}: {ex?.Message}");
    }

    // ── Query path ────────────────────────────────────────────────────────────

    /// <summary>
    /// ToListAsync with a pre-cancelled token must throw OperationCanceledException
    /// before executing any database I/O.
    /// </summary>
    [Fact]
    public async Task QueryAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<CtpEntity>().ToListAsync(PreCancelled()));

        AssertCancelled(ex);
    }

    /// <summary>
    /// CountAsync with a pre-cancelled token must throw OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task CountAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<CtpEntity>().CountAsync(PreCancelled()));

        AssertCancelled(ex);
    }

    /// <summary>
    /// FirstOrDefaultAsync with a pre-cancelled token must throw OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task FirstOrDefaultAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<CtpEntity>().FirstOrDefaultAsync(ct: PreCancelled()));

        AssertCancelled(ex);
    }

    // ── SaveChangesAsync ──────────────────────────────────────────────────────

    /// <summary>
    /// SaveChangesAsync with a pre-cancelled token must throw OperationCanceledException.
    /// No data should be persisted.
    /// </summary>
    [Fact]
    public async Task SaveChangesAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        ctx.Add(new CtpEntity { Value = "should-not-persist" });

        var ex = await Record.ExceptionAsync(() =>
            ctx.SaveChangesAsync(PreCancelled()));

        AssertCancelled(ex);

        // Verify no data was written.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM CtpEntity";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(0, count);
    }

    // ── BulkInsertAsync ───────────────────────────────────────────────────────

    /// <summary>
    /// BulkInsertAsync with a pre-cancelled token must throw OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task BulkInsertAsync_PreCancelledToken_ThrowsOperationCanceledException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        var entities = Enumerable.Range(0, 5)
            .Select(i => new CtpEntity { Value = $"bulk-{i}" })
            .ToList();

        var ex = await Record.ExceptionAsync(() =>
            ctx.BulkInsertAsync(entities, PreCancelled()));

        AssertCancelled(ex);
    }

    // ── Connection remains usable after cancellation ──────────────────────────

    /// <summary>
    /// After a cancelled query, the context connection must remain open and
    /// usable for subsequent operations.
    /// </summary>
    [Fact]
    public async Task AfterCancelledQuery_ConnectionRemainsUsable()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        // Seed a row.
        await ctx.InsertAsync(new CtpEntity { Value = "seed" });

        // Cancel a query.
        try
        {
            await ctx.Query<CtpEntity>().ToListAsync(PreCancelled());
        }
        catch (OperationCanceledException) { }

        // Subsequent query must succeed.
        var rows = await ctx.Query<CtpEntity>().ToListAsync();
        Assert.Single(rows);
    }

    /// <summary>
    /// After a cancelled SaveChangesAsync, the context must remain usable
    /// and can successfully save new data in a subsequent call.
    /// </summary>
    [Fact]
    public async Task AfterCancelledSave_ContextRemainsUsable()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;
        using var __ = cn;

        ctx.Add(new CtpEntity { Value = "cancelled" });
        try { await ctx.SaveChangesAsync(PreCancelled()); } catch (OperationCanceledException) { }

        ctx.Add(new CtpEntity { Value = "committed" });
        await ctx.SaveChangesAsync();

        var rows = await ctx.Query<CtpEntity>().ToListAsync();
        Assert.Contains(rows, r => r.Value == "committed");
    }
}
