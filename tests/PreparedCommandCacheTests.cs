using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that the PreparedInsertCommand cache handles transaction-binding changes correctly:
/// when the active transaction changes, the old cached command is replaced without leaking
/// disposed commands to active callers, and no ObjectDisposedException is thrown in any
/// sequential usage pattern.
/// </summary>
public class PreparedCommandCacheTests
{
    [Table("PccItem")]
    private class PccItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection cn, DbContext ctx) BuildContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PccItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    /// <summary>
    /// Sequential insert without a transaction: the prepared command is cached on first use
    /// and reused on second use — no exception.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_NoTransaction_CachedCommandReused()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;

        var a = new PccItem { Name = "a" };
        var b = new PccItem { Name = "b" };

        await ctx.InsertAsync(a);
        await ctx.InsertAsync(b);

        Assert.True(a.Id > 0);
        Assert.True(b.Id > 0);
    }

    /// <summary>
    /// Insert within a transaction: the cache entry is replaced (old null-tx command disposed,
    /// new tx-bound command created). No ObjectDisposedException in sequential usage.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_TransactionChange_CacheReplacedWithoutException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;

        // First insert: no transaction → caches a null-tx prepared command.
        var a = new PccItem { Name = "a" };
        await ctx.InsertAsync(a);

        // Second insert: within a transaction → cache entry is replaced.
        await using var tx = await ctx.Database.BeginTransactionAsync();
        var b = new PccItem { Name = "b" };
        await ctx.InsertAsync(b);
        await tx.CommitAsync();

        // Third insert: back to no transaction → cache is replaced again.
        var c = new PccItem { Name = "c" };
        await ctx.InsertAsync(c);

        Assert.True(a.Id > 0);
        Assert.True(b.Id > 0);
        Assert.True(c.Id > 0);
    }

    /// <summary>
    /// Multiple transaction starts/commits with inserts between each: the cache correctly
    /// recycles command entries for each transition without leaking or throwing.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_MultipleTransactionCycles_NoException()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;

        for (int i = 0; i < 5; i++)
        {
            // Insert outside transaction.
            await ctx.InsertAsync(new PccItem { Name = $"out{i}" });

            // Insert inside transaction.
            await using var tx = await ctx.Database.BeginTransactionAsync();
            await ctx.InsertAsync(new PccItem { Name = $"in{i}" });
            await tx.CommitAsync();
        }

        // Verify all rows were inserted.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PccItem";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(10, count);
    }

    /// <summary>
    /// Two consecutive inserts inside the same transaction reuse the same cached tx-bound
    /// command — no re-preparation needed.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_SameTransaction_CachedCommandReused()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new PccItem { Name = "x" });
        await ctx.InsertAsync(new PccItem { Name = "y" });
        await tx.CommitAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PccItem";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(2, count);
    }

    /// <summary>
    /// Disposing the context after inserts must cleanly release all cached prepared commands
    /// without leaking or throwing during cleanup.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_ContextDispose_CacheCleanedWithoutException()
    {
        var (cn, ctx) = BuildContext();

        // Insert enough to ensure the prepared command is cached.
        await ctx.InsertAsync(new PccItem { Name = "first" });
        await ctx.InsertAsync(new PccItem { Name = "second" });

        // Verify rows before dispose (connection still open).
        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM PccItem";
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar()));

        // Disposing the context must not throw even with a cached prepared command.
        await ctx.DisposeAsync();
        cn.Dispose();
    }
}
