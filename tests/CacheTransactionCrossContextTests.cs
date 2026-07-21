using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Result-cache correctness across the transaction boundary, beyond the single-context rollback contract:
/// a Cacheable read taken INSIDE a transaction that observed uncommitted rows must, after the transaction
/// COMMITS, serve the now-committed state; and with a cache SHARED across two contexts, one context's
/// uncommitted-in-transaction Cacheable read must never leak to another context — a rolled-back write may
/// not reach a second reader through the shared cache. Both are checked against raw-SQL truth.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CacheTransactionCrossContextTests
{
    [Table("CtxCacheRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static string SharedCs() => $"Data Source=file:ctxc_{Guid.NewGuid():N}?mode=memory&cache=shared";

    private static SqliteConnection Seed(string cs)
    {
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "CREATE TABLE CtxCacheRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL); INSERT INTO CtxCacheRow VALUES (1,10);";
        cmd.ExecuteNonQuery();
        return keeper;
    }

    private static DbContext MakeCtx(string cs, NormMemoryCacheProvider cache)
    {
        var cn = new SqliteConnection(cs);
        cn.Open();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            CacheProvider = cache
        });
    }

    private static async Task<int> CacheableCountAsync(DbContext ctx) =>
        (await ((INormQueryable<Row>)ctx.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync()).Count;

    [Fact]
    public async Task Cacheable_read_taken_inside_a_transaction_then_committed_serves_the_committed_state()
    {
        var cs = SharedCs();
        using var keeper = Seed(cs);
        using var cache = new NormMemoryCacheProvider();
        await using var ctx = MakeCtx(cs, cache);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new Row { Id = 2, V = 20 });
            await ctx.SaveChangesAsync();
            _ = await CacheableCountAsync(ctx);   // reads its own uncommitted rows
            await tx.CommitAsync();
        }

        Assert.Equal(2, await CacheableCountAsync(ctx));
    }

    [Fact]
    public async Task Uncommitted_in_transaction_cacheable_read_does_not_leak_to_another_context_via_a_shared_cache()
    {
        var cs = SharedCs();
        using var keeper = Seed(cs);
        using var cache = new NormMemoryCacheProvider();   // SHARED across both contexts
        await using var ctxA = MakeCtx(cs, cache);
        await using var ctxB = MakeCtx(cs, cache);

        await using (var tx = await ctxA.Database.BeginTransactionAsync())
        {
            ctxA.Add(new Row { Id = 2, V = 20 });
            await ctxA.SaveChangesAsync();
            Assert.Equal(2, await CacheableCountAsync(ctxA));   // ctxA sees its own uncommitted row
            await tx.RollbackAsync();
        }

        // A second context reading the same cacheable query must see only committed state.
        Assert.Equal(1, await CacheableCountAsync(ctxB));
    }
}
