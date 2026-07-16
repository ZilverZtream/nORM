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
/// Contract: every nORM write route invalidates a cached result individually (caching matrix
/// cell). A Cacheable list is populated, one specific write API runs, and the SAME Cacheable
/// query must reflect the write - per route, so a future route added without invalidation
/// wiring fails here instead of serving stale data. Raw ADO SQL bypasses nORM entirely and
/// cannot invalidate; that boundary is a documented design exception on the ticket.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CacheWriteRouteInvalidationContractTests
{
    [Table("CacheRoute_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx, NormMemoryCacheProvider Cache) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CacheRoute_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);" +
                "INSERT INTO CacheRoute_Row VALUES (1, 10), (2, 20);";
            cmd.ExecuteNonQuery();
        }
        var cache = new NormMemoryCacheProvider();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            CacheProvider = cache
        });
        return (cn, ctx, cache);
    }

    // AsNoTracking: set-based and bulk writes bypass the change tracker (EF-identical), so a
    // TRACKED re-read would return the stale tracked instances regardless of the cache. The
    // cache-invalidation contract is asserted through untracked reads.
    private static Task<System.Collections.Generic.List<Row>> CachedAll(DbContext ctx)
        => ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();

    [Fact]
    public async Task Direct_savechanges_insert_invalidates()
    {
        var (cn, ctx, cache) = Create();
        using var _cn = cn; using var _c = cache; await using var _ = ctx;
        Assert.Equal(2, (await CachedAll(ctx)).Count);
        ctx.Add(new Row { Id = 3, V = 30 });
        await ctx.SaveChangesAsync();
        Assert.Equal(3, (await CachedAll(ctx)).Count);
    }

    [Fact]
    public async Task Batched_savechanges_update_invalidates()
    {
        var (cn, ctx, cache) = Create();
        using var _cn = cn; using var _c = cache; await using var _ = ctx;
        Assert.Equal(2, (await CachedAll(ctx)).Count);
        var rows = await ctx.Query<Row>().ToListAsync();
        foreach (var r in rows) r.V += 1;   // multi-entity batch
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { 11, 21 }, (await CachedAll(ctx)).OrderBy(r => r.Id).Select(r => r.V).ToArray());
    }

    [Fact]
    public async Task Bulk_insert_update_delete_each_invalidate()
    {
        var (cn, ctx, cache) = Create();
        using var _cn = cn; using var _c = cache; await using var _ = ctx;

        Assert.Equal(2, (await CachedAll(ctx)).Count);
        await ctx.BulkInsertAsync(new[] { new Row { Id = 3, V = 30 } });
        Assert.Equal(3, (await CachedAll(ctx)).Count);

        await ctx.BulkUpdateAsync(new[] { new Row { Id = 3, V = 31 } });
        Assert.Equal(31, (await CachedAll(ctx)).Single(r => r.Id == 3).V);

        await ctx.BulkDeleteAsync(new[] { new Row { Id = 3, V = 31 } });
        Assert.Equal(2, (await CachedAll(ctx)).Count);
    }

    [Fact]
    public async Task Execute_update_and_delete_each_invalidate()
    {
        var (cn, ctx, cache) = Create();
        using var _cn = cn; using var _c = cache; await using var _ = ctx;

        Assert.Equal(2, (await CachedAll(ctx)).Count);
        await ctx.Query<Row>().Where(r => r.Id == 1).ExecuteUpdateAsync(s => s.SetProperty(r => r.V, 99));
        Assert.Equal(99, (await CachedAll(ctx)).Single(r => r.Id == 1).V);

        await ctx.Query<Row>().Where(r => r.Id == 2).ExecuteDeleteAsync();
        Assert.Single(await CachedAll(ctx));
    }
}
