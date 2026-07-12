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
/// The direct active-record writes InsertAsync/UpdateAsync/DeleteAsync must invalidate the result
/// cache for their table, like SaveChanges/ExecuteUpdate/Bulk do. Otherwise a .Cacheable() query
/// keeps serving pre-write (stale/deleted) rows until the cache entry expires.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DirectWriteCacheInvalidationTests
{
    [Table("DwUser")]
    private class DwUser
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create(NormMemoryCacheProvider cache)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DwUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "INSERT INTO DwUser VALUES (1, 'original');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions { CacheProvider = cache }));
    }

    private static string NameInDb(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM DwUser WHERE Id = 1";
        return (string)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task UpdateAsync_invalidates_cached_query()
    {
        using var cache = new NormMemoryCacheProvider();
        var (cn, ctx) = Create(cache);
        using var _cn = cn;
        using var _ctx = ctx;

        // Warm the cache. AsNoTracking isolates the cache dimension: a tracking re-query would
        // return the already-tracked instance from the identity map regardless of the cache, which
        // is a separate ORM semantic. Real-world caching is read-mostly / no-tracking.
        var before = ((INormQueryable<DwUser>)ctx.Query<DwUser>())
            .AsNoTracking().Where(u => u.Id == 1).Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Equal("original", before[0].Name);

        // Direct write via the active-record API (detached entity, bypasses SaveChanges).
        await ctx.UpdateAsync(new DwUser { Id = 1, Name = "changed" });
        Assert.Equal("changed", NameInDb(cn)); // the write itself must have persisted

        // Same cache key — must reflect the write, not the stale cached row.
        var after = ((INormQueryable<DwUser>)ctx.Query<DwUser>())
            .AsNoTracking().Where(u => u.Id == 1).Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Equal("changed", after[0].Name);
    }

    [Fact]
    public async Task DeleteAsync_invalidates_cached_query()
    {
        using var cache = new NormMemoryCacheProvider();
        var (cn, ctx) = Create(cache);
        using var _cn = cn;
        using var _ctx = ctx;

        var before = ctx.Query<DwUser>().Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Single(before);

        await ctx.DeleteAsync(new DwUser { Id = 1, Name = "original" });

        var after = ctx.Query<DwUser>().Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Empty(after); // deleted row must not be served from cache
    }

    [Fact]
    public async Task InsertAsync_invalidates_cached_query()
    {
        using var cache = new NormMemoryCacheProvider();
        var (cn, ctx) = Create(cache);
        using var _cn = cn;
        using var _ctx = ctx;

        var before = ctx.Query<DwUser>().Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Single(before);

        await ctx.InsertAsync(new DwUser { Id = 2, Name = "new" });

        var after = ctx.Query<DwUser>().Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Assert.Equal(2, after.Count); // inserted row must appear
    }
}
