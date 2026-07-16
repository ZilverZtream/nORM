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
/// Contracts for cache expiration and cross-context scope (caching matrix cells). An expired
/// entry re-fetches fresh data instead of serving forever. Two contexts on the SAME database
/// sharing one provider serve each other's entries. And the sharp edge: two SEPARATE ':memory:'
/// connections have IDENTICAL connection strings but are DIFFERENT databases - a shared cache
/// provider must not serve one database's rows for the other.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CacheExpirationAndScopeContractTests
{
    [Table("CacheScope_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static SqliteConnection Seed(params (int Id, int V)[] rows)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CacheScope_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);"
            + string.Join("", rows.Select(r => $"INSERT INTO CacheScope_Row VALUES ({r.Id}, {r.V});"));
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn, NormMemoryCacheProvider cache)
        => new(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            CacheProvider = cache
        });

    private static Task<System.Collections.Generic.List<Row>> Cached(DbContext ctx, TimeSpan ttl)
        => ((INormQueryable<Row>)ctx.Query<Row>()).Cacheable(ttl).ToListAsync();

    [Fact]
    public async Task Expired_entries_refetch_fresh_data()
    {
        using var cn = Seed((1, 10));
        using var cache = new NormMemoryCacheProvider();
        await using var ctx = Ctx(cn, cache);

        Assert.Single(await Cached(ctx, TimeSpan.FromMilliseconds(80)));

        // Mutate BEHIND the cache (raw SQL does not invalidate - by design); only expiry
        // makes the change visible through the same cached query.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CacheScope_Row VALUES (2, 20);";
            cmd.ExecuteNonQuery();
        }
        Assert.Single(await Cached(ctx, TimeSpan.FromMilliseconds(80)));   // still cached

        await Task.Delay(200);                                              // past the TTL
        Assert.Equal(2, (await Cached(ctx, TimeSpan.FromMilliseconds(80))).Count);
    }

    [Fact]
    public async Task Two_contexts_on_the_same_database_share_cache_entries()
    {
        using var cn = Seed((1, 10));
        using var cache = new NormMemoryCacheProvider();
        await using var a = Ctx(cn, cache);
        await using var b = Ctx(cn, cache);

        Assert.Single(await Cached(a, TimeSpan.FromMinutes(5)));

        // Mutate behind the cache; if B HITS A's entry (shared scope), it sees the cached list.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CacheScope_Row VALUES (2, 20);";
            cmd.ExecuteNonQuery();
        }
        Assert.Single(await Cached(b, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task Separate_in_memory_databases_with_identical_connection_strings_do_not_cross_serve()
    {
        // Two SEPARATE ':memory:' connections: identical connection string, different databases.
        using var cnA = Seed((1, 10));
        using var cnB = Seed((100, 1000), (200, 2000), (300, 3000));
        using var cache = new NormMemoryCacheProvider();
        await using var a = Ctx(cnA, cache);
        await using var b = Ctx(cnB, cache);

        // A populates the cache first; B must NOT be served A's one-row list.
        Assert.Single(await Cached(a, TimeSpan.FromMinutes(5)));
        var bRows = await Cached(b, TimeSpan.FromMinutes(5));
        Assert.Equal(3, bRows.Count);
        Assert.All(bRows, r => Assert.True(r.Id >= 100));

        // And the reverse direction on repeat reads.
        Assert.Single(await Cached(a, TimeSpan.FromMinutes(5)));
    }
}
