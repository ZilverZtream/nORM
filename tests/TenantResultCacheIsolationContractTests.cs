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
/// Contract: cacheable query results never cross tenants (tenant matrix cell). The result-cache
/// key hashes the SQL, parameters (typed), database identity, AND the current tenant id, so two
/// tenants issuing the IDENTICAL Cacheable query through one shared cache provider must each be
/// served their own rows - the second tenant must not receive the first tenant's cached list.
/// Also pins that a tenant's own write still invalidates its cached entry (per-table
/// invalidation), so caching never trades correctness for staleness within a tenant.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantResultCacheIsolationContractTests
{
    [Table("TenCache_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenant : ITenantProvider
    {
        private readonly string _id;
        public FixedTenant(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private static DbContext CreateTenantContext(SqliteConnection cn, string tenant, NormMemoryCacheProvider cache)
        => new(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            TenantProvider = new FixedTenant(tenant),
            CacheProvider = cache
        });

    [Fact]
    public async Task Identical_cacheable_query_serves_each_tenant_its_own_rows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TenCache_Row (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);" +
                "INSERT INTO TenCache_Row VALUES (1, 'a1', 'T1'), (2, 'a2', 'T1'), (3, 'b1', 'T2');";
            cmd.ExecuteNonQuery();
        }
        using var cache = new NormMemoryCacheProvider();
        await using var ctxA = CreateTenantContext(cn, "T1", cache);
        await using var ctxB = CreateTenantContext(cn, "T2", cache);

        // Tenant A populates the cache with the query.
        var firstA = await ((INormQueryable<Row>)ctxA.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, firstA.Count);
        Assert.All(firstA, r => Assert.Equal("T1", r.TenantId));

        // Tenant B issues the IDENTICAL query: must get its own rows, not A's cached list.
        var firstB = await ((INormQueryable<Row>)ctxB.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        var only = Assert.Single(firstB);
        Assert.Equal("T2", only.TenantId);
        Assert.Equal("b1", only.Name);

        // Repeat reads (cache hits) stay scoped in both directions.
        var secondA = await ((INormQueryable<Row>)ctxA.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, secondA.Count);
        Assert.All(secondA, r => Assert.Equal("T1", r.TenantId));
        var secondB = await ((INormQueryable<Row>)ctxB.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(secondB);
        Assert.Equal("T2", secondB[0].TenantId);
    }

    [Fact]
    public async Task Own_tenant_write_invalidates_the_cached_result()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TenCache_Row (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);" +
                "INSERT INTO TenCache_Row VALUES (1, 'a1', 'T1');";
            cmd.ExecuteNonQuery();
        }
        using var cache = new NormMemoryCacheProvider();
        await using var ctxA = CreateTenantContext(cn, "T1", cache);

        var before = await ((INormQueryable<Row>)ctxA.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(before);

        ctxA.Add(new Row { Id = 2, Name = "a2", TenantId = "T1" });
        await ctxA.SaveChangesAsync();

        var after = await ((INormQueryable<Row>)ctxA.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, after.Count);
    }
}
