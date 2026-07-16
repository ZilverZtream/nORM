using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins result-cache isolation between contexts whose GLOBAL FILTERS differ while they share
/// one cache provider over the same database, probed differentially: a filtered and an
/// unfiltered context never serve each other's rows (their SQL differs, so their keys do),
/// and — the sharp case — two contexts whose closure-parameterized filters produce IDENTICAL
/// SQL with different captured values also stay isolated, because the filter's parameter
/// values participate in the cache key. Guards the neighbour class of the tenant-key,
/// connection-private-database, and live-database-swap cache-identity kills.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterCacheIsolationContractTests
{
    [Table("GfcbRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static SqliteConnection Seed()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE GfcbRow_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
            "INSERT INTO GfcbRow_Test VALUES (1,1,0),(2,5,0),(3,9,1);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn, NormMemoryCacheProvider cache, Action<DbContextOptions>? configure)
    {
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<Row>()
        };
        configure?.Invoke(opts);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Filtered_and_unfiltered_contexts_sharing_a_cache_do_not_serve_each_other()
    {
        using var cn = Seed();
        using var cache = new NormMemoryCacheProvider();

        using var filtered = Ctx(cn, cache, o => o.AddGlobalFilter<Row>(r => !r.IsDeleted));
        using var unfiltered = Ctx(cn, cache, null);

        var a = ((INormQueryable<Row>)filtered.Query<Row>()).AsNoTracking()
            .OrderBy(r => r.Id).Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Console.WriteLine($"filtered ctx: [{string.Join(",", a.Select(r => r.Id))}]");

        var b = ((INormQueryable<Row>)unfiltered.Query<Row>()).AsNoTracking()
            .OrderBy(r => r.Id).Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Console.WriteLine($"unfiltered ctx: [{string.Join(",", b.Select(r => r.Id))}]");

        Assert.Equal(new[] { 1, 2 }, a.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 1, 2, 3 }, b.Select(r => r.Id).ToArray());
    }

    [Fact]
    public void Closure_parameterized_filters_with_identical_sql_do_not_share_entries()
    {
        using var cn = Seed();
        using var cache = new NormMemoryCacheProvider();

        var minA = 5;
        var minB = 0;
        using var ctxA = Ctx(cn, cache, o => o.AddGlobalFilter<Row>(r => r.Val >= minA));
        using var ctxB = Ctx(cn, cache, o => o.AddGlobalFilter<Row>(r => r.Val >= minB));

        var a = ((INormQueryable<Row>)ctxA.Query<Row>()).AsNoTracking()
            .OrderBy(r => r.Id).Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Console.WriteLine($"min=5 ctx: [{string.Join(",", a.Select(r => r.Id))}]");

        var b = ((INormQueryable<Row>)ctxB.Query<Row>()).AsNoTracking()
            .OrderBy(r => r.Id).Cacheable(TimeSpan.FromMinutes(5)).ToList();
        Console.WriteLine($"min=0 ctx: [{string.Join(",", b.Select(r => r.Id))}]");

        Assert.Equal(new[] { 2, 3 }, a.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 1, 2, 3 }, b.Select(r => r.Id).ToArray());
    }
}
