using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Global filters (the tenant-isolation mechanism) must apply INSIDE explicit
/// correlated subqueries — predicate and projection alike. A subquery that
/// skips them counts or matches ANOTHER tenant's rows: silent cross-tenant
/// data leakage through an aggregate.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedSubqueryGlobalFilterTests
{
    [Table("CsgfParent_Test")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Tenant { get; set; } = "";
    }

    [Table("CsgfChild_Test")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tenant { get; set; } = "";
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:csgf_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CsgfParent_Test (Id INTEGER PRIMARY KEY, Tenant TEXT NOT NULL);
                CREATE TABLE CsgfChild_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tenant TEXT NOT NULL);
                INSERT INTO CsgfParent_Test VALUES (1, 'T1'), (2, 'T1'), (3, 'T2');
                -- Parent 1: two T1 children and one T2 child (the leak detector).
                INSERT INTO CsgfChild_Test VALUES (1, 1, 'T1'), (2, 1, 'T1'), (3, 1, 'T2'), (4, 2, 'T1'), (5, 3, 'T2');
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Parent>(p => p.Tenant == "T1");
        opts.AddGlobalFilter<Child>(c => c.Tenant == "T1");
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Table("CsgfParent_Test")]
    public class NavParent
    {
        [Key] public int Id { get; set; }
        public string Tenant { get; set; } = "";
        public System.Collections.Generic.List<NavChild> Children { get; set; } = new();
    }

    [Table("CsgfChild_Test")]
    public class NavChild
    {
        [Key] public int Id { get; set; }
        public int NavParentId { get; set; }
        public string Tenant { get; set; } = "";
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateNavDb()
    {
        var cs = $"Data Source=file:csgfnav_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CsgfParent_Test (Id INTEGER PRIMARY KEY, Tenant TEXT NOT NULL);
                CREATE TABLE CsgfChild_Test (Id INTEGER PRIMARY KEY, NavParentId INTEGER NOT NULL, Tenant TEXT NOT NULL);
                INSERT INTO CsgfParent_Test VALUES (1, 'T1'), (2, 'T1'), (3, 'T2');
                -- Parent 1: two T1 children and one T2 child; parent 2: ONLY a T2 child.
                INSERT INTO CsgfChild_Test VALUES (1, 1, 'T1'), (2, 1, 'T1'), (3, 1, 'T2'), (4, 2, 'T2'), (5, 3, 'T2');
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<NavParent>(p => p.Tenant == "T1");
        opts.AddGlobalFilter<NavChild>(c => c.Tenant == "T1");
        return (keeper, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Navigation_count_in_projection_respects_global_filters()
    {
        var (keeper, ctx) = CreateNavDb();
        using var _ = keeper;
        await using var __ = ctx;

        var rows = (await ctx.Query<NavParent>()
                .Select(p => new { p.Id, N = p.Children.Count() })
                .ToListAsync())
            .OrderBy(x => x.Id).ToList();

        Assert.Equal(new[] { (1, 2), (2, 0) }, rows.Select(x => (x.Id, x.N)).ToArray());
    }

    [Fact]
    public async Task Navigation_any_in_predicate_respects_global_filters()
    {
        var (keeper, ctx) = CreateNavDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Parent 2's only child is tenant T2 — Any must be false under the filter.
        var ids = (await ctx.Query<NavParent>()
                .Where(p => p.Children.Any())
                .ToListAsync())
            .Select(p => p.Id).OrderBy(i => i).ToList();

        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Include_respects_child_global_filters()
    {
        var (keeper, ctx) = CreateNavDb();
        using var _ = keeper;
        await using var __ = ctx;

        var parents = (await ((INormQueryable<NavParent>)ctx.Query<NavParent>())
                .Include(p => p.Children)
                .ToListAsync())
            .OrderBy(p => p.Id).ToList();

        Assert.Equal(new[] { 1, 2 }, parents.Select(p => p.Id).ToArray());
        Assert.Equal(new[] { 1, 2 }, parents[0].Children.Select(c => c.Id).OrderBy(i => i).ToArray());
        Assert.Empty(parents[1].Children);
    }

    [Fact]
    public async Task Bulk_update_respects_global_filters()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // A broad bulk update on the filtered context must not touch the other
        // tenant's rows.
        await ctx.Query<Child>().Where(c => c.ParentId >= 1)
            .ExecuteUpdateAsync(s => s.SetProperty(c => c.ParentId, 99));

        using var read = keeper.CreateCommand();
        read.CommandText = "SELECT Id, ParentId, Tenant FROM CsgfChild_Test ORDER BY Id";
        using var reader = read.ExecuteReader();
        while (reader.Read())
        {
            var tenant = reader.GetString(2);
            var parentId = reader.GetInt32(1);
            if (tenant == "T1")
                Assert.True(parentId == 99, $"T1 child {reader.GetInt32(0)} not updated");
            else
                Assert.True(parentId != 99, $"T2 child {reader.GetInt32(0)} was updated across the tenant filter");
        }
    }

    [Fact]
    public async Task Bulk_delete_respects_global_filters()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        await ctx.Query<Child>().Where(c => c.ParentId >= 1).ExecuteDeleteAsync();

        using var read = keeper.CreateCommand();
        read.CommandText = "SELECT Tenant, COUNT(*) FROM CsgfChild_Test GROUP BY Tenant ORDER BY Tenant";
        using var reader = read.ExecuteReader();
        var survivors = new System.Collections.Generic.Dictionary<string, long>();
        while (reader.Read())
            survivors[reader.GetString(0)] = reader.GetInt64(1);

        Assert.False(survivors.ContainsKey("T1"), "T1 children not deleted");
        Assert.True(survivors.TryGetValue("T2", out var t2) && t2 == 2,
            $"T2 children were deleted across the tenant filter (survivors: {string.Join(",", survivors.Select(kv => kv.Key + "=" + kv.Value))})");
    }

    [Fact]
    public async Task Projection_correlated_count_respects_global_filters()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // Parent 1 has THREE children in the table but only TWO in tenant T1 —
        // counting three means the subquery leaked another tenant's row.
        var rows = (await ctx.Query<Parent>()
                .Select(p => new { p.Id, N = ctx.Query<Child>().Count(c => c.ParentId == p.Id) })
                .ToListAsync())
            .OrderBy(x => x.Id).ToList();

        Assert.Equal(new[] { (1, 2), (2, 1) }, rows.Select(x => (x.Id, x.N)).ToArray());
    }

    [Fact]
    public async Task Predicate_correlated_count_respects_global_filters()
    {
        var (keeper, ctx) = CreateDb();
        await using var __ = ctx;
        using var _ = keeper;

        // With the tenant filter, parent 1 has 2 children and parent 2 has 1 —
        // a leak (3 children for parent 1) changes the >= 3 row set.
        var ids = (await ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id) >= 3)
                .ToListAsync())
            .Select(p => p.Id).ToList();

        Assert.Empty(ids);

        var atLeastTwo = (await ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Count(c => c.ParentId == p.Id) >= 2)
                .ToListAsync())
            .Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, atLeastTwo);
    }

    [Fact]
    public async Task Predicate_correlated_any_respects_global_filters()
    {
        var (keeper, ctx) = CreateDb();
        await using var __ = ctx;
        using var _ = keeper;

        // Parent 3 is tenant T2: filtered from the OUTER query. Parent 2's only
        // child is T1: visible. A child-side leak would not change Any here, so
        // probe with a T2-child-only parent: give parent 2 a T2 child first.
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO CsgfChild_Test VALUES (6, 2, 'T2'); DELETE FROM CsgfChild_Test WHERE Id = 4";
            cmd.ExecuteNonQuery();
        }

        // Parent 2 now has ONLY a T2 child — Any must be false under the filter.
        var withChildren = (await ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Any(c => c.ParentId == p.Id))
                .ToListAsync())
            .Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, withChildren);
    }
}
