using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SelectMany over a collection navigation makes the CHILD the query's root: the
/// flatten join is wrapped as a derived table under the default alias so downstream
/// Select/Where/OrderBy/aggregates/paging resolve child members without knowing a
/// join happened. An outer Where and a filtered navigation
/// (SelectMany(e => e.Chores.Where(...))) fold inside the wrapper — their SQL
/// references the join's internal aliases.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CollectionNavigationFlattenTests
{
    [Table("NavFlat_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("NavFlat_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public int Effort { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavFlat_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE NavFlat_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL, Effort INTEGER NOT NULL);
                INSERT INTO NavFlat_Emp VALUES (1, 'ann'), (2, 'bob'), (3, 'cid');
                INSERT INTO NavFlat_Chore VALUES (1, 1, 'code', 5), (2, 1, 'ship', 3), (3, 2, 'ops', 7);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Chore>().HasKey(c => c.Id);
                mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
            }
        });
    }

    [Fact]
    public void Flattened_children_project_their_own_members()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var whats = ctx.Query<Emp>().SelectMany(e => e.Chores).Select(c => c.What)
            .ToList().OrderBy(w => w).ToList();
        Assert.Equal(new[] { "code", "ops", "ship" }, whats);
    }

    [Fact]
    public void Flattened_children_support_where_and_aggregates()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var hard = ctx.Query<Emp>().SelectMany(e => e.Chores).Where(c => c.Effort > 4)
            .Select(c => c.What).ToList().OrderBy(w => w).ToList();
        Assert.Equal(new[] { "code", "ops" }, hard);

        Assert.Equal(15, ctx.Query<Emp>().SelectMany(e => e.Chores).Sum(c => c.Effort));
        Assert.Equal(3, ctx.Query<Emp>().SelectMany(e => e.Chores).Count());
    }

    [Fact]
    public void Outer_where_folds_into_the_flatten()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var whats = ctx.Query<Emp>().Where(e => e.Name == "ann").SelectMany(e => e.Chores)
            .Select(c => c.What).ToList().OrderBy(w => w).ToList();
        Assert.Equal(new[] { "code", "ship" }, whats);
    }

    [Fact]
    public void Filtered_navigation_collection_folds_into_the_flatten()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var whats = ctx.Query<Emp>().SelectMany(e => e.Chores.Where(c => c.Effort >= 5))
            .Select(c => c.What).ToList().OrderBy(w => w).ToList();
        Assert.Equal(new[] { "code", "ops" }, whats);
    }

    [Fact]
    public void Flattened_children_support_ordering_and_paging()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var whats = ctx.Query<Emp>().SelectMany(e => e.Chores)
            .OrderByDescending(c => c.Effort).Take(2).Select(c => c.What).ToList();
        Assert.Equal(new[] { "ops", "code" }, whats);
    }

    [Fact]
    public void Result_selector_overload_still_projects_both_sides()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>().SelectMany(e => e.Chores, (e, c) => new { e.Name, c.What })
            .ToList().OrderBy(r => r.What).ToList();
        Assert.Equal(3, rows.Count);
        Assert.Equal("ann", rows[0].Name);
        Assert.Equal("bob", rows[1].Name);
    }

    [Fact]
    public void Collection_nav_aggregates_in_projections_and_ordering()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var counts = ctx.Query<Emp>().Select(e => new { e.Name, N = e.Chores.Count() })
            .ToList().OrderBy(r => r.Name).Select(r => r.N).ToArray();
        Assert.Equal(new[] { 2, 1, 0 }, counts);

        var sums = ctx.Query<Emp>().Select(e => new { e.Name, S = e.Chores.Sum(c => c.Effort) })
            .ToList().OrderBy(r => r.Name).Select(r => r.S).ToArray();
        Assert.Equal(new[] { 8, 7, 0 }, sums);

        var byLoad = ctx.Query<Emp>().OrderByDescending(e => e.Chores.Count()).ThenBy(e => e.Name)
            .Select(e => e.Name).ToList();
        Assert.Equal(new[] { "ann", "bob", "cid" }, byLoad);

        var busy = ctx.Query<Emp>().Where(e => e.Chores.Any(c => c.Effort > 4))
            .Select(e => e.Name).ToList().OrderBy(n => n).ToList();
        Assert.Equal(new[] { "ann", "bob" }, busy);
    }
}
