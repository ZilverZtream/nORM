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
/// Aggregate selectors accept navigation members: terminal aggregates and GroupBy
/// aggregates route through the predicate translator, and collection-navigation
/// aggregates in projections (e.Chores.Sum(c => c.Dept.Bonus)) render the selector
/// as nested correlated scalars — including nullable casts, two-hop chains, and
/// filtered collections. Previously only a bare uncast member was accepted and a
/// nullable cast threw.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateSelectorTests
{
    [Table("NavAggSel_Region")]
    private class Region
    {
        [Key] public int Id { get; set; }
        public int Tax { get; set; }
    }

    [Table("NavAggSel_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public int Bonus { get; set; }
        public int? RegionId { get; set; }
        [ForeignKey(nameof(RegionId))] public Region? Region { get; set; }
    }

    [Table("NavAggSel_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string City { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("NavAggSel_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public int Effort { get; set; }
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavAggSel_Region (Id INTEGER PRIMARY KEY, Tax INTEGER NOT NULL);
                CREATE TABLE NavAggSel_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Bonus INTEGER NOT NULL, RegionId INTEGER NULL);
                CREATE TABLE NavAggSel_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, City TEXT NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE NavAggSel_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, Effort INTEGER NOT NULL, DeptId INTEGER NULL);
                INSERT INTO NavAggSel_Region VALUES (10, 25);
                INSERT INTO NavAggSel_Dept VALUES (1, 'Eng', 100, 10), (2, 'Ops', 50, NULL);
                INSERT INTO NavAggSel_Emp VALUES (1, 'ann', 'oslo', 1), (2, 'bob', 'oslo', 2), (3, 'cid', 'rome', NULL), (4, 'dan', 'rome', 1);
                INSERT INTO NavAggSel_Chore VALUES (1, 1, 5, 2), (2, 1, 3, NULL), (3, 2, 7, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Region>().HasKey(r => r.Id);
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Chore>().HasKey(c => c.Id);
                mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
            }
        });
    }

    [Fact]
    public void collection_nav_aggregate_two_hop_selector()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .Select(e => new { e.Name, T = e.Chores.Max(c => (int?)c.Dept!.Region!.Tax) })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Null(rows[0].T);      // ann: chores reach Ops(region null) and null dept
        Assert.Equal(25, rows[1].T); // bob: chore reaches Eng → region 10 → tax 25
    }

    [Fact]
    public void filtered_collection_nav_aggregate_with_nav_selector()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .Select(e => new { e.Name, S = e.Chores.Where(c => c.Effort > 4).Sum(c => (int?)c.Dept!.Bonus) })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Equal(50, rows[0].S);   // ann: effort-5 chore → Ops(50)
        Assert.Equal(100, rows[1].S);  // bob: effort-7 chore → Eng(100)
    }

    [Fact]
    public void terminal_aggregate_with_nav_selector()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Sum over nav member; missing dept contributes null → treated as 0 (Enumerable.Sum semantics for nullable? here int → null row skipped by SQL SUM)
        var total = ctx.Query<Emp>().Sum(e => (int?)e.Dept!.Bonus);
        Assert.Equal(250, total); // 100 + 50 + null + 100
    }

    [Fact]
    public void groupby_aggregate_with_nav_selector()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .GroupBy(e => e.City)
            .Select(g => new { g.Key, MaxBonus = g.Max(x => (int?)x.Dept!.Bonus) })
            .ToList().OrderBy(r => r.Key).ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal(100, rows[0].MaxBonus); // oslo: ann(100), bob(50)
        Assert.Equal(100, rows[1].MaxBonus); // rome: cid(null), dan(100)
    }

    [Fact]
    public void collection_nav_aggregate_with_nested_nav_selector()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .Select(e => new { e.Name, S = e.Chores.Sum(c => (int?)c.Dept!.Bonus) })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Equal(50, rows[0].S);   // ann: chore→Ops(50), chore→null
        Assert.Equal(100, rows[1].S);  // bob: chore→Eng(100)
    }
}
