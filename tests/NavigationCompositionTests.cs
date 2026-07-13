using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Navigation features compose with the rest of the query surface: compiled queries
/// bind navigation predicates per invocation, Include eager-loads exactly the
/// windowed parents under Skip/Take/Where/OrderBy/Distinct, global filters apply
/// both to navigation-predicate queries and INSIDE eager-load levels, and
/// ThenInclude chains traverse collection then reference then reference.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompiledNavigationAndIncludeWindowTests
{
    [Table("CompNavW_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("CompNavW_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("CompNavW_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
    }

    private static readonly Func<DbContext, string, Task<List<Emp>>> _compiledNavPredicate =
        Norm.CompileQuery<DbContext, string, Emp>((ctx, title) =>
            ctx.Query<Emp>().Where(e => e.Dept!.Title == title).OrderBy(e => e.Id));

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CompNavW_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE CompNavW_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE CompNavW_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL);
                INSERT INTO CompNavW_Dept VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO CompNavW_Emp VALUES (1, 'ann', 1), (2, 'bob', 2), (3, 'cid', NULL), (4, 'dan', 1), (5, 'eve', 1);
                INSERT INTO CompNavW_Chore VALUES (1, 1, 'code'), (2, 1, 'ship'), (3, 2, 'ops'), (4, 4, 'test'), (5, 5, 'doc');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Chore>().HasKey(c => c.Id);
                mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
            }
        });
    }

    [Fact]
    public async Task compiled_query_with_nav_predicate()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var eng = await _compiledNavPredicate(ctx, "Eng");
        Assert.Equal(new[] { 1, 4, 5 }, eng.Select(e => e.Id).ToArray());
        var ops = await _compiledNavPredicate(ctx, "Ops");
        Assert.Equal(new[] { 2 }, ops.Select(e => e.Id).ToArray());
    }

    [Fact]
    public void include_with_skip_take_loads_only_windowed_parents()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var page = ((INormQueryable<Emp>)ctx.Query<Emp>().OrderBy(e => e.Id).Skip(1).Take(2))
            .Include(e => e.Chores).ToList();
        Assert.Equal(new[] { 2, 3 }, page.Select(e => e.Id).ToArray());
        Assert.Single(page[0].Chores);   // bob: ops
        Assert.Empty(page[1].Chores);    // cid: none
    }

    [Fact]
    public void include_reference_with_where_orderby_take()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ((INormQueryable<Emp>)ctx.Query<Emp>().Where(e => e.Id > 1).OrderByDescending(e => e.Id).Take(2))
            .Include(e => e.Dept!).ToList();
        Assert.Equal(new[] { 5, 4 }, rows.Select(e => e.Id).ToArray());
        Assert.All(rows, e => Assert.Equal("Eng", e.Dept?.Title));
    }
}

[Trait("Category", TestCategory.Fast)]
public class NavigationCompositionTests
{
    [Table("NavComp_Region")]
    private class Region
    {
        [Key] public int Id { get; set; }
        public string Zone { get; set; } = "";
    }

    [Table("NavComp_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public int? RegionId { get; set; }
        [ForeignKey(nameof(RegionId))] public Region? Region { get; set; }
    }

    [Table("NavComp_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool Archived { get; set; }
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("NavComp_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn, bool withFilter = false)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        var opts = new DbContextOptions();
        if (withFilter)
            opts.AddGlobalFilter<Emp>(e => !e.Archived);
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavComp_Region (Id INTEGER PRIMARY KEY, Zone TEXT NOT NULL);
                CREATE TABLE NavComp_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, RegionId INTEGER NULL);
                CREATE TABLE NavComp_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Archived INTEGER NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE NavComp_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO NavComp_Region VALUES (10, 'EU');
                INSERT INTO NavComp_Dept VALUES (1, 'Eng', 10), (2, 'Ops', NULL);
                INSERT INTO NavComp_Emp VALUES (1, 'ann', 0, 1), (2, 'bob', 1, 1), (3, 'cid', 0, 2);
                INSERT INTO NavComp_Chore VALUES (1, 1, 'code', 2), (2, 1, 'ship', NULL), (3, 3, 'ops', 1);
                """;
            cmd.ExecuteNonQuery();
        }
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Region>().HasKey(r => r.Id);
            mb.Entity<Dept>().HasKey(d => d.Id);
            mb.Entity<Emp>().HasKey(e => e.Id);
            mb.Entity<Chore>().HasKey(c => c.Id);
            mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void global_filter_with_nav_predicate_query()
    {
        using var ctx = Ctx(out var cn, withFilter: true);
        using var _cn = cn;
        // Global filter (!Archived) must compose with the nav predicate: only ann is
        // active AND in Eng.
        var names = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng").Select(e => e.Name).ToList();
        Assert.Equal(new[] { "ann" }, names);
    }

    [Fact]
    public void global_filter_applies_inside_include_eager_load()
    {
        using var ctx = Ctx(out var cn, withFilter: true);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Chores).ToList()
            .OrderBy(e => e.Id).ToList();
        Assert.Equal(2, emps.Count); // bob filtered out
    }

    [Fact]
    public void include_after_distinct()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>().Distinct()).Include(e => e.Dept!)
            .ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal(3, emps.Count);
        Assert.Equal("Eng", emps[0].Dept?.Title);
    }

    [Fact]
    public void three_level_theninclude_collection_reference_reference()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Emp → Chores (collection) → Dept (reference) → Region (reference)
        var emps = ((INormQueryable<Emp>)ctx.Query<Emp>())
            .Include(e => e.Chores).ThenInclude(c => c.Dept!).ThenInclude(d => d.Region!)
            .ToList().OrderBy(e => e.Id).ToList();
        var annChores = emps[0].Chores.OrderBy(c => c.Id).ToList();
        Assert.Equal("Ops", annChores[0].Dept?.Title);
        Assert.Null(annChores[0].Dept?.Region);          // Ops has no region
        Assert.Equal("EU", emps[2].Chores.Single().Dept?.Region?.Zone); // cid→ops chore→Eng→EU
    }
}
