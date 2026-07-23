#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probes navigation-based query shapes against an oracle that resolves the navigations by hand from the
/// raw rows: a reference-nav scalar, a two-hop nested reference nav, navigation values in a predicate, and
/// collection-navigation aggregates (Count/Any/predicate-Count) in projections and predicates — plus a
/// filtered Include. NULL and dangling foreign keys are seeded so the LEFT-join null semantics are
/// exercised. Passing shapes stay as regression keepers; a mismatch or throw is a gap to fix.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NavigationShapeProbeTests
{
    [Table("NsRegion")]
    public sealed class Region
    {
        [Key] public int Id { get; set; }
        public string Zone { get; set; } = "";
    }

    [Table("NsDept")]
    public sealed class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public int? RegionId { get; set; }
        [ForeignKey(nameof(RegionId))] public Region? Region { get; set; }
    }

    [Table("NsEmp")]
    public sealed class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("NsChore")]
    public sealed class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public int? DeptId { get; set; }
    }

    // Raw rows (mirror the seed); navigations resolved by hand in oracles.
    private static readonly Region[] Regions = { new() { Id = 10, Zone = "EU" } };
    private static readonly Dept[] Depts =
    {
        new() { Id = 1, Title = "Eng", RegionId = 10 },
        new() { Id = 2, Title = "Ops", RegionId = null },
    };
    private static readonly Emp[] Emps =
    {
        new() { Id = 1, Name = "ann", DeptId = 1 },
        new() { Id = 2, Name = "bob", DeptId = null },
        new() { Id = 3, Name = "cid", DeptId = 1 },
        new() { Id = 4, Name = "dan", DeptId = 99 }, // dangling FK
    };
    private static readonly Chore[] Chores =
    {
        new() { Id = 1, EmpId = 1, What = "code", DeptId = 2 },
        new() { Id = 2, EmpId = 1, What = "ship", DeptId = null },
        new() { Id = 3, EmpId = 3, What = "ops",  DeptId = 1 },
    };

    private static Dept? DeptOf(Emp e) => Depts.FirstOrDefault(d => d.Id == e.DeptId);
    private static Region? RegionOf(Dept? d) => d?.RegionId == null ? null : Regions.FirstOrDefault(r => r.Id == d.RegionId);

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NsRegion (Id INTEGER PRIMARY KEY, Zone TEXT NOT NULL);
                CREATE TABLE NsDept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, RegionId INTEGER NULL);
                CREATE TABLE NsEmp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE NsChore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, What TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO NsRegion VALUES (10, 'EU');
                INSERT INTO NsDept VALUES (1, 'Eng', 10), (2, 'Ops', NULL);
                INSERT INTO NsEmp VALUES (1, 'ann', 1), (2, 'bob', NULL), (3, 'cid', 1), (4, 'dan', 99);
                INSERT INTO NsChore VALUES (1, 1, 'code', 2), (2, 1, 'ship', NULL), (3, 3, 'ops', 1);
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
        }, ownsConnection: true);
    }

    private static IQueryable<Emp> QE(DbContext ctx) => ctx.Query<Emp>();

    [Fact]
    public void Reference_nav_scalar_in_projection()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).OrderBy(e => e.Id).Select(e => (string?)e.Dept!.Title).ToList();
        var oracle = Emps.OrderBy(e => e.Id).Select(e => DeptOf(e)?.Title).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Nested_reference_nav_scalar_in_projection()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).OrderBy(e => e.Id).Select(e => (string?)e.Dept!.Region!.Zone).ToList();
        var oracle = Emps.OrderBy(e => e.Id).Select(e => RegionOf(DeptOf(e))?.Zone).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Reference_nav_value_in_predicate()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).Where(e => e.Dept!.Title == "Eng").OrderBy(e => e.Id).Select(e => e.Id).ToList();
        var oracle = Emps.Where(e => DeptOf(e)?.Title == "Eng").OrderBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Nested_reference_nav_value_in_predicate()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).Where(e => e.Dept!.Region!.Zone == "EU").OrderBy(e => e.Id).Select(e => e.Id).ToList();
        var oracle = Emps.Where(e => RegionOf(DeptOf(e))?.Zone == "EU").OrderBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_count_in_projection()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).OrderBy(e => e.Id).Select(e => new { e.Id, C = e.Chores.Count() })
            .ToList().Select(x => (x.Id, x.C)).ToList();
        var oracle = Emps.OrderBy(e => e.Id).Select(e => (e.Id, C: Chores.Count(c => c.EmpId == e.Id))).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_count_with_predicate_in_projection()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).OrderBy(e => e.Id).Select(e => new { e.Id, C = e.Chores.Count(c => c.DeptId != null) })
            .ToList().Select(x => (x.Id, x.C)).ToList();
        var oracle = Emps.OrderBy(e => e.Id).Select(e => (e.Id, C: Chores.Count(c => c.EmpId == e.Id && c.DeptId != null))).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_any_in_predicate()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).Where(e => e.Chores.Any(c => c.What == "code")).OrderBy(e => e.Id).Select(e => e.Id).ToList();
        var oracle = Emps.Where(e => Chores.Any(c => c.EmpId == e.Id && c.What == "code")).OrderBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_count_filter_in_predicate()
    {
        using var ctx = NewCtx();
        var norm = QE(ctx).Where(e => e.Chores.Count() >= 2).OrderBy(e => e.Id).Select(e => e.Id).ToList();
        var oracle = Emps.Where(e => Chores.Count(c => c.EmpId == e.Id) >= 2).OrderBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Filtered_include_loads_only_matching_children()
    {
        using var ctx = NewCtx();
        var emps = ((INormQueryable<Emp>)QE(ctx)).Include(e => e.Chores.Where(c => c.DeptId != null)).ToList()
            .OrderBy(e => e.Id).ToList();
        // ann (1) keeps only chore 1 (DeptId=2); chore 2 (DeptId null) is filtered out. cid (3) keeps chore 3.
        var ann = emps.Single(e => e.Id == 1);
        Assert.Equal(new[] { "code" }, ann.Chores.Select(c => c.What).OrderBy(x => x).ToArray());
        var cid = emps.Single(e => e.Id == 3);
        Assert.Equal(new[] { "ops" }, cid.Chores.Select(c => c.What).ToArray());
    }
}
