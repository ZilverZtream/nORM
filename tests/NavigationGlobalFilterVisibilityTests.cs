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
/// Global filters (soft-delete, tenant) gate visibility through REFERENCE
/// navigations: a filtered-out principal reads as a MISSING parent, so its data
/// neither matches navigation predicates nor appears in navigation projections —
/// previously the correlated scalar subquery skipped the principal's filter and
/// leaked soft-deleted rows into both. Collection-navigation aggregates already
/// filtered their children and are pinned alongside.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationGlobalFilterVisibilityTests
{
    [Table("NavGf_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public bool Deleted { get; set; }
    }

    [Table("NavGf_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("NavGf_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public bool Done { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavGf_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Deleted INTEGER NOT NULL);
                CREATE TABLE NavGf_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE NavGf_Chore (Id INTEGER PRIMARY KEY, EmpId INTEGER NOT NULL, Done INTEGER NOT NULL);
                INSERT INTO NavGf_Dept VALUES (1, 'Eng', 0), (2, 'Eng', 1);
                INSERT INTO NavGf_Emp VALUES (1, 'ann', 1), (2, 'bob', 2);
                INSERT INTO NavGf_Chore VALUES (1, 1, 0), (2, 1, 1), (3, 2, 0);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Dept>(d => !d.Deleted);
        opts.AddGlobalFilter<Chore>(c => !c.Done);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Dept>().HasKey(d => d.Id);
            mb.Entity<Emp>().HasKey(e => e.Id);
            mb.Entity<Chore>().HasKey(c => c.Id);
            mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void nav_predicate_respects_principal_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // bob's dept (Id 2, 'Eng') is soft-deleted: it must behave like a missing
        // parent — bob must NOT match the Eng predicate.
        var names = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng")
            .Select(e => e.Name).ToList();
        Assert.Equal(new[] { "ann" }, names);
    }

    [Fact]
    public void nav_projection_respects_principal_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>().Select(e => new { e.Name, Title = e.Dept!.Title })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Equal("Eng", rows[0].Title);
        Assert.Null(rows[1].Title); // soft-deleted parent reads as missing
    }

    [Fact]
    public void collection_nav_count_respects_child_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>().Select(e => new { e.Name, N = e.Chores.Count() })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Equal(1, rows[0].N); // ann: chore 2 is Done → filtered out
        Assert.Equal(1, rows[1].N);
    }
}
