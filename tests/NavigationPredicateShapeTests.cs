using System;
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
/// Navigation members compose with the surrounding operator shapes: terminal
/// aggregates with predicates (Any/Count), uncorrelated subquery membership, and
/// ordering feeding a terminal. Pinned after a probe run showed them all handled.
/// </summary>

[Trait("Category", TestCategory.Fast)]
public class NavigationPredicateShapeTests
{
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO Dept VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO Emp VALUES (1, 'ann', 1), (2, 'bob', NULL), (3, 'cid', 2);
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Dept>().HasKey(d => d.Id); mb.Entity<Emp>().HasKey(e => e.Id); }
        });
        return (cn, ctx);
    }

    [Fact]
    public void Any_with_nav_predicate()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;
        Assert.True(ctx.Query<Emp>().Any(e => e.Dept!.Title == "Eng"));
        Assert.False(ctx.Query<Emp>().Any(e => e.Dept!.Title == "HR"));
    }

    [Fact]
    public void Count_with_nav_predicate()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;
        Assert.Equal(1, ctx.Query<Emp>().Count(e => e.Dept!.Title == "Eng"));
    }

    [Fact]
    public void Subquery_contains_over_ids()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;
        // Uncorrelated subquery membership: employees whose DeptId is any Dept id.
        var ids = ctx.Query<Emp>()
            .Where(e => ctx.Query<Dept>().Select(d => (int?)d.Id).Contains(e.DeptId))
            .Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public void OrderBy_nav_then_take()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;
        var first = ctx.Query<Emp>().Where(e => e.DeptId != null)
            .OrderBy(e => e.Dept!.Title).First();
        Assert.Equal(1, first.Id);
    }
}
