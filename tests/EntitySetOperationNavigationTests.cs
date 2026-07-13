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
/// Set operations over ENTITY queries compose with navigation predicates in the
/// arms and with downstream operators: Union/Concat/Except/Intersect arms may
/// filter through a reference navigation, and the combined set accepts further
/// Where, ordering, paging, and aggregate terminals.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntitySetOperationNavigationTests
{
    [Table("SetNav_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("SetNav_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? Salary { get; set; }
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
                CREATE TABLE SetNav_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE SetNav_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Salary INTEGER NULL, DeptId INTEGER NULL);
                INSERT INTO SetNav_Dept VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO SetNav_Emp VALUES (1, 'ann', 10, 1), (2, 'bob', 20, 2), (3, 'cid', 30, NULL), (4, 'dan', 40, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Dept>().HasKey(d => d.Id); mb.Entity<Emp>().HasKey(e => e.Id); }
        });
    }

    [Fact]
    public void entity_union_with_nav_predicates_in_arms()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng")
            .Union(ctx.Query<Emp>().Where(e => e.Salary > 25))
            .ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal(new[] { 1, 3, 4 }, rows.Select(e => e.Id).ToArray());
    }

    [Fact]
    public void entity_concat_count()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var n = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng")
            .Concat(ctx.Query<Emp>().Where(e => e.Salary >= 20)).Count();
        Assert.Equal(5, n); // 2 (ann,dan) + 3 (bob,cid,dan) with duplicates kept
    }

    [Fact]
    public void entity_except_with_downstream_where()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .Except(ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng"))
            .Where(e => e.Salary > 15)
            .ToList().OrderBy(e => e.Id).ToList();
        Assert.Equal(new[] { 2, 3 }, rows.Select(e => e.Id).ToArray());
    }

    [Fact]
    public void entity_intersect_orderby_take()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>().Where(e => e.Salary >= 20)
            .Intersect(ctx.Query<Emp>().Where(e => e.Dept != null))
            .OrderByDescending(e => e.Salary).Take(1)
            .ToList();
        Assert.Equal(new[] { 4 }, rows.Select(e => e.Id).ToArray());
    }
}
