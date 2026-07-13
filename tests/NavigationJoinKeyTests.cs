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
/// Join keys may reach through a reference navigation (e.Dept.Title): inner joins,
/// GroupJoin (either side), and the GroupJoin+SelectMany left-join shape. The
/// GroupJoin runtime never evaluates a navigation key on the client — segmentation
/// uses the outer's PK identity and the inner-match probe uses the inner's PK, since
/// a navigation member is not a materialized column on either side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationJoinKeyTests
{
    [Table("NavJk_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("NavJk_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    [Table("NavJk_Badge")]
    private class Badge
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavJk_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE NavJk_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                CREATE TABLE NavJk_Badge (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                INSERT INTO NavJk_Dept VALUES (1, 'Eng'), (2, 'Ops');
                INSERT INTO NavJk_Emp VALUES (1, 'ann', 1), (2, 'bob', 2), (3, 'cid', NULL), (4, 'dan', 1);
                INSERT INTO NavJk_Badge VALUES (1, 'Eng'), (2, 'Sales');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Badge>().HasKey(b => b.Id);
            }
        });
    }

    [Fact]
    public void Inner_join_on_nav_member_key()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .Join(ctx.Query<Badge>(), e => e.Dept!.Title, b => b.Label, (e, b) => new { e.Name, b.Label })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Equal(2, rows.Count); // ann+dan reach 'Eng'; bob is Ops; cid has no dept
        Assert.Equal("ann", rows[0].Name);
        Assert.Equal("dan", rows[1].Name);
    }

    [Fact]
    public void GroupJoin_on_nav_member_inner_key()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Badge>()
            .GroupJoin(ctx.Query<Emp>(), b => b.Label, e => e.Dept!.Title, (b, es) => new { b.Label, Count = es.Count() })
            .ToList().OrderBy(r => r.Label).ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows.Single(r => r.Label == "Eng").Count);
        Assert.Equal(0, rows.Single(r => r.Label == "Sales").Count);
    }

    [Fact]
    public void GroupJoin_on_nav_member_outer_key()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Emp>()
            .GroupJoin(ctx.Query<Badge>(), e => e.Dept!.Title, b => b.Label, (e, bs) => new { e.Name, Count = bs.Count() })
            .ToList().OrderBy(r => r.Name).ToList();
        Assert.Equal(4, rows.Count);
        Assert.Equal(1, rows.Single(r => r.Name == "ann").Count);
        Assert.Equal(0, rows.Single(r => r.Name == "bob").Count);
        Assert.Equal(0, rows.Single(r => r.Name == "cid").Count); // no dept → null key → no match
        Assert.Equal(1, rows.Single(r => r.Name == "dan").Count);
    }

    [Fact]
    public void Left_join_on_nav_member_key()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Badge>()
            .GroupJoin(ctx.Query<Emp>(), b => b.Label, e => e.Dept!.Title, (b, es) => new { b, es })
            .SelectMany(x => x.es.DefaultIfEmpty(), (x, e) => new { x.b.Label, Emp = e != null ? e.Name : null })
            .ToList().OrderBy(r => r.Label).ThenBy(r => r.Emp).ToList();
        Assert.Equal(3, rows.Count); // Eng→ann, Eng→dan, Sales→null
        Assert.Null(rows[2].Emp);
    }
}
