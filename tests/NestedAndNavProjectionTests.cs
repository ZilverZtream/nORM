using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for chained/nested projections and reference-navigation member
/// projections: Select(...).Select(...), projecting into a nested anonymous object, and projecting
/// a navigation member (e.Dept.Name) with filters/ordering over it. These re-projection and
/// nav-chain shapes are places a rebind could bind the wrong column or drop the nav join.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NestedAndNavProjectionTests
{
    [Table("NnpDept")]
    private sealed class Dept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Region { get; set; }
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("NnpEmp")]
    private sealed class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Salary { get; set; }
        public int DeptId { get; set; }
        public Dept Dept { get; set; } = null!;
    }

    private static readonly Dept[] Depts =
    {
        new() { Id = 1, Name = "Eng", Region = 10 },
        new() { Id = 2, Name = "Sales", Region = 20 },
        new() { Id = 3, Name = "Ops", Region = 10 },
    };
    private static readonly Emp[] Emps = Enumerable.Range(1, 15).Select(i => new Emp
    {
        Id = i, Name = "E" + i, Salary = 1000 + i * 100, DeptId = (i % 3) + 1,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NnpDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Region INTEGER NOT NULL);" +
                              "CREATE TABLE NnpEmp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Salary INTEGER NOT NULL, DeptId INTEGER NOT NULL);";
            foreach (var d in Depts) cmd.CommandText += $"INSERT INTO NnpDept VALUES ({d.Id},'{d.Name}',{d.Region});";
            foreach (var e in Emps) cmd.CommandText += $"INSERT INTO NnpEmp VALUES ({e.Id},'{e.Name}',{e.Salary},{e.DeptId});";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Dept>().HasMany(d => d.Emps).WithOne(e => e.Dept).HasForeignKey(e => e.DeptId, d => d.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Emp> EmpsWithDept()
        => Emps.Select(e => { e.Dept = Depts.First(d => d.Id == e.DeptId); return e; });

    [Fact]
    public void Chained_select_projects_correctly()
    {
        // Select(e => new {e.Salary, e.Name}).Select(x => x.Salary * 2) — re-projection.
        var expected = Emps.Select(e => new { e.Salary, e.Name }).Select(x => x.Salary * 2).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Emp>().Select(e => new { e.Salary, e.Name }).Select(x => x.Salary * 2).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Chained_select_with_computed_intermediate_matches_linq()
    {
        var expected = Emps.Select(e => new { Half = e.Salary / 2, e.Id }).Where(x => x.Half > 600).Select(x => x.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Emp>().Select(e => new { Half = e.Salary / 2, e.Id }).Where(x => x.Half > 600).Select(x => x.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Nav_member_projection_matches_linq()
    {
        // Project a reference-navigation member: e => new { e.Name, DeptName = e.Dept.Name }.
        var expected = EmpsWithDept().Select(e => new { e.Name, DeptName = e.Dept.Name }).OrderBy(x => x.Name)
            .Select(x => x.Name + ":" + x.DeptName).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Emp>().Select(e => new { e.Name, DeptName = e.Dept.Name }).OrderBy(x => x.Name)
            .Select(x => x.Name + ":" + x.DeptName).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Filter_on_nav_member_matches_linq()
    {
        var expected = EmpsWithDept().Where(e => e.Dept.Region == 10).Select(e => e.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Emp>().Where(e => e.Dept.Region == 10).Select(e => e.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Order_by_nav_member_matches_linq()
    {
        var expected = EmpsWithDept().OrderBy(e => e.Dept.Name).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Emp>().OrderBy(e => e.Dept.Name).ThenBy(e => e.Id).Select(e => e.Id).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Nested_object_projection_matches_linq()
    {
        // Project into a nested anonymous shape: new { e.Id, Info = new { e.Salary, e.Name } }.
        var expected = Emps.Select(e => new { e.Id, Info = new { e.Salary, e.Name } })
            .OrderBy(x => x.Id).Select(x => x.Id + "/" + x.Info.Salary + "/" + x.Info.Name).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Emp>().Select(e => new { e.Id, Info = new { e.Salary, e.Name } })
            .OrderBy(x => x.Id).Select(x => x.Id + "/" + x.Info.Salary + "/" + x.Info.Name).ToList();
        Assert.Equal(expected, actual);
    }
}
