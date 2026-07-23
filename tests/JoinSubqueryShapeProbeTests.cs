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
/// Probes join and subquery shapes against a LINQ-to-Objects oracle: inner join, group join (count per
/// group), left join via GroupJoin+SelectMany+DefaultIfEmpty, join then GroupBy, EXISTS/NOT EXISTS via
/// Any, All over a correlated set, IN via a subquery Contains, and a correlated Count filter. These carry
/// the trickiest translation risks — an empty outer row dropped, a NULL-side left row lost, a correlated
/// predicate mis-bound. Passing shapes stay as regression keepers; a mismatch is a bug to fix.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class JoinSubqueryShapeProbeTests
{
    [Table("JDept")]
    public sealed class JDept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("JEmp")]
    public sealed class JEmp
    {
        [Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
        public int Salary { get; set; }
    }

    private static readonly JDept[] Depts =
    {
        new() { Id = 1, Name = "Eng" },
        new() { Id = 2, Name = "Sales" },
        new() { Id = 3, Name = "Empty" },
    };

    private static readonly JEmp[] Emps =
    {
        new() { Id = 1, DeptId = 1, Name = "a", Salary = 100 },
        new() { Id = 2, DeptId = 1, Name = "b", Salary = 200 },
        new() { Id = 3, DeptId = 2, Name = "c", Salary = 150 },
        new() { Id = 4, DeptId = 2, Name = "d", Salary = 50 },
        new() { Id = 5, DeptId = 4, Name = "orphan", Salary = 999 }, // DeptId with no matching dept
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE JDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE JEmp (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Name TEXT NOT NULL, Salary INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<JDept>().HasKey(x => x.Id); mb.Entity<JEmp>().HasKey(x => x.Id); }
        }, ownsConnection: true);
        foreach (var d in Depts) ctx.Add(new JDept { Id = d.Id, Name = d.Name });
        foreach (var e in Emps) ctx.Add(new JEmp { Id = e.Id, DeptId = e.DeptId, Name = e.Name, Salary = e.Salary });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    [Fact]
    public void Inner_join_emp_to_dept()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JEmp>().Join(ctx.Query<JDept>(), e => e.DeptId, d => d.Id, (e, d) => new { E = e.Name, D = d.Name })
            .OrderBy(x => x.E).ToList().Select(x => (x.E, x.D)).ToList();
        var oracle = Emps.Join(Depts, e => e.DeptId, d => d.Id, (e, d) => new { E = e.Name, D = d.Name })
            .OrderBy(x => x.E).Select(x => (x.E, x.D)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Group_join_count_per_dept()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().GroupJoin(ctx.Query<JEmp>(), d => d.Id, e => e.DeptId, (d, es) => new { d.Name, C = es.Count() })
            .OrderBy(x => x.Name).ToList().Select(x => (x.Name, x.C)).ToList();
        var oracle = Depts.GroupJoin(Emps, d => d.Id, e => e.DeptId, (d, es) => new { d.Name, C = es.Count() })
            .OrderBy(x => x.Name).Select(x => (x.Name, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Left_join_via_groupjoin_selectmany_defaultifempty()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().GroupJoin(ctx.Query<JEmp>(), d => d.Id, e => e.DeptId, (d, es) => new { d, es })
            .SelectMany(x => x.es.DefaultIfEmpty(), (x, e) => new { D = x.d.Name, E = e == null ? "<none>" : e.Name })
            .OrderBy(x => x.D).ThenBy(x => x.E).ToList().Select(x => (x.D, x.E)).ToList();
        var oracle = Depts.GroupJoin(Emps, d => d.Id, e => e.DeptId, (d, es) => new { d, es })
            .SelectMany(x => x.es.DefaultIfEmpty(), (x, e) => new { D = x.d.Name, E = e == null ? "<none>" : e.Name })
            .OrderBy(x => x.D).ThenBy(x => x.E).Select(x => (x.D, x.E)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Join_then_groupby_sum_salary()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JEmp>().Join(ctx.Query<JDept>(), e => e.DeptId, d => d.Id, (e, d) => new { d.Name, e.Salary })
            .GroupBy(x => x.Name).Select(g => new { g.Key, S = g.Sum(x => x.Salary) })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.S)).ToList();
        var oracle = Emps.Join(Depts, e => e.DeptId, d => d.Id, (e, d) => new { d.Name, e.Salary })
            .GroupBy(x => x.Name).Select(g => new { g.Key, S = g.Sum(x => x.Salary) })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.S)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Exists_via_any_subquery()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().Where(d => ctx.Query<JEmp>().Any(e => e.DeptId == d.Id)).OrderBy(d => d.Id).Select(d => d.Name).ToList();
        var oracle = Depts.Where(d => Emps.Any(e => e.DeptId == d.Id)).OrderBy(d => d.Id).Select(d => d.Name).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Not_exists_via_negated_any()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().Where(d => !ctx.Query<JEmp>().Any(e => e.DeptId == d.Id)).OrderBy(d => d.Id).Select(d => d.Name).ToList();
        var oracle = Depts.Where(d => !Emps.Any(e => e.DeptId == d.Id)).OrderBy(d => d.Id).Select(d => d.Name).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void All_over_correlated_set()
    {
        using var ctx = NewCtx();
        // Depts where every employee earns > 75 (a dept with no employees is vacuously true).
        var norm = ctx.Query<JDept>().Where(d => ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).All(e => e.Salary > 75))
            .OrderBy(d => d.Id).Select(d => d.Name).ToList();
        var oracle = Depts.Where(d => Emps.Where(e => e.DeptId == d.Id).All(e => e.Salary > 75))
            .OrderBy(d => d.Id).Select(d => d.Name).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void In_via_subquery_contains()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JEmp>().Where(e => ctx.Query<JDept>().Where(d => d.Name == "Eng").Select(d => d.Id).Contains(e.DeptId))
            .OrderBy(e => e.Id).Select(e => e.Name).ToList();
        var oracle = Emps.Where(e => Depts.Where(d => d.Name == "Eng").Select(d => d.Id).Contains(e.DeptId))
            .OrderBy(e => e.Id).Select(e => e.Name).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_count_filter()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().Where(d => ctx.Query<JEmp>().Count(e => e.DeptId == d.Id) >= 2)
            .OrderBy(d => d.Id).Select(d => d.Name).ToList();
        var oracle = Depts.Where(d => Emps.Count(e => e.DeptId == d.Id) >= 2)
            .OrderBy(d => d.Id).Select(d => d.Name).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_nullable_max_salary_in_projection()
    {
        using var ctx = NewCtx();
        // Nullable Max returns null for a dept with no employees (dept 3) — the idiomatic empty-safe form,
        // no DefaultIfEmpty needed.
        var norm = ctx.Query<JDept>().OrderBy(d => d.Id)
            .Select(d => new { d.Name, Top = ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).Max(e => (int?)e.Salary) })
            .ToList().Select(x => (x.Name, x.Top)).ToList();
        var oracle = Depts.OrderBy(d => d.Id)
            .Select(d => new { d.Name, Top = Emps.Where(e => e.DeptId == d.Id).Max(e => (int?)e.Salary) })
            .Select(x => (x.Name, x.Top)).ToList();
        Assert.Equal(oracle, norm);
    }

    // ── DefaultIfEmpty over a correlated value aggregate (empty dept 3 exercises the fallback) ──

    [Fact]
    public void Correlated_max_with_default_if_empty_const()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(-1).Max() })
            .ToList().Select(x => (x.Id, x.V)).ToList();
        var oracle = Depts.OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = Emps.Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(-1).Max() })
            .Select(x => (x.Id, x.V)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_min_with_default_if_empty_const()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>().OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(9999).Min() })
            .ToList().Select(x => (x.Id, x.V)).ToList();
        var oracle = Depts.OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = Emps.Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(9999).Min() })
            .Select(x => (x.Id, x.V)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_sum_with_default_if_empty_const()
    {
        using var ctx = NewCtx();
        // DefaultIfEmpty(777) makes an empty dept's sum 777 rather than 0 — distinguishes the fallback from
        // the plain empty-Sum-is-0 behavior.
        var norm = ctx.Query<JDept>().OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(777).Sum() })
            .ToList().Select(x => (x.Id, x.V)).ToList();
        var oracle = Depts.OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = Emps.Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(777).Sum() })
            .Select(x => (x.Id, x.V)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_max_with_default_if_empty_no_arg()
    {
        using var ctx = NewCtx();
        // No-argument DefaultIfEmpty() uses default(int) = 0 for an empty dept.
        var norm = ctx.Query<JDept>().OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty().Max() })
            .ToList().Select(x => (x.Id, x.V)).ToList();
        var oracle = Depts.OrderBy(d => d.Id)
            .Select(d => new { d.Id, V = Emps.Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty().Max() })
            .Select(x => (x.Id, x.V)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_max_default_if_empty_in_predicate()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<JDept>()
            .Where(d => ctx.Query<JEmp>().Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(0).Max() >= 150)
            .OrderBy(d => d.Id).Select(d => d.Name).ToList();
        var oracle = Depts
            .Where(d => Emps.Where(e => e.DeptId == d.Id).Select(e => e.Salary).DefaultIfEmpty(0).Max() >= 150)
            .OrderBy(d => d.Id).Select(d => d.Name).ToList();
        Assert.Equal(oracle, norm);
    }
}
