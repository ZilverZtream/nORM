using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial correctness hunt: Join / SelectMany / GroupJoin translation on SQLite, focused on
/// join-KEY types and left-join NULL semantics. Every case is oracle-checked against the SAME join
/// lambda run over in-memory Lists (LINQ-to-Objects). A row silently dropped / added / mismatched is
/// the target class of bug.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class JoinKeyAndNullSemanticsAdversarialTests
{
    // ---- plain-column entities ----
    [Table("JkParent")]
    public class JkParent { [Key] public int Id { get; set; } public int? K { get; set; } public string Name { get; set; } = ""; }
    [Table("JkChild")]
    public class JkChild { [Key] public int Id { get; set; } public int? K { get; set; } public string Tag { get; set; } = ""; }

    [Table("JkOrder")]
    public class JkOrder { [Key] public int Id { get; set; } public int X { get; set; } public int Y { get; set; } public string Info { get; set; } = ""; }
    [Table("JkLine")]
    public class JkLine { [Key] public int Id { get; set; } public int X { get; set; } public int Y { get; set; } public string Data { get; set; } = ""; }

    [Table("JkEmp")]
    public class JkEmp { [Key] public int Id { get; set; } public int? ManagerId { get; set; } public string Name { get; set; } = ""; }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params string[] ddl)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = string.Join(";", ddl) + ";";
            cmd.ExecuteNonQuery();
        }
        return await Task.FromResult(new DbContext(cn, new SqliteProvider()));
    }

    // Seed data reused by several tests.
    private static readonly (int Id, int? K, string Name)[] Parents =
    {
        (1, 10, "p1"), (2, null, "p2"), (3, 20, "p3"), (4, null, "p4"), (5, 10, "p5"),
    };
    private static readonly (int Id, int? K, string Tag)[] Children =
    {
        (1, 10, "c1"), (2, null, "c2"), (3, 10, "c3"), (4, 30, "c4"),
    };

    private static async Task SeedParentChild(DbContext ctx)
    {
        foreach (var p in Parents) ctx.Add(new JkParent { Id = p.Id, K = p.K, Name = p.Name });
        foreach (var c in Children) ctx.Add(new JkChild { Id = c.Id, K = c.K, Tag = c.Tag });
        await ctx.SaveChangesAsync();
    }

    private static List<JkParent> ParentList() => Parents.Select(p => new JkParent { Id = p.Id, K = p.K, Name = p.Name }).ToList();
    private static List<JkChild> ChildList() => Children.Select(c => new JkChild { Id = c.Id, K = c.K, Tag = c.Tag }).ToList();

    // ============================ NULLABLE-KEY INNER JOIN ============================

    [Fact]
    public async Task InnerJoin_nullable_int_key_matches_linq_join_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)");
        await SeedParentChild(ctx);

        var actual = ctx.Query<JkParent>()
            .Join(ctx.Query<JkChild>(), p => p.K, c => c.K, (p, c) => new { p.Name, c.Tag })
            .ToList()
            .Select(r => r.Name + "|" + r.Tag).OrderBy(s => s).ToList();

        var expected = ParentList()
            .Join(ChildList(), p => p.K, c => c.K, (p, c) => p.Name + "|" + c.Tag)
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    // ============================ GROUPJOIN + DefaultIfEmpty (LEFT JOIN) ============================

    [Fact]
    public async Task GroupJoin_DefaultIfEmpty_keeps_unmatched_left_rows_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)");
        await SeedParentChild(ctx);

        var actual = (from p in ctx.Query<JkParent>()
                      join c in ctx.Query<JkChild>() on p.K equals c.K into g
                      from c in g.DefaultIfEmpty()
                      select new { p.Name, Tag = c != null ? c.Tag : "<none>" })
                      .ToList()
                      .Select(r => r.Name + "|" + r.Tag).OrderBy(s => s).ToList();

        var expected = (from p in ParentList()
                        join c in ChildList() on p.K equals c.K into g
                        from c in g.DefaultIfEmpty()
                        select p.Name + "|" + (c != null ? c.Tag : "<none>"))
                        .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    // NOTE: left-anti-join / left-join with an INTERVENING WHERE on the null-probe
    // (`from p join c ... into g from c in g.DefaultIfEmpty() where c == null select p.Name`)
    // is now implemented — see LeftAntiJoinWhereProjectionTests for the oracle-diffed coverage.

    // ============================ SELECTMANY: cross join & correlated ============================

    [Fact]
    public async Task SelectMany_cross_join_row_count_and_values_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)");
        await SeedParentChild(ctx);

        var actual = ctx.Query<JkParent>()
            .SelectMany(p => ctx.Query<JkChild>(), (p, c) => new { p.Name, c.Tag })
            .ToList()
            .Select(r => r.Name + "|" + r.Tag).OrderBy(s => s).ToList();

        var expected = ParentList()
            .SelectMany(p => ChildList(), (p, c) => p.Name + "|" + c.Tag)
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task SelectMany_correlated_where_on_nullable_key_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)");
        await SeedParentChild(ctx);

        var actual = ctx.Query<JkParent>()
            .SelectMany(p => ctx.Query<JkChild>().Where(c => c.K == p.K), (p, c) => new { p.Name, c.Tag })
            .ToList()
            .Select(r => r.Name + "|" + r.Tag).OrderBy(s => s).ToList();

        var expected = ParentList()
            .SelectMany(p => ChildList().Where(c => c.K == p.K), (p, c) => p.Name + "|" + c.Tag)
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    // ============================ COMPOSITE-KEY JOIN ============================

    [Fact]
    public async Task CompositeKey_join_all_components_must_match_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkOrder (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL, Y INTEGER NOT NULL, Info TEXT NOT NULL)",
            "CREATE TABLE JkLine (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL, Y INTEGER NOT NULL, Data TEXT NOT NULL)");
        var orders = new[] { (1, 1, 1, "o11"), (2, 1, 2, "o12"), (3, 2, 1, "o21"), (4, 2, 2, "o22") };
        var lines = new[] { (1, 1, 1, "L11"), (2, 1, 2, "L12"), (3, 2, 9, "L29"), (4, 1, 1, "L11b") };
        foreach (var o in orders) ctx.Add(new JkOrder { Id = o.Item1, X = o.Item2, Y = o.Item3, Info = o.Item4 });
        foreach (var l in lines) ctx.Add(new JkLine { Id = l.Item1, X = l.Item2, Y = l.Item3, Data = l.Item4 });
        await ctx.SaveChangesAsync();

        var orderList = orders.Select(o => new JkOrder { Id = o.Item1, X = o.Item2, Y = o.Item3, Info = o.Item4 }).ToList();
        var lineList = lines.Select(l => new JkLine { Id = l.Item1, X = l.Item2, Y = l.Item3, Data = l.Item4 }).ToList();

        var actual = ctx.Query<JkOrder>()
            .Join(ctx.Query<JkLine>(), o => new { o.X, o.Y }, l => new { l.X, l.Y }, (o, l) => new { o.Info, l.Data })
            .ToList()
            .Select(r => r.Info + "|" + r.Data).OrderBy(s => s).ToList();

        var expected = orderList
            .Join(lineList, o => new { o.X, o.Y }, l => new { l.X, l.Y }, (o, l) => o.Info + "|" + l.Data)
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    // ============================ SELF-JOIN ============================

    [Fact]
    public async Task SelfJoin_employee_manager_picks_right_columns_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkEmp (Id INTEGER PRIMARY KEY, ManagerId INTEGER NULL, Name TEXT NOT NULL)");
        var emps = new (int, int?, string)[] { (1, null, "CEO"), (2, 1, "VP"), (3, 2, "Mgr"), (4, 2, "Eng") };
        foreach (var e in emps) ctx.Add(new JkEmp { Id = e.Item1, ManagerId = e.Item2, Name = e.Item3 });
        await ctx.SaveChangesAsync();
        var empList = emps.Select(e => new JkEmp { Id = e.Item1, ManagerId = e.Item2, Name = e.Item3 }).ToList();

        var actual = ctx.Query<JkEmp>()
            .Join(ctx.Query<JkEmp>(), e => e.ManagerId, m => m.Id, (e, m) => new { Emp = e.Name, Mgr = m.Name })
            .ToList()
            .Select(r => r.Emp + "->" + r.Mgr).OrderBy(s => s).ToList();

        var expected = empList
            .Join(empList, e => e.ManagerId, m => m.Id, (e, m) => e.Name + "->" + m.Name)
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    // ============================ MULTI-JOIN CHAIN (A join B join C) ============================

    [Fact]
    public async Task MultiJoin_chain_projects_right_columns_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)",
            "CREATE TABLE JkLine (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL, Y INTEGER NOT NULL, Data TEXT NOT NULL)");
        await SeedParentChild(ctx);
        var lines = new[] { (1, 10, 0, "LA"), (2, 30, 0, "LB"), (3, 10, 0, "LC") };
        foreach (var l in lines) ctx.Add(new JkLine { Id = l.Item1, X = l.Item2, Y = l.Item3, Data = l.Item4 });
        await ctx.SaveChangesAsync();
        var lineList = lines.Select(l => new JkLine { Id = l.Item1, X = l.Item2, Y = l.Item3, Data = l.Item4 }).ToList();

        // Parent K = Child K (int?), Child K = Line X (int). Join on int keys.
        var actual = ctx.Query<JkParent>()
            .Join(ctx.Query<JkChild>(), p => p.K, c => c.K, (p, c) => new { p, c })
            .Join(ctx.Query<JkLine>(), pc => pc.c.K, l => (int?)l.X, (pc, l) => new { pc.p.Name, pc.c.Tag, l.Data })
            .ToList()
            .Select(r => r.Name + "|" + r.Tag + "|" + r.Data).OrderBy(s => s).ToList();

        var expected = ParentList()
            .Join(ChildList(), p => p.K, c => c.K, (p, c) => new { p, c })
            .Join(lineList, pc => pc.c.K, l => (int?)l.X, (pc, l) => pc.p.Name + "|" + pc.c.Tag + "|" + l.Data)
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    // ============================ JOIN THEN downstream ops ============================

    [Fact]
    public async Task Join_then_groupby_count_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)");
        await SeedParentChild(ctx);

        var actual = ctx.Query<JkParent>()
            .Join(ctx.Query<JkChild>(), p => p.K, c => c.K, (p, c) => new { p.Name, c.Tag })
            .GroupBy(r => r.Name)
            .Select(g => new { Name = g.Key, Cnt = g.Count() })
            .ToList()
            .Select(r => r.Name + "=" + r.Cnt).OrderBy(s => s).ToList();

        var expected = ParentList()
            .Join(ChildList(), p => p.K, c => c.K, (p, c) => new { p.Name, c.Tag })
            .GroupBy(r => r.Name)
            .Select(g => g.Key + "=" + g.Count())
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Join_then_distinct_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn,
            "CREATE TABLE JkParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL)",
            "CREATE TABLE JkChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL)");
        await SeedParentChild(ctx);

        var actual = ctx.Query<JkParent>()
            .Join(ctx.Query<JkChild>(), p => p.K, c => c.K, (p, c) => p.Name)
            .Distinct()
            .ToList()
            .OrderBy(s => s).ToList();

        var expected = ParentList()
            .Join(ChildList(), p => p.K, c => c.K, (p, c) => p.Name)
            .Distinct()
            .OrderBy(s => s).ToList();

        Assert.Equal(expected, actual);
    }
}
