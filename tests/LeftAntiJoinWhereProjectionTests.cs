using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Query-syntax left join with an INTERVENING WHERE on the null-probe (the classic left-anti-join
/// idiom) must translate: the WHERE between DefaultIfEmpty() and the projection lowers to a nested
/// transparent identifier (GroupJoin(...,(p,g)=>new{p,g}).SelectMany(DefaultIfEmpty,(t,c)=>new{t,c})
/// .Where(x=>x.c==null).Select(x=>x.t.p.Name)); the join-projection rewrite must resolve x.t.p.Name
/// through it. Every result is diffed against the LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LeftAntiJoinWhereProjectionTests
{
    [Table("LajParent")] public class Parent { [Key] public int Id { get; set; } public int? K { get; set; } public string Name { get; set; } = ""; }
    [Table("LajChild")] public class Child { [Key] public int Id { get; set; } public int? K { get; set; } public string Tag { get; set; } = ""; }

    private static async Task<DbContext> NewCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE LajParent (Id INTEGER PRIMARY KEY, K INTEGER NULL, Name TEXT NOT NULL);" +
                "CREATE TABLE LajChild (Id INTEGER PRIMARY KEY, K INTEGER NULL, Tag TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var p in ParentList()) ctx.Add(p);
        foreach (var c in ChildList()) ctx.Add(c);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static List<Parent> ParentList() => new()
    {
        new Parent { Id = 1, K = 10, Name = "p10" },
        new Parent { Id = 2, K = 20, Name = "p20" },   // no matching child -> anti-join hit
        new Parent { Id = 3, K = null, Name = "pnull" }, // null key never matches
    };
    private static List<Child> ChildList() => new()
    {
        new Child { Id = 1, K = 10, Tag = "c1" },
        new Child { Id = 2, K = 30, Tag = "c3" },
    };

    [Fact]
    public async Task LeftAntiJoin_where_inner_is_null_projects_outer_member()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var rows = await (from p in ctx.Query<Parent>()
                      join c in ctx.Query<Child>() on p.K equals c.K into g
                      from c in g.DefaultIfEmpty()
                      where c == null
                      select p.Name)
                      .ToListAsync();
        var actual = rows.OrderBy(s => s).ToList();
        var expected = (from p in ParentList()
                        join c in ChildList() on p.K equals c.K into g
                        from c in g.DefaultIfEmpty()
                        where c == null
                        select p.Name)
                        .OrderBy(s => s).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task LeftJoin_where_on_inner_value_3vl_projects_anon()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn);
        var rows = await (from p in ctx.Query<Parent>()
                      join c in ctx.Query<Child>() on p.K equals c.K into g
                      from c in g.DefaultIfEmpty()
                      where c == null || c.Tag == "c1"
                      select new { p.Name, Tag = c != null ? c.Tag : "<none>" })
                      .ToListAsync();
        var actual = rows.Select(r => r.Name + "|" + r.Tag).OrderBy(s => s).ToList();
        var expected = (from p in ParentList()
                        join c in ChildList() on p.K equals c.K into g
                        from c in g.DefaultIfEmpty()
                        where c == null || c.Tag == "c1"
                        select p.Name + "|" + (c != null ? c.Tag : "<none>"))
                        .OrderBy(s => s).ToList();
        Assert.Equal(expected, actual);
    }
}
