using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A correlated Last/LastOrDefault over an ordered, scalar-projected subquery must translate
/// to the same single-row scalar subquery as First but over the REVERSED ordering. Without an
/// ordering "last" is undefined in SQL and must fail closed rather than silently return the
/// first row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedLastSubqueryTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("ClsParent")]
    public class Parent { [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [System.ComponentModel.DataAnnotations.Schema.Table("ClsChild")]
    public class Child { [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; } public int ParentId { get; set; } public int Val { get; set; } }

    private static readonly (int Id, int Pid, int Val)[] ChildRows =
    {
        (1, 1, 10), (2, 1, 30), (3, 2, 5), (4, 2, 50), (5, 3, 7), (6, 3, 12),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ClsParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE ClsChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO ClsParent VALUES (1,'a'),(2,'b'),(3,'c');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, val) in ChildRows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO ClsChild VALUES ({id},{pid},{val})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Parent>().HasKey(p => p.Id); mb.Entity<Child>().HasKey(c => c.Id); }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Child> Oracle() =>
        ChildRows.Select(r => new Child { Id = r.Id, ParentId = r.Pid, Val = r.Val });

    [Fact]
    public async Task Last_of_ascending_order_in_predicate_matches_oracle()
    {
        await using var ctx = Make();
        // Last of ascending Val = the max; > 20 keeps p1(30) and p2(50).
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Val).Select(c => c.Val).Last() > 20)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        var oracle = new[] { 1, 2, 3 }
            .Where(pid => Oracle().Where(c => c.ParentId == pid)
                              .OrderBy(c => c.Val).Select(c => c.Val).Last() > 20)
            .OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Last_in_projection_matches_oracle()
    {
        await using var ctx = Make();
        // Last child by ascending Id per parent: p1->30(id2), p2->50(id4), p3->12(id6).
        var got = (await ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                Lv = ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                        .OrderBy(c => c.Id).Select(c => c.Val).Last()
            })
            .ToListAsync());

        var oracle = new[] { 1, 2, 3 }
            .Select(pid => Oracle().Where(c => c.ParentId == pid).OrderBy(c => c.Id).Select(c => c.Val).Last())
            .ToArray();
        Assert.Equal(oracle, got.OrderBy(x => x.Id).Select(x => x.Lv).ToArray());
    }

    [Fact]
    public async Task Last_without_ordering_fails_closed()
    {
        await using var ctx = Make();
        await Assert.ThrowsAsync<nORM.Core.NormUnsupportedFeatureException>(async () =>
            _ = (await ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                                .Select(c => c.Val).Last() > 20)
                .Select(p => new { p.Id }).ToListAsync()));
    }
}
