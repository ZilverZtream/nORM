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
/// A correlated First/FirstOrDefault over an ordered, scalar-projected subquery used
/// inside a SELECT projection must lower to a single-row scalar subquery (ORDER BY …
/// LIMIT 1) — the "top child value per parent" report shape. Previously the projection
/// path only handled Count/Sum/Min/Max/Average and threw client-eval for First.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedFirstProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CfjParent")]
    public class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CfjChild")]
    public class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    // Parent 4 deliberately has NO children (empty-subquery case).
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
                CREATE TABLE CfjParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CfjChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO CfjParent VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, val) in ChildRows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO CfjChild VALUES ({id},{pid},{val})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Parent>().HasKey(p => p.Id); mb.Entity<Child>().HasKey(c => c.Id); }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Top_child_value_per_parent_projects_correctly()
    {
        await using var ctx = Make();
        // Parents 1,2,3 have children; restrict to them for the non-nullable First.
        var got = (await ctx.Query<Parent>().Where(p => p.Id <= 3)
            .OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                Top = ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                          .OrderByDescending(c => c.Val).Select(c => c.Val).First()
            })
            .ToListAsync());

        Assert.Equal(new[] { 30, 50, 12 }, got.OrderBy(x => x.Id).Select(x => x.Top).ToArray());
    }

    [Fact]
    public async Task First_with_predicate_in_projection_projects_correctly()
    {
        await using var ctx = Make();
        // Smallest-Id child with Val > 8, per parent (parents 1..3 all qualify).
        var got = (await ctx.Query<Parent>().Where(p => p.Id <= 3)
            .OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                V = ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                        .OrderBy(c => c.Id).Select(c => c.Val).First(v => v > 8)
            })
            .ToListAsync());

        // p1: first Val>8 by Id -> 10; p2: 50; p3: 12
        Assert.Equal(new[] { 10, 50, 12 }, got.OrderBy(x => x.Id).Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Empty_subquery_projects_null_for_nullable_first()
    {
        await using var ctx = Make();
        // Parent 4 has no children — FirstOrDefault into a NULLABLE projection member must
        // surface SQL NULL, not a spurious value or a throw.
        var got = (await ctx.Query<Parent>()
            .OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                Top = ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                          .OrderByDescending(c => c.Val).Select(c => (int?)c.Val).FirstOrDefault()
            })
            .ToListAsync());

        var byId = got.ToDictionary(x => x.Id, x => x.Top);
        Assert.Equal(30, byId[1]);
        Assert.Equal(50, byId[2]);
        Assert.Equal(12, byId[3]);
        Assert.Null(byId[4]); // no children -> NULL
    }

    [Fact]
    public async Task Uncorrelated_first_with_closure_is_not_folded_into_the_cached_plan()
    {
        await using var ctx = Make();

        // The inner subquery references NO outer row — only a closure. It must stay a
        // server-side subquery whose closure re-binds each execution, NOT be constant-folded
        // and baked into the cached plan (which would replay the first threshold).
        static async Task<int?[]> Run(DbContext ctx, int threshold)
            => (await ctx.Query<Parent>().Where(p => p.Id <= 3).OrderBy(p => p.Id)
                .Select(p => new
                {
                    Hi = ctx.Query<Child>().Where(c => c.Val >= threshold)
                             .OrderByDescending(c => c.Val).Select(c => (int?)c.Val).FirstOrDefault()
                })
                .ToListAsync()).Select(x => x.Hi).ToArray();

        // Highest child Val >= threshold across ALL children: 50, then (>=13) 30, then (>=100) none.
        Assert.All(await Run(ctx, 1), v => Assert.Equal(50, v));
        Assert.All(await Run(ctx, 13), v => Assert.Equal(50, v));
        Assert.All(await Run(ctx, 40), v => Assert.Equal(50, v));
        Assert.All(await Run(ctx, 51), v => Assert.Null(v));
    }
}
