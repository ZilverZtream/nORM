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
/// A correlated ElementAt/ElementAtOrDefault over an ordered, scalar-projected subquery in a
/// predicate must translate to a single-row scalar subquery skipping N rows (LIMIT 1 OFFSET n /
/// SQL Server OFFSET…FETCH). Without an ordering the position is undefined, so it fails closed.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedElementAtSubqueryTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CeaParent")]
    public class Parent { [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [System.ComponentModel.DataAnnotations.Schema.Table("CeaChild")]
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
                CREATE TABLE CeaParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CeaChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO CeaParent VALUES (1,'a'),(2,'b'),(3,'c');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, val) in ChildRows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO CeaChild VALUES ({id},{pid},{val})";
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
    public async Task ElementAt_index_in_predicate_matches_oracle()
    {
        await using var ctx = Make();
        // 2nd-smallest (index 1) child value per parent: p1->30, p2->50, p3->12. > 20 => {1,2}.
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Val).Select(c => c.Val).ElementAt(1) > 20)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        var oracle = new[] { 1, 2, 3 }
            .Where(pid => Oracle().Where(c => c.ParentId == pid)
                              .OrderBy(c => c.Val).Select(c => c.Val).ElementAt(1) > 20)
            .OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task ElementAt_closure_index_rebinds_across_executions()
    {
        await using var ctx = Make();
        async Task<int[]> Run(int idx) => (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Val).Select(c => c.Val).ElementAt(idx) >= 12)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        int[] Oracle2(int idx) => new[] { 1, 2, 3 }
            .Where(pid => Oracle().Where(c => c.ParentId == pid).OrderBy(c => c.Val).Select(c => c.Val).ElementAt(idx) >= 12)
            .OrderBy(x => x).ToArray();

        Assert.Equal(Oracle2(0), await Run(0)); // smallest: p1=10,p2=5,p3=7 -> none >=12 => {}
        Assert.Equal(Oracle2(1), await Run(1)); // 2nd: 30,50,12 -> all >=12 => {1,2,3}
    }

    [Fact]
    public async Task ElementAt_without_ordering_fails_closed()
    {
        await using var ctx = Make();
        await Assert.ThrowsAsync<nORM.Core.NormUnsupportedFeatureException>(async () =>
            _ = (await ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                                .Select(c => c.Val).ElementAt(1) > 20)
                .Select(p => new { p.Id }).ToListAsync()));
    }
}
