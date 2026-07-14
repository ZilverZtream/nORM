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
/// Two correlated subqueries side by side in one predicate, each carrying its own closure,
/// must keep their compiled-parameter (@cp) slots distinct — no cross-binding — and re-bind
/// each closure across cached-plan executions. Guards the closure-ordinal alignment that the
/// correlated First/Last/aggregate translation depends on.
/// </summary>
[Trait("Category", "Fast")]
public class SiblingCorrelatedSubqueryClosureTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("ScsParent")]
    public class Parent { [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [System.ComponentModel.DataAnnotations.Schema.Table("ScsChild")]
    public class Child { [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; } public int ParentId { get; set; } public int Val { get; set; } }

    private static readonly (int Id, int Pid, int Val)[] ChildRows =
    {
        (1, 1, 5), (2, 1, 40), (3, 2, 15), (4, 2, 25), (5, 3, 2), (6, 3, 3), (7, 3, 60),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ScsParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE ScsChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO ScsParent VALUES (1,'a'),(2,'b'),(3,'c');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, val) in ChildRows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO ScsChild VALUES ({id},{pid},{val})";
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
    public async Task Two_sibling_correlated_subqueries_with_distinct_closures()
    {
        await using var ctx = Make();

        // Parents with at least one child Val > hi AND at least one child Val < lo. The two
        // closures (hi, lo) land in sibling correlated subqueries — their @cp slots must not
        // cross-bind, and must re-bind across cached-plan executions.
        async Task<int[]> Run(int hi, int lo) => (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id && c.Val > hi).Count() > 0
                     && ctx.Query<Child>().Where(c => c.ParentId == p.Id && c.Val < lo).Count() > 0)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        int[] OracleRun(int hi, int lo) => new[] { 1, 2, 3 }
            .Where(pid => Oracle().Any(c => c.ParentId == pid && c.Val > hi)
                       && Oracle().Any(c => c.ParentId == pid && c.Val < lo))
            .OrderBy(x => x).ToArray();

        Assert.Equal(OracleRun(30, 10), await Run(30, 10)); // hi=30,lo=10: p1(40>30 &5<10)✓, p3(60>30 &2<10)✓ => {1,3}
        Assert.Equal(OracleRun(20, 20), await Run(20, 20)); // different needles re-bind
        Assert.Equal(OracleRun(50, 4), await Run(50, 4));
    }

    [Fact]
    public async Task Sibling_first_and_max_with_swapped_closure_order()
    {
        await using var ctx = Make();
        // First-of-ordered and Max in sibling subqueries, each compared to a distinct closure,
        // in an order where a slot mixup would swap the two needles.
        async Task<int[]> Run(int firstMin, int maxMin) => (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).OrderBy(c => c.Id).Select(c => c.Val).First() >= firstMin
                     && ctx.Query<Child>().Where(c => c.ParentId == p.Id).Max(c => c.Val) >= maxMin)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        int[] OracleRun(int firstMin, int maxMin) => new[] { 1, 2, 3 }
            .Where(pid => Oracle().Where(c => c.ParentId == pid).OrderBy(c => c.Id).Select(c => c.Val).First() >= firstMin
                       && Oracle().Where(c => c.ParentId == pid).Max(c => c.Val) >= maxMin)
            .OrderBy(x => x).ToArray();

        Assert.Equal(OracleRun(5, 30), await Run(5, 30));
        Assert.Equal(OracleRun(15, 60), await Run(15, 60));
        Assert.Equal(OracleRun(3, 3), await Run(3, 3));
    }
}
