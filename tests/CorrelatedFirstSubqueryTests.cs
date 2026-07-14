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
/// A correlated First/FirstOrDefault over an ordered, scalar-projected subquery in a
/// predicate must translate to a single-row scalar subquery (ORDER BY … LIMIT 1) and
/// match the LINQ-to-Objects result. Previously these threw
/// NormUnsupportedFeatureException ("Queryable method 'First' is not supported").
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CorrelatedFirstSubqueryTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CfsParent")]
    public class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CfsChild")]
    public class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    // (Id, ParentId, Val) — every parent has at least one child (First over a non-empty
    // sequence, so LINQ-to-Objects does not throw).
    private static readonly (int Id, int Pid, int Val)[] ChildRows =
    {
        (1, 1, 10), (2, 1, 30), (3, 2, 5), (4, 2, 50), (5, 3, 7), (6, 3, 7),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CfsParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CfsChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO CfsParent VALUES (1,'a'),(2,'b'),(3,'c');
                """;
            cmd.ExecuteNonQuery();
            foreach (var (id, pid, val) in ChildRows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO CfsChild VALUES ({id},{pid},{val})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Parent>().HasKey(p => p.Id); mb.Entity<Child>().HasKey(c => c.Id); }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // LINQ-to-Objects oracle over the seed data.
    private static IEnumerable<Child> OracleChildren() =>
        ChildRows.Select(r => new Child { Id = r.Id, ParentId = r.Pid, Val = r.Val });

    [Fact]
    public async Task Highest_child_value_via_ordered_first_matches_oracle()
    {
        await using var ctx = Make();
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderByDescending(c => c.Val).Select(c => c.Val).First() > 20)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        var oracle = new[] { 1, 2, 3 }
            .Where(pid => OracleChildren().Where(c => c.ParentId == pid)
                              .OrderByDescending(c => c.Val).Select(c => c.Val).First() > 20)
            .OrderBy(x => x).ToArray();

        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Lowest_child_value_via_ordered_first_matches_oracle()
    {
        await using var ctx = Make();
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Val).Select(c => c.Val).First() <= 7)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        var oracle = new[] { 1, 2, 3 }
            .Where(pid => OracleChildren().Where(c => c.ParentId == pid)
                              .OrderBy(c => c.Val).Select(c => c.Val).First() <= 7)
            .OrderBy(x => x).ToArray();

        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task First_with_inner_predicate_matches_oracle()
    {
        await using var ctx = Make();
        // First child (ordered by Id) whose Val is above 6, per parent.
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Id).Select(c => c.Val).First(v => v > 6) >= 10)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        var oracle = new[] { 1, 2, 3 }
            .Where(pid => OracleChildren().Where(c => c.ParentId == pid)
                              .OrderBy(c => c.Id).Select(c => c.Val).First(v => v > 6) >= 10)
            .OrderBy(x => x).ToArray();

        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Closure_in_inner_filter_is_not_baked_into_the_cached_plan()
    {
        await using var ctx = Make();

        static async Task<int[]> Run(DbContext ctx, int threshold)
            => (await ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id && c.Val >= threshold)
                                .OrderByDescending(c => c.Val).Select(c => c.Val).First() > 0)
                .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        int[] Oracle(int threshold) => new[] { 1, 2, 3 }
            .Where(pid => OracleChildren().Where(c => c.ParentId == pid && c.Val >= threshold)
                              .OrderByDescending(c => c.Val).Select(c => c.Val).FirstOrDefault() > 0)
            .OrderBy(x => x).ToArray();

        // First execution primes the plan cache with threshold=40; a second execution with a
        // different threshold must re-bind, not replay the first needle.
        Assert.Equal(Oracle(40), await Run(ctx, 40));
        Assert.Equal(Oracle(8), await Run(ctx, 8));
        Assert.Equal(Oracle(100), await Run(ctx, 100));
    }
}
