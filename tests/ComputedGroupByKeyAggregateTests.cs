using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for GroupBy with COMPUTED keys (arithmetic, modulo, conditional) and
/// COMPUTED aggregate selectors (Sum/Avg over an expression, not a bare member). These shapes are
/// easy to mistranslate silently — a computed key that groups differently than LINQ, or an aggregate
/// selector that binds the wrong expression, changes the result without throwing. Every comparison is
/// ordered by the key so ties can't mask a difference.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ComputedGroupByKeyAggregateTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CgkRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int Cat { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 40).Select(i => new Row
    {
        Id = i,
        A = (i * 7) % 20,          // 0..19
        B = (i * 3) % 11 + 1,      // 1..11
        Cat = i % 5,               // 0..4
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CgkRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Cat INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO CgkRow VALUES ({r.Id},{r.A},{r.B},{r.Cat});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void GroupBy_modulo_key_with_count_and_sum_matches_linq()
    {
        var expected = Rows.GroupBy(r => r.A % 3).Select(g => new { g.Key, N = g.Count(), S = g.Sum(x => x.B) })
            .OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.A % 3).Select(g => new { g.Key, N = g.Count(), S = g.Sum(x => x.B) })
            .OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.N, r.S)), actual.Select(r => (r.Key, r.N, r.S)));
    }

    [Fact]
    public void GroupBy_arithmetic_key_matches_linq()
    {
        var expected = Rows.GroupBy(r => r.A + r.Cat).Select(g => new { g.Key, N = g.Count() }).OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.A + r.Cat).Select(g => new { g.Key, N = g.Count() }).OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.N)), actual.Select(r => (r.Key, r.N)));
    }

    [Fact]
    public void GroupBy_conditional_key_matches_linq()
    {
        // Ternary key: bucket into "high"/"low" as a bool key.
        var expected = Rows.GroupBy(r => r.A >= 10).Select(g => new { g.Key, N = g.Count(), S = g.Sum(x => x.A) })
            .OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.A >= 10).Select(g => new { g.Key, N = g.Count(), S = g.Sum(x => x.A) })
            .OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.N, r.S)), actual.Select(r => (r.Key, r.N, r.S)));
    }

    [Fact]
    public void GroupBy_with_computed_aggregate_selector_matches_linq()
    {
        // Aggregate over an EXPRESSION (A*B, A-B), not a bare member.
        var expected = Rows.GroupBy(r => r.Cat)
            .Select(g => new { g.Key, Prod = g.Sum(x => x.A * x.B), Diff = g.Sum(x => x.A - x.B), MaxProd = g.Max(x => x.A * x.B) })
            .OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.Cat)
            .Select(g => new { g.Key, Prod = g.Sum(x => x.A * x.B), Diff = g.Sum(x => x.A - x.B), MaxProd = g.Max(x => x.A * x.B) })
            .OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.Prod, r.Diff, r.MaxProd)),
                     actual.Select(r => (r.Key, r.Prod, r.Diff, r.MaxProd)));
    }

    [Fact]
    public void GroupBy_computed_key_average_of_expression_matches_linq()
    {
        var expected = Rows.GroupBy(r => r.B % 2).Select(g => new { g.Key, Avg = g.Average(x => x.A * 2) })
            .OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().GroupBy(r => r.B % 2).Select(g => new { g.Key, Avg = g.Average(x => x.A * 2) })
            .OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.Avg)), actual.Select(r => (r.Key, r.Avg)));
    }
}
