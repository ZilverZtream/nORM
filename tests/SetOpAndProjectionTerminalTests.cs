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
/// Oracle-compared coverage for terminals over set operations and over scalar projections:
/// Union/Concat/Except(...).Count()/Sum()/Max(), and First/Single over a projected scalar. These
/// compose a set op or a projection with a scalar terminal — places a missing fold could crash or
/// aggregate the wrong set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SetOpAndProjectionTerminalTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SoptRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 24).Select(i => new Row
    {
        Id = i,
        A = i % 6,           // 0..5
        B = (i * 2) % 9,     // 0..8
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SoptRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO SoptRow VALUES ({r.Id},{r.A},{r.B});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static (List<int> nA, List<int> nB) N(DbContext ctx)
        => (ctx.Query<Row>().Where(r => r.A >= 2).Select(r => r.A).ToList(),
            ctx.Query<Row>().Where(r => r.B >= 4).Select(r => r.B).ToList());

    [Fact]
    public void Union_scalar_count_matches_linq()
    {
        var expected = Rows.Where(r => r.A >= 2).Select(r => r.A)
            .Union(Rows.Where(r => r.B >= 4).Select(r => r.B)).Count();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.A >= 2).Select(r => r.A)
            .Union(ctx.Query<Row>().Where(r => r.B >= 4).Select(r => r.B)).Count();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Union_scalar_sum_matches_linq()
    {
        var expected = Rows.Where(r => r.A >= 2).Select(r => r.A)
            .Union(Rows.Where(r => r.B >= 4).Select(r => r.B)).Sum();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.A >= 2).Select(r => r.A)
            .Union(ctx.Query<Row>().Where(r => r.B >= 4).Select(r => r.B)).Sum();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Union_scalar_max_and_min_match_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(
            Rows.Where(r => r.A >= 2).Select(r => r.A).Union(Rows.Where(r => r.B >= 4).Select(r => r.B)).Max(),
            ctx.Query<Row>().Where(r => r.A >= 2).Select(r => r.A).Union(ctx.Query<Row>().Where(r => r.B >= 4).Select(r => r.B)).Max());
        Assert.Equal(
            Rows.Select(r => r.A).Union(Rows.Select(r => r.B)).Min(),
            ctx.Query<Row>().Select(r => r.A).Union(ctx.Query<Row>().Select(r => r.B)).Min());
    }

    [Fact]
    public void Concat_scalar_sum_matches_linq()
    {
        // Concat = UNION ALL (keeps duplicates), so the sum includes duplicates.
        var expected = Rows.Where(r => r.A >= 2).Select(r => r.A)
            .Concat(Rows.Where(r => r.B >= 4).Select(r => r.B)).Sum();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.A >= 2).Select(r => r.A)
            .Concat(ctx.Query<Row>().Where(r => r.B >= 4).Select(r => r.B)).Sum();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Union_computed_scalar_sum_and_average_match_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(
            Rows.Select(r => r.A - r.B).Union(Rows.Select(r => r.A + r.B)).Sum(),
            ctx.Query<Row>().Select(r => r.A - r.B).Union(ctx.Query<Row>().Select(r => r.A + r.B)).Sum());
        Assert.Equal(
            Rows.Select(r => r.A * 2).Union(Rows.Select(r => r.B * 3)).Average(),
            ctx.Query<Row>().Select(r => r.A * 2).Union(ctx.Query<Row>().Select(r => r.B * 3)).Average(), 6);
    }

    [Fact]
    public void Concat_scalar_count_matches_linq()
    {
        // Concat preserves duplicates; Count must count the multiset.
        var expected = Rows.Where(r => r.A >= 2).Select(r => r.A)
            .Concat(Rows.Where(r => r.B >= 4).Select(r => r.B)).Count();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.A >= 2).Select(r => r.A)
            .Concat(ctx.Query<Row>().Where(r => r.B >= 4).Select(r => r.B)).Count();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Projected_scalar_first_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.A * 10 + r.B).First();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.A * 10 + r.B).First();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Projected_scalar_first_with_predicate_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.A).First(a => a >= 3);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.A).First(a => a >= 3);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Projected_computed_scalar_firstordefault_with_predicate_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.A * 10 + r.B).FirstOrDefault(v => v > 40);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.A * 10 + r.B).FirstOrDefault(v => v > 40);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Entity_first_with_predicate_still_matches_linq()
    {
        // Regression guard for the visit-order reorder: plain entity First(pred) must still work.
        var expected = Rows.OrderBy(r => r.Id).First(r => r.A >= 3).Id;
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).First(r => r.A >= 3).Id;
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Projected_scalar_any_and_all_match_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(Rows.Select(r => r.A).Any(a => a == 5), ctx.Query<Row>().Select(r => r.A).Any(a => a == 5));
        Assert.Equal(Rows.Select(r => r.A).All(a => a < 6), ctx.Query<Row>().Select(r => r.A).All(a => a < 6));
    }
}
