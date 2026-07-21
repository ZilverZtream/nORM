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
/// Oracle-compared coverage for TOP-LEVEL aggregates (Sum/Average/Max/Min/Count) over COMPOSED
/// sources: filtered, projected, and paged (OrderBy+Take/Skip) queries, with computed selectors and
/// nullable columns. A paged aggregate that aggregates the whole table instead of the window, or a
/// computed selector that binds the wrong expression, changes the scalar result without throwing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class TopLevelAggregateCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("TlaRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int? N { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 30).Select(i => new Row
    {
        Id = i,
        A = (i * 5) % 17,
        B = (i % 6) + 1,
        N = i % 4 == 0 ? (int?)null : i * 2,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TlaRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, N INTEGER NULL);";
            foreach (var r in Rows)
                cmd.CommandText += $"INSERT INTO TlaRow VALUES ({r.Id},{r.A},{r.B},{(r.N.HasValue ? r.N.Value.ToString() : "NULL")});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Filtered_sum_of_computed_selector_matches_linq()
    {
        var expected = Rows.Where(r => r.A > 3).Sum(r => r.A * r.B);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.A > 3).Sum(r => r.A * r.B);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Paged_average_aggregates_only_the_window()
    {
        // OrderBy+Take(5) then Average — must average the top-5 window, not the whole table.
        var expected = Rows.OrderByDescending(r => r.A).ThenBy(r => r.Id).Take(5).Average(r => r.B);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderByDescending(r => r.A).ThenBy(r => r.Id).Take(5).Average(r => r.B);
        Assert.Equal(expected, actual, 6);
    }

    [Fact]
    public void Paged_sum_after_skip_take_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Skip(3).Take(7).Sum(r => r.A);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Skip(3).Take(7).Sum(r => r.A);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Projected_then_max_matches_linq()
    {
        var expected = Rows.Where(r => r.B >= 2).Select(r => r.A - r.B).Max();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.B >= 2).Select(r => r.A - r.B).Max();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Nullable_sum_and_max_over_filter_match_linq()
    {
        // Sum over a nullable column (SQL SUM ignores NULLs; LINQ Sum(int?) treats null as 0-ish per
        // Enumerable). Max over nullable returns the max non-null.
        var expected = (Sum: Rows.Where(r => r.Id > 5).Sum(r => r.N), Max: Rows.Where(r => r.Id > 5).Max(r => r.N));
        using var ctx = Ctx();
        var actualSum = ctx.Query<Row>().Where(r => r.Id > 5).Sum(r => r.N);
        var actualMax = ctx.Query<Row>().Where(r => r.Id > 5).Max(r => r.N);
        Assert.Equal(expected.Sum, actualSum);
        Assert.Equal(expected.Max, actualMax);
    }

    [Fact]
    public void Count_and_longcount_with_predicate_match_linq()
    {
        var expected = (C: Rows.Count(r => r.A % 2 == 0), L: Rows.LongCount(r => r.N != null));
        using var ctx = Ctx();
        var actualC = ctx.Query<Row>().Count(r => r.A % 2 == 0);
        var actualL = ctx.Query<Row>().LongCount(r => r.N != null);
        Assert.Equal(expected.C, actualC);
        Assert.Equal(expected.L, actualL);
    }
}
