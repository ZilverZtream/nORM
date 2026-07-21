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
/// Oracle-compared coverage for scalar-projection + Distinct + terminal compositions:
/// Select(x => scalar).Distinct().Count()/Sum()/Max()/Min(), and Distinct scalar ordering.
/// These compose a scalar projection, a set-dedup, and an aggregate/terminal — each a place a
/// missing fold could aggregate the wrong (non-deduped) set or crash.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DistinctScalarTerminalTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DstRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    // A deliberately has heavy duplication so distinct-vs-non-distinct aggregates differ.
    private static readonly Row[] Rows = Enumerable.Range(1, 30).Select(i => new Row
    {
        Id = i,
        A = i % 5,        // values 0..4, each repeated 6×
        B = (i % 7) + 1,  // 1..7
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DstRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            foreach (var r in Rows) cmd.CommandText += $"INSERT INTO DstRow VALUES ({r.Id},{r.A},{r.B});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Distinct_scalar_count_matches_linq()
    {
        var expected = Rows.Select(r => r.A).Distinct().Count(); // 5, not 30
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => r.A).Distinct().Count();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Distinct_scalar_sum_matches_linq()
    {
        var expected = Rows.Select(r => r.A).Distinct().Sum(); // 0+1+2+3+4=10, not 30*avg
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => r.A).Distinct().Sum();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Distinct_scalar_max_and_min_match_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(Rows.Select(r => r.A).Distinct().Max(), ctx.Query<Row>().Select(r => r.A).Distinct().Max());
        Assert.Equal(Rows.Select(r => r.B).Distinct().Min(), ctx.Query<Row>().Select(r => r.B).Distinct().Min());
    }

    [Fact]
    public void Filtered_distinct_scalar_sum_matches_linq()
    {
        // Pre-Distinct Where must be inside the distinct set: sum the distinct A values among rows
        // with B >= 3. (The Count() form of this shape is a separate open bug in the Count path.)
        var expected = Rows.Where(r => r.B >= 3).Select(r => r.A).Distinct().Sum();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.B >= 3).Select(r => r.A).Distinct().Sum();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Distinct_scalar_ordered_matches_linq()
    {
        var expected = Rows.Select(r => r.A).Distinct().OrderByDescending(x => x).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => r.A).Distinct().OrderByDescending(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Distinct_computed_scalar_sum_matches_linq()
    {
        var expected = Rows.Select(r => r.A * 10 + r.B % 2).Distinct().Sum();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => r.A * 10 + r.B % 2).Distinct().Sum();
        Assert.Equal(expected, actual);
    }
}
