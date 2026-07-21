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
/// Oracle-compared coverage for GroupBy projections that combine MULTIPLE / FILTERED / CONDITIONAL
/// aggregates in one shot — the common reporting pattern (per-group max+min+count, Count(predicate),
/// Sum(ternary), decimal max/min/sum/avg, distinct-count-in-group, where-then-sum-in-group,
/// Any/All-in-group, order-by-aggregate top-N groups, HAVING). Each case runs the identical LINQ
/// expression against nORM (SQLite) and LINQ-to-Objects over the same seeded rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupAggregateProjectionCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GapcRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int G { get; set; }
        public int A { get; set; }
        public decimal M { get; set; }
        public bool Flag { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 30).Select(i => new Row
    {
        Id = i,
        G = i % 4,
        A = (i % 2 == 0 ? 1 : -1) * (i % 7),
        M = (i % 5) * 1.5m - 3m,
        Flag = i % 3 == 0,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE GapcRow (Id INTEGER PRIMARY KEY, G INTEGER NOT NULL, A INTEGER NOT NULL, M TEXT NOT NULL, Flag INTEGER NOT NULL);";
        foreach (var r in Rows)
            cmd.CommandText += $"INSERT INTO GapcRow VALUES ({r.Id},{r.G},{r.A},'{r.M.ToString(System.Globalization.CultureInfo.InvariantCulture)}',{(r.Flag ? 1 : 0)});";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Rows.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Multiple_aggregates_in_one_projection() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, Max = g.Max(x => x.A), Min = g.Min(x => x.A), Cnt = g.Count() })
        .Select(x => $"{x.Key}:{x.Max}:{x.Min}:{x.Cnt}"));

    [Fact]
    public void Average_and_sum_together() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, Avg = g.Average(x => x.A), Sum = g.Sum(x => x.A) })
        .Select(x => $"{x.Key}:{x.Avg:F3}:{x.Sum}"));

    [Fact]
    public void Count_with_predicate() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, Pos = g.Count(x => x.A > 0) })
        .Select(x => $"{x.Key}:{x.Pos}"));

    [Fact]
    public void Sum_of_ternary() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, S = g.Sum(x => x.Flag ? x.A : 0) })
        .Select(x => $"{x.Key}:{x.S}"));

    [Fact]
    public void Sum_of_bool_as_int() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, F = g.Sum(x => x.Flag ? 1 : 0) })
        .Select(x => $"{x.Key}:{x.F}"));

    [Fact]
    public void Decimal_max_min_sum() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, Mx = g.Max(x => x.M), Mn = g.Min(x => x.M), Sm = g.Sum(x => x.M) })
        .Select(x => $"{x.Key}:{x.Mx}:{x.Mn}:{x.Sm}"));

    [Fact]
    public void Decimal_average() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, Av = g.Average(x => x.M) })
        .Select(x => $"{x.Key}:{x.Av}"));

    [Fact]
    public void Sum_of_expression() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, S = g.Sum(x => x.A * 2 + 1) })
        .Select(x => $"{x.Key}:{x.S}"));

    [Fact]
    public void Max_of_expression() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, S = g.Max(x => x.A * x.A) })
        .Select(x => $"{x.Key}:{x.S}"));

    [Fact]
    public void OrderBy_aggregate_take_top_groups() => Assert_(q => q.GroupBy(r => r.G)
        .OrderByDescending(g => g.Count()).ThenBy(g => g.Key).Take(2)
        .Select(g => $"{g.Key}:{g.Count()}"));

    [Fact]
    public void Having_on_aggregate() => Assert_(q => q.GroupBy(r => r.G).Where(g => g.Sum(x => x.A) > 0).OrderBy(g => g.Key)
        .Select(g => $"{g.Key}:{g.Sum(x => x.A)}"));

    [Fact]
    public void Distinct_count_in_group() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, D = g.Select(x => x.A).Distinct().Count() })
        .Select(x => $"{x.Key}:{x.D}"));

    [Fact]
    public void Where_then_sum_in_group() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, S = g.Where(x => x.A > 0).Sum(x => x.A) })
        .Select(x => $"{x.Key}:{x.S}"));

    [Fact]
    public void Any_and_all_in_group() => Assert_(q => q.GroupBy(r => r.G).OrderBy(g => g.Key)
        .Select(g => new { g.Key, AnyPos = g.Any(x => x.A > 3), AllPos = g.All(x => x.A >= 0) })
        .Select(x => $"{x.Key}:{x.AnyPos}:{x.AllPos}"));
}
