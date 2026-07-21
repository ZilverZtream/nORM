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
/// Oracle-compared coverage for a GroupBy projection with an INTERMEDIATE OrderBy/ThenBy between the two
/// Selects — the common report shape <c>GroupBy(k).Select(g =&gt; new { g.Key, N = g.Count() })
/// .OrderBy(x =&gt; x.Key).Select(x =&gt; ...)</c>. Previously the outer Select over the ordered grouping
/// was SILENTLY DROPPED (only the group key came back). The translator now composes both the outer
/// projection and the order keys through the inner GroupBy projection and rebuilds as a single
/// ordered-then-computed GroupBy. Each case runs the identical LINQ expression against nORM (SQLite) and
/// LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupByOrderedThenProjectedTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GotpRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int G { get; set; }
        public int V { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    { Id = i, G = i % 4, V = i }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE GotpRow (Id INTEGER PRIMARY KEY, G INTEGER NOT NULL, V INTEGER NOT NULL);";
        foreach (var r in Rows)
            cmd.CommandText += $"INSERT INTO GotpRow VALUES ({r.Id},{r.G},{r.V});";
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
    public void OrderBy_key_then_concat_projection() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.Key)
        .Select(x => x.Key + ":" + x.C));

    [Fact]
    public void OrderBy_key_then_interpolation() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.Key)
        .Select(x => $"{x.Key}:{x.C}"));

    [Fact]
    public void OrderByDescending_key_then_projection() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.V) }).OrderByDescending(x => x.Key)
        .Select(x => $"{x.Key}:{x.C}:{x.S}"));

    [Fact]
    public void OrderBy_aggregate_member_then_projection() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, S = g.Sum(x => x.V) }).OrderBy(x => x.S)
        .Select(x => $"{x.Key}={x.S}"));

    [Fact]
    public void OrderBy_then_thenby_then_projection() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.V) }).OrderBy(x => x.C).ThenBy(x => x.Key)
        .Select(x => $"{x.Key}:{x.C}:{x.S}"));

    [Fact]
    public void OrderBy_computed_key_then_projection() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.C - x.Key)
        .Select(x => $"{x.Key}:{x.C}"));

    [Fact]
    public void OrderBy_key_then_anonymous_projection() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.Key)
        .Select(x => new { x.Key, Doubled = x.C * 2 })
        .Select(x => $"{x.Key}/{x.Doubled}"));

    [Fact]
    public void OrderBy_aggregate_outer_key_only() => Assert_(q => q.GroupBy(r => r.G)
        .Select(g => new { g.Key, S = g.Sum(x => x.V) }).OrderBy(x => x.S)
        .Select(x => x.Key));
}
