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
/// Oracle-compared coverage for window-function BASE results (RowNumber / Rank / DenseRank) materialized
/// directly. Oracles: RowNumber = 1-based ordered index; Rank = 1 + count strictly smaller; DenseRank =
/// 1 + distinct smaller.
///
/// NOTE: operations composed AFTER a window function — e.g. WithRowNumber(...).Where(x => x.Rn &lt;= 3)
/// (top-N-per-group) or a projection over the rank — are NOT yet supported: the following Select/Where is
/// inlined into the window result selector, which the window builder rejects ("Window function projection
/// must be an anonymous object initializer"). Implementing that (wrap the window as a derived table and
/// apply the following operator over the window alias) is a tracked feature gap; those shapes are omitted
/// here rather than pinned as failing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class WindowFunctionCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("WfcRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Score { get; set; }
    }

    private static readonly (int Id, int Score)[] Data =
    {
        (1, 50), (2, 90), (3, 50), (4, 70), (5, 90), (6, 30), (7, 70), (8, 50), (9, 10), (10, 90),
    };

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE WfcRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);";
            foreach (var d in Data) cmd.CommandText += $"INSERT INTO WfcRow VALUES ({d.Id},{d.Score});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void RowNumber_base_matches_linq()
    {
        var expected = Data.OrderByDescending(d => d.Score).ThenBy(d => d.Id)
            .Select((d, i) => (d.Id, Rn: i + 1)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderByDescending(r => r.Score).ThenBy(r => r.Id)
            .WithRowNumber((r, rn) => new { r.Id, Rn = rn })
            .ToList().Select(x => (x.Id, x.Rn)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Rank_base_matches_linq()
    {
        // Rank over Score DESC only (no unique tiebreak), so equal scores share a rank. Each Id's rank
        // is a deterministic function of its score, so compare (Id, Rank) as a set sorted by Id.
        var expected = Data.Select(d => (d.Id, Rk: 1 + Data.Count(o => o.Score > d.Score))).OrderBy(x => x.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderByDescending(r => r.Score)
            .WithRank((r, rk) => new { r.Id, Rk = rk })
            .ToList().Select(x => (x.Id, x.Rk)).OrderBy(x => x.Id).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void DenseRank_base_matches_linq()
    {
        var expected = Data.Select(d => (d.Id, Dr: 1 + Data.Select(o => o.Score).Distinct().Count(s => s > d.Score)))
            .OrderBy(x => x.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderByDescending(r => r.Score)
            .WithDenseRank((r, dr) => new { r.Id, Dr = dr })
            .ToList().Select(x => (x.Id, x.Dr)).OrderBy(x => x.Id).ToList();
        Assert.Equal(expected, actual);
    }
}
