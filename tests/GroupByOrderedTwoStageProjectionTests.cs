using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A two-stage projection over a server-side grouped query with an OrderBy BETWEEN the GroupBy and the
/// result-Select — source.Select(...).GroupBy(k).OrderBy(g => g.Key).Select(g => new { Key, Agg }).Select(x => x.Agg)
/// — matched neither SelectTranslator collapse (one requires GroupBy directly under the result-Select, the
/// other handles ordering placed AFTER the result-Select). It fell through, the outer projection was stored
/// but never emitted, and the materializer read the grouped columns positionally — returning the group KEY
/// instead of the aggregate. Silent data loss.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByOrderedTwoStageProjectionTests
{
    [Table("GotRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int G { get; set; }
        public int A { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GotRow (Id INTEGER PRIMARY KEY, G INTEGER NOT NULL, A INTEGER NOT NULL);" +
                "INSERT INTO GotRow VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Ordered_grouped_two_stage_projection_returns_the_aggregate_not_the_key()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Source is a Select projection (stays server-side); OrderBy sits between GroupBy and the result-Select;
        // a second Select reads the aggregate member.
        var totals = ctx.Query<Row>()
            .Select(r => new { r.G, r.A })
            .GroupBy(x => x.G)
            .OrderBy(g => g.Key)
            .Select(g => new { g.Key, S = g.Sum(x => x.A) })
            .Select(x => x.S)
            .ToList();

        // Group G=1 -> 30, G=2 -> 5 (ordered by key). The bug returned the keys [1, 2].
        Assert.Equal(new[] { 30, 5 }, totals.ToArray());
    }

    [Fact]
    public void Ordered_grouped_two_stage_projection_reshape_keeps_members_aligned()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var rows = ctx.Query<Row>()
            .Select(r => new { r.G, r.A })
            .GroupBy(x => x.G)
            .OrderBy(g => g.Key)
            .Select(g => new { g.Key, S = g.Sum(x => x.A) })
            .Select(x => new { x.S, x.Key })   // reshape — S and Key must not swap
            .ToList();

        Assert.Equal((30, 1), (rows[0].S, rows[0].Key));
        Assert.Equal((5, 2), (rows[1].S, rows[1].Key));
    }
}
