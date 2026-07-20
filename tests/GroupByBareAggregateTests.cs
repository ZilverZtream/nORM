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
/// A bare aggregate projection over a grouping — GroupBy(k).Select(g => g.Count()) / g.Sum(x => x.V) —
/// returns the scalar aggregate per group. nORM emitted both the group key and the aggregate as two columns,
/// so the scalar materializer read column 0 (the key) and silently returned the group keys instead of the
/// counts/sums. The keyed form (new { Key, Count }) was unaffected. Found by the coverage-guided query-IR fuzzer.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByBareAggregateTests
{
    [Table("GbRow")]
    public class Row { [Key] public int Id { get; set; } public int A { get; set; } }

    private static DbContext Ctx(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        // A = 4,4,2,3,4,0,4 -> groups A=4:{Id 1,2,5,7}, A=2:{3}, A=3:{4}, A=0:{6}.
        cmd.CommandText = "CREATE TABLE GbRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL);" +
            "INSERT INTO GbRow VALUES (1,4),(2,4),(3,2),(4,3),(5,4),(6,0),(7,4);";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
    }

    [Fact]
    public void Bare_count_returns_group_sizes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var counts = ctx.Query<Row>().GroupBy(r => r.A).Select(g => g.Count()).ToList().OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 1, 1, 4 }, counts);
    }

    [Fact]
    public void Bare_sum_returns_group_sums()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        // Id sums: A=4 -> 1+2+5+7=15, A=2 -> 3, A=3 -> 4, A=0 -> 6.
        var sums = ctx.Query<Row>().GroupBy(r => r.A).Select(g => g.Sum(x => x.Id)).ToList().OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 3, 4, 6, 15 }, sums);
    }

    [Fact]
    public void Keyed_count_projection_still_works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var byKey = ctx.Query<Row>().GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() })
            .ToList().OrderBy(x => x.Key).ToArray();
        Assert.Equal(new[] { (0, 1), (2, 1), (3, 1), (4, 4) }, byKey.Select(x => (x.Key, x.C)).ToArray());
    }
}
