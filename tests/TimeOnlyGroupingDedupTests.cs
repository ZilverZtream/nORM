using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// GROUP BY / DISTINCT over a <see cref="TimeOnly"/> column must key by value, not by stored TEXT format.
/// Rows 1 and 2 are the same time (12:00:00) in canonical and extra-fraction TEXT, so they belong to ONE
/// group and collapse to ONE distinct value. The decimal fix wired its canonical key into grouping/dedup;
/// this checks TimeOnly does too (the equality-only fix would leave grouping keyed on the raw TEXT).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TimeOnlyGroupingDedupTests
{
    [Table("ToGrp_Test")]
    public sealed class ToRow
    {
        [Key] public int Id { get; set; }
        [Column("Tval")] public TimeOnly T { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE ToGrp_Test (Id INTEGER PRIMARY KEY, Tval TEXT NOT NULL);" +
                "INSERT INTO ToGrp_Test VALUES (1,'12:00:00'),(2,'12:00:00.0000000'),(3,'13:30:00');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task GroupBy_timeonly_keys_by_value()
    {
        using var ctx = NewCtx();
        var groups = await ctx.Query<ToRow>()
            .GroupBy(r => r.T)
            .Select(g => new { g.Key, N = g.Count() })
            .ToListAsync();
        Assert.Equal(2, groups.Count); // { 12:00:00 -> 2, 13:30:00 -> 1 }
        Assert.Equal(2, groups.Single(g => g.Key == new TimeOnly(12, 0, 0)).N);
    }

    [Fact]
    public async Task Distinct_timeonly_collapses_equal_values()
    {
        using var ctx = NewCtx();
        var distinct = await ctx.Query<ToRow>().Select(r => new { r.T }).Distinct().ToListAsync();
        Assert.Equal(2, distinct.Count); // 12:00:00 and 13:30:00
    }
}
