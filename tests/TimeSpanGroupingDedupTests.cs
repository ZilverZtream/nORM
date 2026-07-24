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
/// GROUP BY / DISTINCT over a <see cref="TimeSpan"/> column must key by duration, not by stored TEXT
/// format. Rows 1 and 2 are both one day (canonical and extra-fraction TEXT), so they belong to ONE group
/// and collapse to ONE distinct value. Parallel of the TimeOnly exact-key fix.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TimeSpanGroupingDedupTests
{
    [Table("TsGrp_Test")]
    public sealed class TsRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE TsGrp_Test (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);" +
                "INSERT INTO TsGrp_Test VALUES (1,'1.00:00:00'),(2,'1.00:00:00.0000000'),(3,'2.00:00:00');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task GroupBy_timespan_keys_by_duration()
    {
        using var ctx = NewCtx();
        var groups = await ctx.Query<TsRow>()
            .GroupBy(r => r.Duration)
            .Select(g => new { g.Key, N = g.Count() })
            .ToListAsync();
        Assert.Equal(2, groups.Count); // { 1 day -> 2, 2 days -> 1 }
        Assert.Equal(2, groups.Single(g => g.Key == TimeSpan.FromDays(1)).N);
    }

    [Fact]
    public async Task Distinct_timespan_collapses_equal_values()
    {
        using var ctx = NewCtx();
        var distinct = await ctx.Query<TsRow>().Select(r => new { r.Duration }).Distinct().ToListAsync();
        Assert.Equal(2, distinct.Count); // 1 day and 2 days
    }
}
