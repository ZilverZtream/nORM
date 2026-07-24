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
/// A <see cref="TimeSpan"/> equality predicate must match by duration, not by the stored TEXT's format.
/// SQLite stores TimeSpan as canonical 'c' TEXT; a value written elsewhere with extra fractional digits
/// (<c>"1.00:00:00.0000000"</c>) is the same duration as <c>"1.00:00:00"</c>. The full translator wraps
/// both sides with NormalizeTimeSpanForCompare (fractional seconds) so they match; the read fast paths
/// emit a raw <c>col = @p</c>. Diagnostic probe for a fast-path-vs-full-translator divergence in the same
/// family as the decimal-scale and DateTimeOffset fixes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TimeSpanEqualityFastPathTests
{
    [Table("TsEq_Test")]
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
            // Rows 1 and 2 are both exactly one day, in canonical and extra-fraction TEXT.
            // Rows 4 and 5 expose lexical multi-day mis-ordering: "10.00:00:00" < "9.23:59:59" as strings
            // but 10 days > 9 days 23:59:59 as durations.
            c.CommandText =
                "CREATE TABLE TsEq_Test (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);" +
                "INSERT INTO TsEq_Test VALUES (1,'1.00:00:00'),(2,'1.00:00:00.0000000'),(3,'2.00:00:00')," +
                "(4,'10.00:00:00'),(5,'9.23:59:59');";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task TimeSpan_equality_full_translator_matches_by_duration()
    {
        using var ctx = NewCtx();
        var oneDay = TimeSpan.FromDays(1);
        var ids = await ctx.Query<TsRow>().Where(r => r.Duration == oneDay).Select(r => new { r.Id }).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task TimeSpan_equality_simple_where_matches_by_duration()
    {
        using var ctx = NewCtx();
        var oneDay = TimeSpan.FromDays(1);
        var rows = await ctx.Query<TsRow>().Where(r => r.Duration == oneDay).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task TimeSpan_equality_count_matches_by_duration()
    {
        using var ctx = NewCtx();
        var oneDay = TimeSpan.FromDays(1);
        var n = await ctx.Query<TsRow>().CountAsync(r => r.Duration == oneDay);
        Assert.Equal(2, n);
    }

    [Fact]
    public async Task TimeSpan_equality_filtered_ordered_matches_by_duration()
    {
        using var ctx = NewCtx();
        var oneDay = TimeSpan.FromDays(1);
        var rows = await ctx.Query<TsRow>().Where(r => r.Duration == oneDay).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task TimeSpan_range_filtered_ordered_is_numeric_not_lexical()
    {
        using var ctx = NewCtx();
        var nineDaysAlmost = new TimeSpan(9, 23, 59, 59); // 9d 23:59:59
        // Only row 4 (10 days) exceeds it; lexically "10.00:00:00" < "9.23:59:59" would wrongly exclude it.
        var rows = await ctx.Query<TsRow>().Where(r => r.Duration > nineDaysAlmost).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 4 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task TimeSpan_order_by_is_numeric_not_lexical()
    {
        using var ctx = NewCtx();
        var rows = await ctx.Query<TsRow>().Where(r => r.Id > 0).OrderBy(r => r.Duration).ToListAsync();
        // Durations must be non-decreasing (1d, 1d, 2d, 9d23:59:59, 10d) even though the TEXT sorts
        // lexically ("10.00:00:00" < "2.00:00:00" as strings).
        for (var i = 1; i < rows.Count; i++)
            Assert.True(rows[i - 1].Duration <= rows[i].Duration, $"ORDER BY Duration not numeric at index {i}");
    }
}
