using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// C# DateOnly.AddMonths/AddYears clamp the day to the target month's length
/// (Jan 31 + 1 month = Feb 29 in a leap year); SQLite's 'months'/'years' modifiers
/// normalize the overflowed day forward into the next month. TimeOnly.IsBetween
/// supports midnight-crossing ranges (22:00..02:00 contains 23:30 and 01:00).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateOnlyCalendarClampTests
{
    [Table("DoClamp_Event")]
    private class Event
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly At { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DoClamp_Event (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Day TEXT NOT NULL,
                    At TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Event { Day = new DateOnly(2020, 1, 31), At = new TimeOnly(23, 30, 0) });
        ctx.Add(new Event { Day = new DateOnly(2020, 2, 29), At = new TimeOnly(1, 0, 0) });
        ctx.Add(new Event { Day = new DateOnly(2020, 6, 15), At = new TimeOnly(12, 0, 0) });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return (cn, ctx);
    }

    [Fact]
    public void AddMonths_clamps_end_of_month_days()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Event>().OrderBy(e => e.Id).Select(e => e.Day.AddMonths(1)).ToList();
        var expected = new[]
        {
            new DateOnly(2020, 1, 31).AddMonths(1),  // 2020-02-29
            new DateOnly(2020, 2, 29).AddMonths(1),  // 2020-03-29
            new DateOnly(2020, 6, 15).AddMonths(1),  // 2020-07-15
        };
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AddYears_clamps_leap_day()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Event>().OrderBy(e => e.Id).Select(e => e.Day.AddYears(1)).ToList();
        var expected = new[]
        {
            new DateOnly(2020, 1, 31).AddYears(1),   // 2021-01-31
            new DateOnly(2020, 2, 29).AddYears(1),   // 2021-02-28
            new DateOnly(2020, 6, 15).AddYears(1),   // 2021-06-15
        };
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AddMonths_in_predicate_uses_clamped_date()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var target = new DateOnly(2020, 2, 29);
        var ids = ctx.Query<Event>().Where(e => e.Day.AddMonths(1) == target).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void TimeOnly_IsBetween_handles_midnight_crossing_range()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var from = new TimeOnly(22, 0, 0);
        var to = new TimeOnly(2, 0, 0);
        // C#: wraparound range contains 23:30 and 01:00, not 12:00.
        var ids = ctx.Query<Event>().Where(e => e.At.IsBetween(from, to)).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public void TimeOnly_IsBetween_handles_ordinary_range()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var from = new TimeOnly(11, 0, 0);
        var to = new TimeOnly(13, 0, 0);
        var ids = ctx.Query<Event>().Where(e => e.At.IsBetween(from, to)).Select(e => e.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 3 }, ids);
    }
}
