using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies every DateTime member translation against a real database. Each test pins the
/// extracted column value (year, month, day, etc.) to a row count or specific id so a
/// regression in the provider's TranslateFunction shows up immediately.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeMemberTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtRow (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO DtRow VALUES
                (1, '2020-01-15 09:30:45'),
                (2, '2021-06-30 12:00:00'),
                (3, '2022-12-31 23:59:59'),
                (4, '2020-02-29 00:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Year_filters_by_year_component()
    {
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.Year == 2020).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 1, 4 }, hits.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Month_filters_by_month_component()
    {
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.Month == 12).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(3, hits[0].Id);
    }

    [Fact]
    public async Task Day_filters_by_day_component()
    {
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.Day == 29).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task Hour_filters_by_hour_component()
    {
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.Hour == 23).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(3, hits[0].Id);
    }

    [Fact]
    public async Task Minute_filters_by_minute_component()
    {
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.Minute == 30).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Fact]
    public async Task Second_filters_by_second_component()
    {
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.Second == 45).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Fact]
    public async Task DayOfYear_filters_correctly_for_leap_day()
    {
        // 2020-02-29 is the 60th day of the year (leap year).
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.DayOfYear == 60).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task DayOfWeek_filters_by_weekday()
    {
        // 2020-01-15 = Wednesday(3), 2021-06-30 = Wednesday(3), 2022-12-31 = Saturday(6), 2020-02-29 = Saturday(6).
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.DayOfWeek == DayOfWeek.Saturday).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 3, 4 }, hits.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task AddHours_shifts_into_next_day_so_hour_comparison_matches()
    {
        // 2022-12-31 23:59:59 + 1 hour -> 2023-01-01 00:59:59, so the resulting hour is 0.
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.AddHours(1).Hour == 0).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(3, hits[0].Id);
    }

    [Fact]
    public async Task AddMinutes_can_isolate_a_specific_row()
    {
        // 2020-01-15 09:30:45 + 30 minutes -> 10:00:45, hour 10.
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.AddMinutes(30).Hour == 10).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Fact]
    public async Task AddSeconds_increments_minute_when_crossing_boundary()
    {
        // 2020-01-15 09:30:45 + 20s -> 09:31:05, so minute becomes 31.
        var hits = await _ctx.Query<DtRow>().Where(r => r.Stamp.AddSeconds(20).Minute == 31).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Table("DtRow")]
    public sealed class DtRow
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
