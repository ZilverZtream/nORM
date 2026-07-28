using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Ordering or grouping directly on a DateTime/TimeOnly subtraction (`x.End - x.Start`, a
/// computed TimeSpan) must sort/group by the true duration. The raw SQL `(End - Start)` over two
/// ISO-TEXT columns collapses under SQLite numeric affinity to `2020 - 2020 = 0` for every row,
/// so all sort keys tie (rows come back in insertion order) and every distinct duration falls
/// into one group — silently, with no exception. The comparison and projection paths already
/// lower the subtraction to difference-in-seconds; the ORDER BY / GROUP BY key paths must too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeSubtractionOrderGroupKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE SubRow (Id INTEGER PRIMARY KEY, Start TEXT NOT NULL, End TEXT NOT NULL," +
            " TStart TEXT NOT NULL, TEnd TEXT NOT NULL);" +
            // Ids are inserted 1,2,3 but durations are deliberately NOT monotonic in Id, so
            // insertion order (what the affinity-collapsed sort falls back to) matches neither
            // the ascending nor the descending duration order — both order tests stay meaningful.
            // DateTime durations by Id: 1=12h, 2=10d, 3=9d23h59m59s.
            //   ascending  => [1,3,2]   descending => [2,3,1]   insertion => [1,2,3]
            // TimeOnly durations by Id: 1=45m, 2=15m, 3=30m (all share the '08' hour prefix, so
            //   SQLite numeric affinity truncates every TEnd/TStart to 8 => diff 0 for all rows).
            //   ascending => [2,3,1]
            "INSERT INTO SubRow VALUES " +
            "(1,'2020-01-05 00:00:00','2020-01-05 12:00:00','08:00:00','08:45:00')," +
            "(2,'2020-01-01 00:00:00','2020-01-11 00:00:00','08:00:00','08:15:00')," +
            "(3,'2020-01-01 00:00:00','2020-01-10 23:59:59','08:00:00','08:30:00');";
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SubRow>().HasKey(r => r.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public void OrderBy_datetime_subtraction_sorts_by_duration()
    {
        var ids = _ctx.Query<SubRow>()
            .OrderBy(x => x.End - x.Start)
            .Select(x => x.Id)
            .ToList();
        Assert.Equal(new[] { 1, 3, 2 }, ids);   // 12h < 9d23h59m59s < 10d
    }

    [Fact]
    public void OrderByDescending_datetime_subtraction_sorts_by_duration()
    {
        var ids = _ctx.Query<SubRow>()
            .OrderByDescending(x => x.End - x.Start)
            .Select(x => x.Id)
            .ToList();
        Assert.Equal(new[] { 2, 3, 1 }, ids);   // 10d > 9d23h59m59s > 12h
    }

    [Fact]
    public void GroupBy_datetime_subtraction_forms_one_group_per_distinct_duration()
    {
        var counts = _ctx.Query<SubRow>()
            .GroupBy(x => x.End - x.Start)
            .Select(g => g.Count())
            .ToList();
        Assert.Equal(3, counts.Count);           // three distinct durations => three groups
        Assert.All(counts, c => Assert.Equal(1, c));
    }

    [Fact]
    public async Task GroupBy_datetime_subtraction_key_materializes_to_correct_timespan()
    {
        var groups = await _ctx.Query<SubRow>()
            .GroupBy(x => x.End - x.Start)
            .Select(g => new { g.Key, C = g.Count() })
            .ToListAsync();
        var keys = groups.Select(g => g.Key).OrderBy(k => k).ToList();
        Assert.Equal(new[]
        {
            TimeSpan.FromHours(12),
            TimeSpan.FromDays(9) + new TimeSpan(23, 59, 59),
            TimeSpan.FromDays(10),
        }, keys);
    }

    [Fact]
    public void OrderBy_timeonly_subtraction_sorts_by_duration()
    {
        var ids = _ctx.Query<SubRow>()
            .OrderBy(x => x.TEnd - x.TStart)
            .Select(x => x.Id)
            .ToList();
        Assert.Equal(new[] { 2, 3, 1 }, ids);   // 15m < 30m < 45m
    }

    [Table("SubRow")]
    public sealed class SubRow
    {
        [Key] public int Id { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
        public TimeOnly TStart { get; set; }
        public TimeOnly TEnd { get; set; }
    }
}
