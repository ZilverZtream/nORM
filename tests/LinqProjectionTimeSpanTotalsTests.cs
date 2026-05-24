using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Strict pin for column-side <c>TimeSpan.TotalHours</c>,
/// <c>TotalMinutes</c>, <c>TotalSeconds</c> in projection AND Where. These
/// are the "duration arithmetic" properties that return double rather than
/// the component getters that return int. Sister to <c>b17440e</c>'s
/// Hours/Minutes/Seconds work; uses the same SUBSTR slices into Microsoft
/// .Data.Sqlite's canonical 'HH:mm:ss' binding plus a sum-and-divide.
///
/// Silent-wrongness shapes:
///   * .TotalMinutes collapsing to .Minutes -> integer 30 vs floating 90.0
///     for 1h30m -- different scale AND wrong type.
///   * Integer division of a TimeSpan sum (e.g. dividing seconds by 60
///     without forcing REAL) returns truncated minutes instead of fractional.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanTotalsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PttItem (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var spans = new (int id, TimeSpan dur)[]
        {
            (1, new TimeSpan(0, 30, 0)),     // 0.5h / 30min / 1800s
            (2, new TimeSpan(1, 30, 0)),     // 1.5h / 90min / 5400s
            (3, new TimeSpan(2, 0, 0)),      // 2h / 120min / 7200s
            (4, new TimeSpan(0, 0, 45)),     // 0.0125h / 0.75min / 45s
            (5, new TimeSpan(3, 15, 30)),    // 3.2583..h / 195.5min / 11730s
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PttItem (Id, Duration) VALUES ($id, $d)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var pd = insert.CreateParameter(); pd.ParameterName = "$d"; insert.Parameters.Add(pd);
        foreach (var (id, dur) in spans)
        {
            pid.Value = id;
            pd.Value = dur.ToString("c");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PttItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_column_TimeSpan_TotalSeconds_projects_total_seconds_per_row()
    {
        var result = await _ctx.Query<PttItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, S = i.Duration.TotalSeconds })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(1800.0, result[0].S);
        Assert.Equal(5400.0, result[1].S);
        Assert.Equal(7200.0, result[2].S);
        Assert.Equal(45.0, result[3].S);
        Assert.Equal(11730.0, result[4].S);
    }

    [Fact]
    public async Task Select_column_TimeSpan_TotalMinutes_projects_total_minutes_per_row()
    {
        var result = await _ctx.Query<PttItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, M = i.Duration.TotalMinutes })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(30.0, result[0].M);
        Assert.Equal(90.0, result[1].M);
        Assert.Equal(120.0, result[2].M);
        Assert.Equal(0.75, result[3].M);
        Assert.Equal(195.5, result[4].M);
    }

    [Fact]
    public async Task Select_column_TimeSpan_TotalHours_projects_fractional_hours_per_row()
    {
        var result = await _ctx.Query<PttItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, H = i.Duration.TotalHours })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(0.5, result[0].H);
        Assert.Equal(1.5, result[1].H);
        Assert.Equal(2.0, result[2].H);
        Assert.Equal(45.0 / 3600.0, result[3].H, 10);
        Assert.Equal(11730.0 / 3600.0, result[4].H, 10);
    }

    [Fact]
    public async Task Where_with_column_TimeSpan_TotalMinutes_threshold_filters_rows()
    {
        // Duration.TotalMinutes >= 90 -> {Id 2 (90), Id 3 (120), Id 5 (195.5)}.
        // Silent-wrongness: collapse to .Minutes -> {Id 1 (30), 2 (30), 5 (15)}
        // doesn't even match the >= predicate consistently -- visibly broken.
        var result = await _ctx.Query<PttItem>()
            .Where(i => i.Duration.TotalMinutes >= 90)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PttItem")]
    public sealed class PttItem
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
