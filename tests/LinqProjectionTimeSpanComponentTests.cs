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
/// Strict pin + implement-first for column-side TimeSpan component access
/// (<c>.Hours</c>, <c>.Minutes</c>, <c>.Seconds</c>) in projection AND Where.
/// Microsoft.Data.Sqlite stores TimeSpan as 'HH:mm:ss' canonical text
/// (TimeSpan.ToString('c') format) for sub-day spans, so the component
/// extraction is a string-slice operation per component.
///
/// Silent-wrongness shapes:
///   * .Hours collapsing to .TotalHours -> integer 1 vs floating 1.5 for
///     a 1h30m span -- semantically very different.
///   * .Minutes collapsing to .TotalMinutes -> 30 vs 90.
///   * Either direction throwing instead of returning the component.
///
/// Scope: sub-day TimeSpans only (matches Microsoft.Data.Sqlite's 'HH:mm:ss'
/// binding). Multi-day spans ('d.HH:mm:ss') are out of scope for v1; a future
/// iteration could extend the strftime equivalent via SUBSTR/INSTR.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanComponentTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtsItem (Id INTEGER PRIMARY KEY, Duration TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var spans = new (int id, TimeSpan dur)[]
        {
            (1, new TimeSpan(0, 5, 30)),     // 00:05:30 -> H=0 M=5 S=30
            (2, new TimeSpan(1, 30, 0)),     // 01:30:00 -> H=1 M=30 S=0
            (3, new TimeSpan(2, 15, 45)),    // 02:15:45 -> H=2 M=15 S=45
            (4, new TimeSpan(0, 0, 7)),      // 00:00:07 -> H=0 M=0 S=7
            (5, new TimeSpan(23, 59, 59)),   // 23:59:59 -> H=23 M=59 S=59
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PtsItem (Id, Duration) VALUES ($id, $d)";
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
            OnModelCreating = mb => mb.Entity<PtsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_column_TimeSpan_Hours_extracts_hour_component_per_row()
    {
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, H = i.Duration.Hours })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(0, result[0].H);
        Assert.Equal(1, result[1].H);
        Assert.Equal(2, result[2].H);
        Assert.Equal(0, result[3].H);
        Assert.Equal(23, result[4].H);
    }

    [Fact]
    public async Task Select_column_TimeSpan_Minutes_extracts_minute_component_per_row()
    {
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, M = i.Duration.Minutes })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(5, result[0].M);
        Assert.Equal(30, result[1].M);
        Assert.Equal(15, result[2].M);
        Assert.Equal(0, result[3].M);
        Assert.Equal(59, result[4].M);
    }

    [Fact]
    public async Task Select_column_TimeSpan_Seconds_extracts_second_component_per_row()
    {
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, S = i.Duration.Seconds })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(30, result[0].S);
        Assert.Equal(0, result[1].S);
        Assert.Equal(45, result[2].S);
        Assert.Equal(7, result[3].S);
        Assert.Equal(59, result[4].S);
    }

    [Fact]
    public async Task Where_with_column_TimeSpan_Hours_threshold_filters_rows()
    {
        // Duration.Hours >= 2 -> {Id 3 (2h), Id 5 (23h)}. Silent-wrongness:
        // collapsing to TotalHours -> Id 2 (1.5) AND Id 3 (2.25) AND Id 5
        // (23.999) which is a different set.
        var result = await _ctx.Query<PtsItem>()
            .Where(i => i.Duration.Hours >= 2)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PtsItem")]
    public sealed class PtsItem
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
