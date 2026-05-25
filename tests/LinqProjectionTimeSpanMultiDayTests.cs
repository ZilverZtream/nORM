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
/// Probe pin for `TimeSpan.From{Hours,Minutes,Seconds,Milliseconds,Ticks,Days}`
/// when the result spans more than 24 hours. The unified printf emit
/// in 088d591 produced 'HH:mm:ss.fffffff' which TimeSpan.Parse silently
/// MISINTERPRETS when HH >= 24 -- the first segment becomes days, not
/// hours (e.g. "48:00:00" parses as 48 DAYS, not 2 days). Fix: emit
/// with leading `days.` prefix `d.HH:mm:ss.fffffff` so the day count
/// is unambiguous. .NET's TimeSpan.Parse accepts the prefix even when
/// days=0, so the single emit shape handles all magnitudes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanMultiDayTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtsmdItem (Id INTEGER PRIMARY KEY, H REAL NOT NULL, D REAL NOT NULL);
            INSERT INTO PtsmdItem VALUES
                (1, 48.0,  2.0),    -- 2 days
                (2, 25.5,  1.0625), -- 1 day + 1.5h
                (3, 0.5,   0.5),    -- 12 hours
                (4, 0.0,   0.0);    -- zero
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtsmdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeSpan_FromHours_above_one_day_round_trips_without_days_ambiguity()
    {
        var rows = await _ctx.Query<PtsmdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromHours(p.H) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        // Row 1: 48h = exactly 2 days, NOT 48 days.
        Assert.Equal(TimeSpan.FromHours(48.0),  rows[0].Span);
        Assert.Equal(TimeSpan.FromDays(2.0),    rows[0].Span);  // alias check
        Assert.Equal(TimeSpan.FromHours(25.5),  rows[1].Span);
        Assert.Equal(TimeSpan.FromHours(0.5),   rows[2].Span);
        Assert.Equal(TimeSpan.Zero,             rows[3].Span);
    }

    [Fact]
    public async Task Select_TimeSpan_FromDays_above_one_day_round_trips()
    {
        var rows = await _ctx.Query<PtsmdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromDays(p.D) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(TimeSpan.FromDays(2.0),    rows[0].Span);
        Assert.Equal(TimeSpan.FromDays(1.0625), rows[1].Span);
        Assert.Equal(TimeSpan.FromDays(0.5),    rows[2].Span);
        Assert.Equal(TimeSpan.Zero,             rows[3].Span);
    }

    [Table("PtsmdItem")]
    public sealed class PtsmdItem
    {
        [Key] public int Id { get; set; }
        public double H { get; set; }
        public double D { get; set; }
    }
}
