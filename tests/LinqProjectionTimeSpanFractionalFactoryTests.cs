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
/// Probe pin for `TimeSpan.FromMilliseconds(col)` and `TimeSpan.FromTicks(col)`
/// in projection. Sister of 49d489c (FromHours/FromMinutes/FromSeconds)
/// but with fractional-second precision -- the SQL emit must produce
/// 'HH:mm:ss.fffffff' rather than the integer-second 'HH:mm:ss' format
/// because milliseconds/ticks are inherently sub-second.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanFractionalFactoryTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtsffItem (Id INTEGER PRIMARY KEY, Ms INTEGER NOT NULL, Tk INTEGER NOT NULL);
            INSERT INTO PtsffItem VALUES
                (1, 1500, 15000000),     -- 1.5 sec / 1.5 sec
                (2, 125,  1250000),      -- 0.125 sec / 0.125 sec
                (3, 0,    0),            -- zero
                (4, 60000, 600000000);   -- 1 min / 1 min
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtsffItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeSpan_FromMilliseconds_of_column_materializes_with_fractional_precision()
    {
        var rows = await _ctx.Query<PtsffItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromMilliseconds(p.Ms) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(TimeSpan.FromMilliseconds(1500),  rows[0].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(125),   rows[1].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(0),     rows[2].Span);
        Assert.Equal(TimeSpan.FromMilliseconds(60000), rows[3].Span);
    }

    [Fact]
    public async Task Select_TimeSpan_FromTicks_of_column_materializes_with_full_tick_precision()
    {
        var rows = await _ctx.Query<PtsffItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromTicks(p.Tk) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(TimeSpan.FromTicks(15000000),  rows[0].Span);
        Assert.Equal(TimeSpan.FromTicks(1250000),   rows[1].Span);
        Assert.Equal(TimeSpan.FromTicks(0),         rows[2].Span);
        Assert.Equal(TimeSpan.FromTicks(600000000), rows[3].Span);
    }

    [Table("PtsffItem")]
    public sealed class PtsffItem
    {
        [Key] public int Id { get; set; }
        public long Ms { get; set; }
        public long Tk { get; set; }
    }
}
