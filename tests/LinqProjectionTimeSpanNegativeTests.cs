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
/// with a negative argument. The canonical 'c' format prefixes the
/// entire span with `-`: e.g. TimeSpan.FromHours(-1).ToString("c") =
/// "-01:00:00". The unified emit must produce the same shape so
/// TimeSpan.Parse round-trips correctly and WHERE comparisons against
/// negative parameters work.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanNegativeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtsnItem (Id INTEGER PRIMARY KEY, H REAL NOT NULL);
            INSERT INTO PtsnItem VALUES
                (1, -1.0),
                (2, -0.5),
                (3, -48.0),
                (4,  0.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtsnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeSpan_FromHours_negative_column_round_trips()
    {
        var rows = await _ctx.Query<PtsnItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromHours(p.H) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(TimeSpan.FromHours(-1.0),  rows[0].Span);
        Assert.Equal(TimeSpan.FromHours(-0.5),  rows[1].Span);
        Assert.Equal(TimeSpan.FromHours(-48.0), rows[2].Span);  // -2 days
        Assert.Equal(TimeSpan.Zero,             rows[3].Span);
    }

    [Table("PtsnItem")]
    public sealed class PtsnItem
    {
        [Key] public int Id { get; set; }
        public double H { get; set; }
    }
}
