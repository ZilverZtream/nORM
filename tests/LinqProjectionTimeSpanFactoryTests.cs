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
/// Probe pin for `Select(p =&gt; TimeSpan.FromHours(p.H))` and FromMinutes
/// / FromSeconds in projection. The translator lowers each factory to a
/// SQL expression that formats the per-row duration as 'HH:MM:SS' text
/// which the existing TimeSpan materializer parses via TimeSpan.Parse.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanFactoryTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtsfItem (Id INTEGER PRIMARY KEY, H REAL NOT NULL, M REAL NOT NULL, S REAL NOT NULL);
            INSERT INTO PtsfItem VALUES
                (1, 2.0,  30.0, 45.0),
                (2, 0.5,  90.0, 3600.0),
                (3, 1.25, 0.0,  0.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtsfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeSpan_FromHours_of_column_materializes_per_row_TimeSpan()
    {
        var rows = await _ctx.Query<PtsfItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromHours(p.H) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(TimeSpan.FromHours(2.0),  rows[0].Span);
        Assert.Equal(TimeSpan.FromHours(0.5),  rows[1].Span);
        Assert.Equal(TimeSpan.FromHours(1.25), rows[2].Span);
    }

    [Fact]
    public async Task Select_TimeSpan_FromMinutes_of_column_materializes_per_row_TimeSpan()
    {
        var rows = await _ctx.Query<PtsfItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromMinutes(p.M) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(TimeSpan.FromMinutes(30.0), rows[0].Span);
        Assert.Equal(TimeSpan.FromMinutes(90.0), rows[1].Span);
        Assert.Equal(TimeSpan.FromMinutes(0.0),  rows[2].Span);
    }

    [Fact]
    public async Task Select_TimeSpan_FromSeconds_of_column_materializes_per_row_TimeSpan()
    {
        var rows = await _ctx.Query<PtsfItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Span = TimeSpan.FromSeconds(p.S) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(TimeSpan.FromSeconds(45.0),   rows[0].Span);
        Assert.Equal(TimeSpan.FromSeconds(3600.0), rows[1].Span);
        Assert.Equal(TimeSpan.FromSeconds(0.0),    rows[2].Span);
    }

    [Table("PtsfItem")]
    public sealed class PtsfItem
    {
        [Key] public int Id { get; set; }
        public double H { get; set; }
        public double M { get; set; }
        public double S { get; set; }
    }
}
