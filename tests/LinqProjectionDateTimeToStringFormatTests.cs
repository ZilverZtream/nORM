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
/// Strict pin for <c>DateTime.ToString(format)</c> in projection. Common
/// shape: project a date column as a display-formatted string. SQLite's
/// strftime maps cleanly for the common format specifiers; mapping the
/// .NET tokens to strftime tokens is the work.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeToStringFormatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtsfItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdtsfItem VALUES
                (1, '2026-05-25 12:34:56'),
                (2, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtsfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_ToString_iso_date_format_returns_yyyy_MM_dd_per_row()
    {
        var r = await _ctx.Query<PdtsfItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.Stamp.ToString("yyyy-MM-dd") }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("2026-05-25", r[0].S);
        Assert.Equal("2026-12-31", r[1].S);
    }

    [Table("PdtsfItem")]
    public sealed class PdtsfItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
