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
/// Strict pin for <c>DateTime.ToString(format)</c> inside WHERE. Sister
/// to the projection-side fix in cd66470. Real use case:
///   <c>Where(p =&gt; p.Stamp.ToString("yyyy-MM-dd") == "2026-05-25")</c>
/// to filter on the date portion as text. The ETSV path needs the same
/// strftime translation that SCV uses for projections.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeToStringFormatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtsfItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO WdtsfItem VALUES
                (1, '2026-05-25 12:00:00'),
                (2, '2026-05-26 09:30:00'),
                (3, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtsfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTime_ToString_yyyy_MM_dd_returns_matching_day_only()
    {
        var ids = await _ctx.Query<WdtsfItem>()
            .Where(p => p.Stamp.ToString("yyyy-MM-dd") == "2026-05-25")
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_DateTime_ToString_yyyy_MM_returns_matching_month()
    {
        var ids = await _ctx.Query<WdtsfItem>()
            .Where(p => p.Stamp.ToString("yyyy-MM") == "2026-05")
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdtsfItem")]
    public sealed class WdtsfItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
