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
/// Probe pin for `Where(p =&gt; p.Stamp > threshold.UtcDateTime)` where
/// p.Stamp is DateTime and threshold is a closure-captured
/// DateTimeOffset. The .UtcDateTime member returns DateTime; the
/// comparison should resolve as DateTime-to-DateTime via normal type
/// promotion, exercising the DateTime CAST AS REAL / datetime() path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeOffsetUtcDateTimeCrossCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtoxItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO WdtoxItem VALUES
                (1, '2025-01-01 00:00:00'),
                (2, '2026-06-15 12:00:00'),
                (3, '2027-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtoxItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTime_column_greater_than_DateTimeOffset_UtcDateTime_closure_filters_chronologically()
    {
        var thresholdOffset = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ids = await _ctx.Query<WdtoxItem>()
            .Where(p => p.Stamp > thresholdOffset.UtcDateTime)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        // Stamps > 2026-01-01 UTC: ids 2 (2026-06-15), 3 (2027-12-31).
        Assert.Equal(new[] { 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdtoxItem")]
    public sealed class WdtoxItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
