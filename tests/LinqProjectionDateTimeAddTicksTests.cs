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
/// Strict pin for <c>DateTime.AddTicks(long)</c> in projection. Sister
/// of the AddDays/AddHours/etc family already mapped (pinned in
/// c03a128). Common shape for high-precision DateTime arithmetic.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeAddTicksTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtatItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdtatItem VALUES
                (1, '2026-05-25 12:00:00'),
                (2, '2026-12-31 23:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtatItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_AddTicks_one_hour_returns_advanced_per_row()
    {
        // One hour = 36_000_000_000 ticks.
        var r = await _ctx.Query<PdtatItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.AddTicks(36_000_000_000L) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 5, 25, 13, 0, 0), r[0].D);
        Assert.Equal(new DateTime(2027, 1, 1, 0, 0, 0), r[1].D);
    }

    [Table("PdtatItem")]
    public sealed class PdtatItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
