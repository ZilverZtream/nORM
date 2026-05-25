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
/// Strict pins for the DateTime.Add* family in projection:
/// AddYears / AddMonths / AddDays / AddHours / AddMinutes / AddSeconds /
/// AddMilliseconds. SQLite's date(modifier...) and strftime accept
/// expressive modifier strings; the provider should map each .NET
/// adder to the appropriate modifier.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeAddersTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtaItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdtaItem VALUES
                (1, '2026-05-25 12:00:00'),
                (2, '2026-12-31 23:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_AddYears_one_year_returns_advanced_date_per_row()
    {
        var r = await _ctx.Query<PdtaItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.AddYears(1) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2027, 5, 25, 12, 0, 0), r[0].D);
        Assert.Equal(new DateTime(2027, 12, 31, 23, 0, 0), r[1].D);
    }

    [Fact]
    public async Task Select_DateTime_AddMonths_three_returns_advanced_date_per_row()
    {
        var r = await _ctx.Query<PdtaItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.AddMonths(3) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 8, 25, 12, 0, 0), r[0].D);
        Assert.Equal(new DateTime(2027, 3, 31, 23, 0, 0), r[1].D);
    }

    [Fact]
    public async Task Select_DateTime_AddHours_two_returns_advanced_per_row()
    {
        var r = await _ctx.Query<PdtaItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.AddHours(2) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 5, 25, 14, 0, 0), r[0].D);
        Assert.Equal(new DateTime(2027, 1, 1, 1, 0, 0), r[1].D);
    }

    [Table("PdtaItem")]
    public sealed class PdtaItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
