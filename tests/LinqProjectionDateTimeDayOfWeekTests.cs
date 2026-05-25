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
/// Strict pin for <c>DateTime.DayOfWeek</c> in projection. .NET
/// DayOfWeek is an int enum (Sunday=0..Saturday=6); SQLite strftime('%w')
/// returns text '0'..'6' matching the same convention. Pin verifies
/// the materializer correctly converts the SQL string to the enum.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeDayOfWeekTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtdowItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdtdowItem VALUES
                (1, '2026-05-25 12:00:00'),  -- Monday
                (2, '2026-05-24 12:00:00'),  -- Sunday
                (3, '2026-05-30 12:00:00');  -- Saturday
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtdowItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_DayOfWeek_returns_correct_enum_per_row()
    {
        var r = await _ctx.Query<PdtdowItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.DayOfWeek }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(DayOfWeek.Monday, r[0].D);
        Assert.Equal(DayOfWeek.Sunday, r[1].D);
        Assert.Equal(DayOfWeek.Saturday, r[2].D);
    }

    [Table("PdtdowItem")]
    public sealed class PdtdowItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
