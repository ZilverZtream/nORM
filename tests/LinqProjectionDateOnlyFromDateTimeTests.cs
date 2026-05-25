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
/// Strict pin for <c>DateOnly.FromDateTime(DateTime)</c> in projection.
/// Drops the time portion. SQLite's date(t) emits 'YYYY-MM-DD' which
/// the materializer parses back to DateOnly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateOnlyFromDateTimeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdoftItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdoftItem VALUES
                (1, '2026-05-25 14:30:00'),
                (2, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdoftItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateOnly_FromDateTime_DateTime_column_drops_time_per_row()
    {
        var r = await _ctx.Query<PdoftItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateOnly.FromDateTime(p.Stamp) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateOnly(2026, 5, 25), r[0].D);
        Assert.Equal(new DateOnly(2026, 12, 31), r[1].D);
    }

    [Table("PdoftItem")]
    public sealed class PdoftItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
