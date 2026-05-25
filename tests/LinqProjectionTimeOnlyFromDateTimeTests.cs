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
/// Strict pin for <c>TimeOnly.FromDateTime(DateTime)</c> in projection.
/// Drops the date portion. SQLite's time() emits 'HH:mm:ss' which the
/// materializer parses to TimeOnly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeOnlyFromDateTimeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtoftItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PtoftItem VALUES
                (1, '2026-05-25 14:30:45'),
                (2, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtoftItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeOnly_FromDateTime_DateTime_column_drops_date_per_row()
    {
        var r = await _ctx.Query<PtoftItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = TimeOnly.FromDateTime(p.Stamp) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new TimeOnly(14, 30, 45), r[0].T);
        Assert.Equal(new TimeOnly(23, 59, 59), r[1].T);
    }

    [Table("PtoftItem")]
    public sealed class PtoftItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
