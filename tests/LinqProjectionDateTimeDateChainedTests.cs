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
/// Strict pin for chained <c>p.Stamp.Date.AddDays(n)</c> in projection.
/// Date drops the time portion; AddDays then advances by n days. Both
/// pieces have provider entries; pin guards the composition.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeDateChainedTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtdcItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO PdtdcItem VALUES
                (1, '2026-05-25 14:30:45'),
                (2, '2026-12-31 23:59:59');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtdcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_Date_AddDays_one_returns_next_midnight_per_row()
    {
        var r = await _ctx.Query<PdtdcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.Stamp.Date.AddDays(1) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateTime(2026, 5, 26, 0, 0, 0), r[0].D);
        Assert.Equal(new DateTime(2027, 1, 1, 0, 0, 0), r[1].D);
    }

    [Table("PdtdcItem")]
    public sealed class PdtdcItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
