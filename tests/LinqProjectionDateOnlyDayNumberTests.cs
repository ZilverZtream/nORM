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
/// Strict pin for <c>DateOnly.DayNumber</c> in projection -- days since
/// DateOnly.MinValue (0001-01-01). SQLite expresses this via julianday
/// arithmetic since julianday is calendar-aware. The DayNumber for
/// 0001-01-01 is 0, so subtract the JD of MinValue (1721425.5).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateOnlyDayNumberTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdodnItem (Id INTEGER PRIMARY KEY, D TEXT NOT NULL);
            INSERT INTO PdodnItem VALUES
                (1, '2026-05-25'),
                (2, '2000-01-01');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdodnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateOnly_DayNumber_returns_days_since_min_per_row()
    {
        var r = await _ctx.Query<PdodnItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, N = p.D.DayNumber }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateOnly(2026, 5, 25).DayNumber, r[0].N);
        Assert.Equal(new DateOnly(2000, 1, 1).DayNumber, r[1].N);
    }

    [Table("PdodnItem")]
    public sealed class PdodnItem
    {
        [Key] public int Id { get; set; }
        public DateOnly D { get; set; }
    }
}
