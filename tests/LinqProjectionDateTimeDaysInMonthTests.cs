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
/// Strict pin for <c>DateTime.DaysInMonth(year, month)</c> in projection.
/// CASE-based month-length table with the leap-year exception for
/// February. Sister to DateTime.IsLeapYear.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeDaysInMonthTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtdimItem (Id INTEGER PRIMARY KEY, Yr INTEGER NOT NULL, Mo INTEGER NOT NULL);
            INSERT INTO PdtdimItem VALUES
                (1, 2024, 2),   -- leap Feb -> 29
                (2, 2025, 2),   -- non-leap Feb -> 28
                (3, 2024, 7),   -- July -> 31
                (4, 2024, 4),   -- April -> 30
                (5, 1900, 2);   -- century non-leap Feb -> 28
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtdimItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_DaysInMonth_two_columns_returns_correct_count_per_row()
    {
        var r = await _ctx.Query<PdtdimItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateTime.DaysInMonth(p.Yr, p.Mo) }).ToListAsync();
        Assert.Equal(new[] { 29, 28, 31, 30, 28 }, r.Select(x => x.D).ToArray());
    }

    [Table("PdtdimItem")]
    public sealed class PdtdimItem
    {
        [Key] public int Id { get; set; }
        public int Yr { get; set; }
        public int Mo { get; set; }
    }
}
