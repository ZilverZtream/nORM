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
/// Strict pin for <c>DateTime.IsLeapYear(int)</c> in projection.
/// Gregorian rule: divisible by 4, except centuries that aren't
/// divisible by 400. Expressible in pure SQL via modulo arithmetic:
///   (y % 4 = 0 AND y % 100 != 0) OR (y % 400 = 0)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeIsLeapYearTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtlyItem (Id INTEGER PRIMARY KEY, Yr INTEGER NOT NULL);
            INSERT INTO PdtlyItem VALUES
                (1, 2024),  -- divisible by 4, not century -> leap
                (2, 2025),  -- not divisible by 4 -> not leap
                (3, 1900),  -- century not div by 400 -> NOT leap
                (4, 2000);  -- century div by 400 -> leap
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtlyItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_IsLeapYear_int_column_flags_gregorian_leap_per_row()
    {
        var r = await _ctx.Query<PdtlyItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, L = DateTime.IsLeapYear(p.Yr) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.True(r[0].L);    // 2024
        Assert.False(r[1].L);   // 2025
        Assert.False(r[2].L);   // 1900
        Assert.True(r[3].L);    // 2000
    }

    [Table("PdtlyItem")]
    public sealed class PdtlyItem
    {
        [Key] public int Id { get; set; }
        public int Yr { get; set; }
    }
}
