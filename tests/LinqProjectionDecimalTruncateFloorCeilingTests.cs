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
/// Strict pins for <c>decimal.Truncate / decimal.Floor / decimal.Ceiling</c>
/// in projection. Sisters of the Math.* overloads (which act on double).
/// Direct SQLite mappings:
///   decimal.Truncate(d) -> CAST(d AS INTEGER)   -- toward zero
///   decimal.Floor(d)    -> FLOOR(d)
///   decimal.Ceiling(d)  -> CEIL(d)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalTruncateFloorCeilingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtfcItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PdtfcItem VALUES
                (1, '2.7'),
                (2, '-2.7'),
                (3, '5.0'),
                (4, '-0.4');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtfcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_decimal_Truncate_drops_fraction_toward_zero_per_row()
    {
        var r = await _ctx.Query<PdtfcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Truncate(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(2m,  r[0].V);   // 2.7 -> 2
        Assert.Equal(-2m, r[1].V);   // -2.7 -> -2
        Assert.Equal(5m,  r[2].V);   // 5.0 -> 5
        Assert.Equal(0m,  r[3].V);   // -0.4 -> 0
    }

    [Fact]
    public async Task Select_decimal_Floor_rounds_toward_negative_infinity_per_row()
    {
        var r = await _ctx.Query<PdtfcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Floor(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(2m,  r[0].V);   // 2.7 -> 2
        Assert.Equal(-3m, r[1].V);   // -2.7 -> -3
        Assert.Equal(5m,  r[2].V);   // 5.0 -> 5
        Assert.Equal(-1m, r[3].V);   // -0.4 -> -1
    }

    [Fact]
    public async Task Select_decimal_Ceiling_rounds_toward_positive_infinity_per_row()
    {
        var r = await _ctx.Query<PdtfcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Ceiling(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(3m,  r[0].V);   // 2.7 -> 3
        Assert.Equal(-2m, r[1].V);   // -2.7 -> -2
        Assert.Equal(5m,  r[2].V);   // 5.0 -> 5
        Assert.Equal(0m,  r[3].V);   // -0.4 -> 0
    }

    [Table("PdtfcItem")]
    public sealed class PdtfcItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
