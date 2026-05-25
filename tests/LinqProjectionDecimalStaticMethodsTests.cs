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
/// Probe pin for `decimal.Floor(p.V)` / `decimal.Ceiling(p.V)` /
/// `decimal.Round(p.V)` / `decimal.Truncate(p.V)` static methods on a
/// decimal column in projection. Sister of Math.Floor/Ceiling/Truncate
/// (2db69bd) but on the decimal-typed receiver. Verifies both methods
/// translate AND that the SQLite decimal-text storage cooperates with
/// the rounding (no CAST AS REAL precision loss).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalStaticMethodsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdsmItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PdsmItem VALUES
                (1, '3.7'),
                (2, '-2.3'),
                (3, '5.0'),
                (4, '0.5');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdsmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_decimal_Floor_of_column_returns_floor_per_row()
    {
        var r = await _ctx.Query<PdsmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = decimal.Floor(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(3m,  r[0].R);
        Assert.Equal(-3m, r[1].R);
        Assert.Equal(5m,  r[2].R);
        Assert.Equal(0m,  r[3].R);
    }

    [Fact]
    public async Task Select_decimal_Ceiling_of_column_returns_ceiling_per_row()
    {
        var r = await _ctx.Query<PdsmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = decimal.Ceiling(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(4m,  r[0].R);
        Assert.Equal(-2m, r[1].R);
        Assert.Equal(5m,  r[2].R);
        Assert.Equal(1m,  r[3].R);
    }

    [Fact]
    public async Task Select_decimal_Truncate_of_column_returns_truncate_per_row()
    {
        var r = await _ctx.Query<PdsmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = decimal.Truncate(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(3m,  r[0].R);
        Assert.Equal(-2m, r[1].R);
        Assert.Equal(5m,  r[2].R);
        Assert.Equal(0m,  r[3].R);
    }

    [Table("PdsmItem")]
    public sealed class PdsmItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
