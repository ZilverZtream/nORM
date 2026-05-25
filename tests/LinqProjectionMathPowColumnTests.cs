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
/// Probe pin for `Select(p =&gt; Math.Pow(p.X, p.Y))` and column-with-
/// constant variants. SQLite has a math extension that provides
/// pow()/power() (added in 3.35.0); Microsoft.Data.Sqlite bundles a
/// recent SQLite. The translator should lower Math.Pow to the
/// provider's POWER function.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathPowColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmpcItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL, Y REAL NOT NULL);
            INSERT INTO PmpcItem VALUES
                (1, 2.0, 3.0),    -- 2^3 = 8
                (2, 5.0, 2.0),    -- 5^2 = 25
                (3, 10.0, 0.0),   -- 10^0 = 1
                (4, 4.0, 0.5);    -- 4^0.5 = 2
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmpcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Pow_two_columns_returns_per_row_power()
    {
        var rows = await _ctx.Query<PmpcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Pow(p.X, p.Y) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(8.0,  rows[0].R, precision: 9);
        Assert.Equal(25.0, rows[1].R, precision: 9);
        Assert.Equal(1.0,  rows[2].R, precision: 9);
        Assert.Equal(2.0,  rows[3].R, precision: 9);
    }

    [Fact]
    public async Task Select_Math_Pow_column_with_constant_exponent_returns_per_row_power()
    {
        var rows = await _ctx.Query<PmpcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Pow(p.X, 2.0) })
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(4.0,   rows[0].R, precision: 9);  // 2^2
        Assert.Equal(25.0,  rows[1].R, precision: 9);  // 5^2
        Assert.Equal(100.0, rows[2].R, precision: 9);  // 10^2
        Assert.Equal(16.0,  rows[3].R, precision: 9);  // 4^2
    }

    [Table("PmpcItem")]
    public sealed class PmpcItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
    }
}
