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
/// Probe pin for `Select(p =&gt; Math.Pow(p.X, 2.0))` with a constant
/// exponent. The constant 2.0 in the expression tree is a
/// ConstantExpression; the SQL should embed it as a literal or
/// parameter and call POWER(col, 2.0) per provider. Verifies the
/// Math.Pow path handles constant args correctly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathPowConstantExponentTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmpceItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL);
            INSERT INTO PmpceItem VALUES (1, 2.0), (2, 3.0), (3, 10.0), (4, 0.5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmpceItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Pow_column_squared_returns_per_row_square()
    {
        var rows = await _ctx.Query<PmpceItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Pow(p.X, 2.0) }).ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(4.0,   rows[0].R, precision: 9);
        Assert.Equal(9.0,   rows[1].R, precision: 9);
        Assert.Equal(100.0, rows[2].R, precision: 9);
        Assert.Equal(0.25,  rows[3].R, precision: 9);
    }

    [Fact]
    public async Task Select_Math_Pow_column_cubed_returns_per_row_cube()
    {
        var rows = await _ctx.Query<PmpceItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Pow(p.X, 3.0) }).ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Equal(8.0,    rows[0].R, precision: 9);
        Assert.Equal(27.0,   rows[1].R, precision: 9);
        Assert.Equal(1000.0, rows[2].R, precision: 9);
        Assert.Equal(0.125,  rows[3].R, precision: 9);
    }

    [Table("PmpceItem")]
    public sealed class PmpceItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
    }
}
