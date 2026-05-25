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
/// Probe pin for transcendental Math.* functions on column args:
/// Sqrt, Log, Log10, Exp, Sin, Cos. SQLite's math extension provides
/// all of these (>= 3.35.0); verify the translator lowers each.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathTranscendentalTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmtItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL);
            INSERT INTO PmtItem VALUES (1, 4.0), (2, 100.0), (3, 1.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmtItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Sqrt_of_column_returns_square_root_per_row()
    {
        var rows = await _ctx.Query<PmtItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Sqrt(p.X) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(2.0,  rows[0].R, precision: 9);
        Assert.Equal(10.0, rows[1].R, precision: 9);
        Assert.Equal(1.0,  rows[2].R, precision: 9);
    }

    [Fact]
    public async Task Select_Math_Log10_of_column_returns_base10_log_per_row()
    {
        var rows = await _ctx.Query<PmtItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Log10(p.X) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(Math.Log10(4.0),   rows[0].R, precision: 9);
        Assert.Equal(2.0,               rows[1].R, precision: 9);  // log10(100)
        Assert.Equal(0.0,               rows[2].R, precision: 9);  // log10(1)
    }

    [Fact]
    public async Task Select_Math_Exp_of_column_returns_e_to_the_x_per_row()
    {
        var rows = await _ctx.Query<PmtItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Exp(p.X) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(Math.Exp(4.0),   rows[0].R, precision: 6);
        Assert.Equal(Math.Exp(100.0), rows[1].R, precision: 0);  // very large; loose tolerance
        Assert.Equal(Math.E,          rows[2].R, precision: 9);
    }

    [Table("PmtItem")]
    public sealed class PmtItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
    }
}
