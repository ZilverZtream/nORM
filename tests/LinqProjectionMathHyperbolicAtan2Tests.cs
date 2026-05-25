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
/// Strict pins for hyperbolic and 2-arg trig in projection:
/// <c>Math.Sinh / Math.Cosh / Math.Tanh / Math.Atan2</c>. SQLite 3.35+
/// math extension exposes sinh, cosh, tanh, atan2 -- direct mappings.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathHyperbolicAtan2Tests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmhatItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL, Y REAL NOT NULL);
            INSERT INTO PmhatItem VALUES
                (1, 0.0, 1.0),
                (2, 1.0, 1.0),
                (3, 2.0, 0.5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmhatItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Sinh_double_column_projects_sinh_per_row()
    {
        var r = await _ctx.Query<PmhatItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Sinh(p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Sinh(0.0), r[0].V, 6);
        Assert.Equal(Math.Sinh(1.0), r[1].V, 6);
        Assert.Equal(Math.Sinh(2.0), r[2].V, 6);
    }

    [Fact]
    public async Task Select_Math_Cosh_double_column_projects_cosh_per_row()
    {
        var r = await _ctx.Query<PmhatItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Cosh(p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Cosh(0.0), r[0].V, 6);
        Assert.Equal(Math.Cosh(1.0), r[1].V, 6);
        Assert.Equal(Math.Cosh(2.0), r[2].V, 6);
    }

    [Fact]
    public async Task Select_Math_Tanh_double_column_projects_tanh_per_row()
    {
        var r = await _ctx.Query<PmhatItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Tanh(p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Tanh(0.0), r[0].V, 6);
        Assert.Equal(Math.Tanh(1.0), r[1].V, 6);
        Assert.Equal(Math.Tanh(2.0), r[2].V, 6);
    }

    [Fact]
    public async Task Select_Math_Atan2_two_double_columns_projects_atan2_per_row()
    {
        var r = await _ctx.Query<PmhatItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Atan2(p.Y, p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Atan2(1.0, 0.0), r[0].V, 6);
        Assert.Equal(Math.Atan2(1.0, 1.0), r[1].V, 6);
        Assert.Equal(Math.Atan2(0.5, 2.0), r[2].V, 6);
    }

    [Table("PmhatItem")]
    public sealed class PmhatItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
    }
}
