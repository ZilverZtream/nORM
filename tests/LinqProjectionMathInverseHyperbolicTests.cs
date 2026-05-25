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
/// Strict pins for inverse hyperbolic in projection: <c>Math.Asinh /
/// Math.Acosh / Math.Atanh</c>. SQLite 3.35+ math extension exposes
/// asinh(), acosh(), atanh() -- direct mappings, sister set to
/// Sinh/Cosh/Tanh just landed.
///
/// Domain notes for the test data:
///   * Asinh accepts all reals.
///   * Acosh requires x >= 1.
///   * Atanh requires -1 < x < 1.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathInverseHyperbolicTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // Asinh: any real; Acosh: >= 1; Atanh: |x| < 1.
        cmd.CommandText = """
            CREATE TABLE PmihItem (Id INTEGER PRIMARY KEY, A REAL NOT NULL, B REAL NOT NULL, C REAL NOT NULL);
            INSERT INTO PmihItem VALUES
                (1, 0.0, 1.0, 0.0),
                (2, 1.0, 2.0, 0.5),
                (3, 2.0, 3.0, -0.25);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmihItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Asinh_double_column_projects_asinh_per_row()
    {
        var r = await _ctx.Query<PmihItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Asinh(p.A) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Asinh(0.0), r[0].V, 6);
        Assert.Equal(Math.Asinh(1.0), r[1].V, 6);
        Assert.Equal(Math.Asinh(2.0), r[2].V, 6);
    }

    [Fact]
    public async Task Select_Math_Acosh_double_column_projects_acosh_per_row()
    {
        var r = await _ctx.Query<PmihItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Acosh(p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Acosh(1.0), r[0].V, 6);
        Assert.Equal(Math.Acosh(2.0), r[1].V, 6);
        Assert.Equal(Math.Acosh(3.0), r[2].V, 6);
    }

    [Fact]
    public async Task Select_Math_Atanh_double_column_projects_atanh_per_row()
    {
        var r = await _ctx.Query<PmihItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Atanh(p.C) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.Atanh(0.0), r[0].V, 6);
        Assert.Equal(Math.Atanh(0.5), r[1].V, 6);
        Assert.Equal(Math.Atanh(-0.25), r[2].V, 6);
    }

    [Table("PmihItem")]
    public sealed class PmihItem
    {
        [Key] public int Id { get; set; }
        public double A { get; set; }
        public double B { get; set; }
        public double C { get; set; }
    }
}
