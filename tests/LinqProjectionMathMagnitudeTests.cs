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
/// Strict pins for <c>Math.MaxMagnitude</c> and <c>Math.MinMagnitude</c>
/// in projection. Both return whichever of two values has the larger
/// (resp. smaller) absolute value. SQLite has no direct primitive but
/// the simple CASE-on-ABS expression matches .NET for non-equal
/// magnitudes (the common case in column data). Equal-magnitude
/// tie-breaks follow IEEE 754 sign rules in .NET; tests use distinct
/// magnitudes so the CASE result agrees with .NET.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathMagnitudeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmgItem (Id INTEGER PRIMARY KEY, A REAL NOT NULL, B REAL NOT NULL);
            INSERT INTO PmgItem VALUES
                (1,  3.0, -5.0),    -- |3|=3, |-5|=5; Max returns -5; Min returns 3
                (2, -7.0,  2.0),    -- |-7|=7, |2|=2; Max returns -7; Min returns 2
                (3,  4.0,  1.0);    -- |4|=4, |1|=1; Max returns 4; Min returns 1
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmgItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_MaxMagnitude_two_columns_returns_larger_abs_per_row()
    {
        var r = await _ctx.Query<PmgItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.MaxMagnitude(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.MaxMagnitude(3.0, -5.0), r[0].V, 6);
        Assert.Equal(Math.MaxMagnitude(-7.0, 2.0), r[1].V, 6);
        Assert.Equal(Math.MaxMagnitude(4.0, 1.0), r[2].V, 6);
    }

    [Fact]
    public async Task Select_Math_MinMagnitude_two_columns_returns_smaller_abs_per_row()
    {
        var r = await _ctx.Query<PmgItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.MinMagnitude(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(Math.MinMagnitude(3.0, -5.0), r[0].V, 6);
        Assert.Equal(Math.MinMagnitude(-7.0, 2.0), r[1].V, 6);
        Assert.Equal(Math.MinMagnitude(4.0, 1.0), r[2].V, 6);
    }

    [Table("PmgItem")]
    public sealed class PmgItem
    {
        [Key] public int Id { get; set; }
        public double A { get; set; }
        public double B { get; set; }
    }
}
