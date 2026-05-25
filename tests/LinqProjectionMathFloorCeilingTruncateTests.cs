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
/// Probe pin for `Math.Floor(p.V)` / `Math.Ceiling(p.V)` / `Math.Truncate(p.V)`
/// in projection. These are common report-rounding operations. SQLite's
/// math extension provides floor / ceil; SQL Server has FLOOR/CEILING;
/// PostgreSQL has floor/ceil/trunc; MySQL has FLOOR/CEILING/TRUNCATE.
/// Verify each materializes the expected value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathFloorCeilingTruncateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmfctItem (Id INTEGER PRIMARY KEY, V REAL NOT NULL);
            INSERT INTO PmfctItem VALUES (1, 3.7), (2, -2.3), (3, 5.0), (4, 0.5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmfctItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Floor_of_column_returns_floor_per_row()
    {
        var r = await _ctx.Query<PmfctItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Floor(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(3.0,  r[0].R);
        Assert.Equal(-3.0, r[1].R);  // floor(-2.3) = -3
        Assert.Equal(5.0,  r[2].R);
        Assert.Equal(0.0,  r[3].R);
    }

    [Fact]
    public async Task Select_Math_Ceiling_of_column_returns_ceiling_per_row()
    {
        var r = await _ctx.Query<PmfctItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Ceiling(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(4.0,  r[0].R);
        Assert.Equal(-2.0, r[1].R);  // ceiling(-2.3) = -2
        Assert.Equal(5.0,  r[2].R);
        Assert.Equal(1.0,  r[3].R);
    }

    [Fact]
    public async Task Select_Math_Truncate_of_column_returns_truncate_per_row()
    {
        var r = await _ctx.Query<PmfctItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Truncate(p.V) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(3.0,  r[0].R);
        Assert.Equal(-2.0, r[1].R);  // truncate(-2.3) = -2 (toward zero)
        Assert.Equal(5.0,  r[2].R);
        Assert.Equal(0.0,  r[3].R);
    }

    [Table("PmfctItem")]
    public sealed class PmfctItem
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
    }
}
