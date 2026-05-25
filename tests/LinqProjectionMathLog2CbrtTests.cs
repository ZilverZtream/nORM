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
/// Strict pins for <c>Math.Log2(double)</c> and <c>Math.Cbrt(double)</c>
/// in projection. Both are .NET Core+ additions; users reach for them
/// in analytical queries (bit-width via log2, geometric means via cbrt).
/// SQLite 3.35+ ships log2() and pow() built-ins, so both can be
/// translated directly: log2 is identity, cbrt is pow(x, 1.0/3.0).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathLog2CbrtTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmlcItem (Id INTEGER PRIMARY KEY, Value REAL NOT NULL);
            INSERT INTO PmlcItem VALUES
                (1, 8.0),
                (2, 27.0),
                (3, 64.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmlcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Log2_double_column_projects_log2_per_row()
    {
        var result = await _ctx.Query<PmlcItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, L = Math.Log2(p.Value) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(3.0, result[0].L, 6);   // log2(8) = 3
        Assert.Equal(Math.Log2(27.0), result[1].L, 6);
        Assert.Equal(6.0, result[2].L, 6);   // log2(64) = 6
    }

    [Fact]
    public async Task Select_Math_Cbrt_double_column_projects_cube_root_per_row()
    {
        var result = await _ctx.Query<PmlcItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = Math.Cbrt(p.Value) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(2.0, result[0].C, 6);   // cbrt(8) = 2
        Assert.Equal(3.0, result[1].C, 6);   // cbrt(27) = 3
        Assert.Equal(4.0, result[2].C, 6);   // cbrt(64) = 4
    }

    [Table("PmlcItem")]
    public sealed class PmlcItem
    {
        [Key] public int Id { get; set; }
        public double Value { get; set; }
    }
}
