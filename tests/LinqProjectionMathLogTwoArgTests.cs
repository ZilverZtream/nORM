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
/// Strict pin + implement-first for the 2-arg <c>Math.Log(value, newBase)</c>
/// overload in projection. .NET's signature is <c>Log(x, b)</c> meaning
/// log-base-b of x. SQLite's signature is <c>log(b, x)</c> -- same order
/// AS .NET in modern SQLite (3.45+). The 1-arg overload already works
/// (natural log per ef61bb4); this run pins the 2-arg.
///
/// Silent-wrongness shape:
///   * Argument order swap -> log(x, b) instead of log(b, x) -- returns
///     log_x(b) instead of log_b(x). For Log(8, 2) the correct answer is
///     3 (since 2^3=8), but log_8(2) returns ~0.333.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathLogTwoArgTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmltItem (Id INTEGER PRIMARY KEY, Value REAL NOT NULL);
            INSERT INTO PmltItem VALUES
                (1, 8.0),
                (2, 16.0),
                (3, 1000.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmltItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Log_with_base_2_returns_log2_per_row()
    {
        var result = await _ctx.Query<PmltItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, L = Math.Log(p.Value, 2.0) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(3.0, result[0].L, 8);   // log2(8) = 3
        Assert.Equal(4.0, result[1].L, 8);   // log2(16) = 4
        Assert.Equal(Math.Log2(1000), result[2].L, 8);
    }

    [Fact]
    public async Task Select_Math_Log_with_base_10_returns_log10_per_row()
    {
        var result = await _ctx.Query<PmltItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, L = Math.Log(p.Value, 10.0) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(Math.Log10(8), result[0].L, 8);
        Assert.Equal(Math.Log10(16), result[1].L, 8);
        Assert.Equal(3.0, result[2].L, 8);   // log10(1000) = 3
    }

    [Table("PmltItem")]
    public sealed class PmltItem
    {
        [Key] public int Id { get; set; }
        public double Value { get; set; }
    }
}
