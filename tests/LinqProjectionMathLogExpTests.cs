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
/// Strict pin + implement-first for <c>Math.Log(x)</c> and <c>Math.Exp(x)</c>
/// in projection. The existing scientific-functions test covers Sqrt/Pow/
/// Sign/Truncate; Log + Exp were missing and have a SILENT-WRONGNESS trap:
///
///   * <c>Math.Log(x)</c> in .NET is NATURAL log (base e).
///   * SQLite's <c>log(x)</c> is BASE-10 in some builds, natural in others
///     -- the behavior depends on the build flags / extension version.
///     SQLite's portable natural log is <c>ln(x)</c>.
///   * If SCV falls through to a raw <c>LOG(x)</c> emit, the result is
///     0.43429... for <c>Math.Log(e)</c> instead of the expected 1.0.
///
/// Implementation maps explicitly:
///   Math.Log(x) -> ln(x)         (natural log)
///   Math.Log10(x) -> log10(x)    (base-10)
///   Math.Exp(x) -> exp(x)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathLogExpTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmleItem (Id INTEGER PRIMARY KEY, Value REAL NOT NULL);
            -- Math.E = 2.7182818284590451; the natural log of E is exactly 1.0,
            -- making the silent-wrongness trap of base-10 visible (would yield ~0.434).
            INSERT INTO PmleItem VALUES
                (1, 2.718281828459045),
                (2, 1.0),
                (3, 0.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmleItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Log_returns_natural_log_for_E()
    {
        var result = await _ctx.Query<PmleItem>()
            .Where(p => p.Id == 1)
            .Select(p => new { L = Math.Log(p.Value) })
            .FirstAsync();
        // Silent-wrongness: log10 emission would return ~0.434 here.
        Assert.Equal(1.0, result.L, 8);
    }

    [Fact]
    public async Task Select_Math_Log_returns_zero_for_one()
    {
        var result = await _ctx.Query<PmleItem>()
            .Where(p => p.Id == 2)
            .Select(p => new { L = Math.Log(p.Value) })
            .FirstAsync();
        Assert.Equal(0.0, result.L, 10);
    }

    [Fact]
    public async Task Select_Math_Exp_returns_e_for_one_and_one_for_zero()
    {
        // Exp(1) = e ~ 2.71828; Exp(0) = 1.
        var resultOne = await _ctx.Query<PmleItem>()
            .Where(p => p.Id == 2)
            .Select(p => new { E = Math.Exp(p.Value) })
            .FirstAsync();
        Assert.Equal(Math.E, resultOne.E, 8);

        var resultZero = await _ctx.Query<PmleItem>()
            .Where(p => p.Id == 3)
            .Select(p => new { E = Math.Exp(p.Value) })
            .FirstAsync();
        Assert.Equal(1.0, resultZero.E, 10);
    }

    [Table("PmleItem")]
    public sealed class PmleItem
    {
        [Key] public int Id { get; set; }
        public double Value { get; set; }
    }
}
