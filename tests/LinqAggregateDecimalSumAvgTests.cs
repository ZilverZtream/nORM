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
/// Strict pin set for SumAsync / AverageAsync over decimal columns,
/// including the precision-tradeoff dimensions called out in the
/// post-2002200 review:
///   * mixed magnitude (10.5 vs 2.0 ordering correctness)
///   * negative values
///   * nullable decimal? with NULL rows mixed in
///   * grouped aggregates (GroupBy + Sum)
///   * many-decimal fractional values inside the 15-digit double window
///
/// PRECISION CAVEAT: SQLite has no native DECIMAL storage; nORM stores
/// decimal as TEXT and aggregates via CAST AS REAL (8d795f4 / 2002200).
/// REAL is IEEE-754 double -- roughly 15-17 significant decimal digits.
/// Values OUTSIDE that window (e.g. 19-digit accounting totals) will
/// silently round on aggregate. For money-grade precision, use the
/// SqlServer/Postgres providers which have native DECIMAL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateDecimalSumAvgTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AdsaItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL, NV TEXT, Cat TEXT NOT NULL);
            INSERT INTO AdsaItem VALUES
                (1, '10.5',     '10.5',  'a'),
                (2, '2.0',      NULL,    'a'),
                (3, '1.5',      '1.5',   'b'),
                (4, '100.0',    '100.0', 'b'),
                (5, '-12.75',   NULL,    'a'),
                (6, '0.123456789012', '0.0', 'b');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AdsaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SumAsync_decimal_column_returns_numerically_correct_total()
    {
        var sum = await _ctx.Query<AdsaItem>().SumAsync(p => p.V);
        // 10.5 + 2.0 + 1.5 + 100.0 + (-12.75) + 0.123456789012
        // = 101.373456789012
        Assert.Equal(101.373456789012m, sum, precision: 9);
    }

    [Fact]
    public async Task AverageAsync_decimal_column_returns_numerically_correct_average()
    {
        var avg = await _ctx.Query<AdsaItem>().AverageAsync(p => p.V);
        // 101.373456789012 / 6 ≈ 16.8955761315
        Assert.Equal(16.8955761315m, avg, precision: 8);
    }

    [Fact]
    public async Task SumAsync_nullable_decimal_column_skips_null_rows()
    {
        // Microsoft.Data.Sqlite + nORM treat SUM(NULL) as the standard SQL
        // behaviour: NULL rows are skipped. 10.5 + 1.5 + 100.0 + 0.0 = 112.0
        var sum = await _ctx.Query<AdsaItem>().SumAsync(p => p.NV);
        Assert.Equal(112.0m, sum!.Value, precision: 9);
    }

    [Fact]
    public async Task SumAsync_decimal_with_large_magnitude_inside_double_window_is_exact()
    {
        // 10-digit integer + 2-digit fraction stays well inside double's
        // ~15-17 significant decimal digits. Pin documents the safe range.
        await using var c = _cn.CreateCommand();
        c.CommandText = "INSERT INTO AdsaItem VALUES (100, '1234567890.12', NULL, 'big');";
        await c.ExecuteNonQueryAsync();
        var sum = await _ctx.Query<AdsaItem>().Where(p => p.Cat == "big").SumAsync(p => p.V);
        Assert.Equal(1234567890.12m, sum, precision: 2);
    }

    [Fact]
    public async Task GroupBy_Sum_decimal_returns_correct_per_group_total()
    {
        // 'a' group: 10.5 + 2.0 + -12.75 = -0.25
        // 'b' group: 1.5 + 100.0 + 0.123456789012 = 101.623456789012
        var groups = await _ctx.Query<AdsaItem>()
            .GroupBy(p => p.Cat)
            .Select(g => new { Cat = g.Key, Total = g.Sum(x => x.V) })
            .OrderBy(g => g.Cat)
            .ToListAsync();
        Assert.Equal(2, groups.Count);
        Assert.Equal("a", groups[0].Cat);
        Assert.Equal(-0.25m, groups[0].Total, precision: 9);
        Assert.Equal("b", groups[1].Cat);
        Assert.Equal(101.623456789012m, groups[1].Total, precision: 9);
    }

    [Table("AdsaItem")]
    public sealed class AdsaItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
        public decimal? NV { get; set; }
        public string Cat { get; set; } = string.Empty;
    }
}
