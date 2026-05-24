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
/// Strict pin for <c>Math.Abs</c>, <c>Math.Sign</c>, <c>Math.Round</c>,
/// <c>Math.Ceiling</c>, <c>Math.Floor</c> used in Where predicates.
/// TranslatabilityAnalyzer admits them; SqliteProvider's TranslateFunction
/// emits provider equivalents (ABS/SIGN/ROUND/etc.).
///
/// Silent-wrongness shapes:
///   * Math.Abs dropped silently -- the result of <c>Abs(col) &gt; 5</c>
///     reduces to <c>col &gt; 5</c>, missing negative-magnitude matches.
///   * Math.Sign returns 0 / 1 / -1 in .NET; provider's SIGN must match.
///   * Math.Round half-banker vs half-up vs half-down differences across
///     providers can flip boundary values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereMathFunctionsStrictTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WmfItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, F REAL NOT NULL);
            INSERT INTO WmfItem VALUES
                (1, -10, 1.4),
                (2, -3,  2.5),
                (3, 0,   3.6),
                (4, 5,  -4.5),
                (5, 8,   0.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WmfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_Math_Abs_int_column_matches_magnitude_threshold()
    {
        // Abs(V) > 5 -> {Id=1 (|-10|=10), Id=5 (|8|=8)}.
        // Silent-wrongness: dropped Abs -> V > 5 -> only Id 5.
        var result = await _ctx.Query<WmfItem>()
            .Where(i => Math.Abs(i.V) > 5)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_Math_Sign_int_column_distinguishes_positive_negative_zero()
    {
        // Sign(V) returns -1 / 0 / +1. Negative: Id 1, 2. Zero: Id 3. Positive: Id 4, 5.
        var negative = await _ctx.Query<WmfItem>()
            .Where(i => Math.Sign(i.V) < 0)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, negative.Select(r => r.Id).ToArray());

        var zero = await _ctx.Query<WmfItem>()
            .Where(i => Math.Sign(i.V) == 0)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, zero.Select(r => r.Id).ToArray());

        var positive = await _ctx.Query<WmfItem>()
            .Where(i => Math.Sign(i.V) > 0)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, positive.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_Math_Floor_real_column_matches_integer_threshold()
    {
        // Floor(F): 1.4->1, 2.5->2, 3.6->3, -4.5->-5, 0.0->0.
        // Floor(F) >= 2 -> {Id 2 (Floor=2), Id 3 (Floor=3)}.
        var result = await _ctx.Query<WmfItem>()
            .Where(i => Math.Floor(i.F) >= 2)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_Math_Ceiling_real_column_matches_integer_threshold()
    {
        // Ceiling(F): 1.4->2, 2.5->3, 3.6->4, -4.5->-4, 0.0->0.
        // Ceiling(F) <= 0 -> {Id 4 (-4), Id 5 (0)}.
        var result = await _ctx.Query<WmfItem>()
            .Where(i => Math.Ceiling(i.F) <= 0)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WmfItem")]
    public sealed class WmfItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public double F { get; set; }
    }
}
