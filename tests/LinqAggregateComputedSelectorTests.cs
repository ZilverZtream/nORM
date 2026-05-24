using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of aggregate operators whose selector is a
/// computed expression rather than a plain column reference:
/// <c>Sum(x => x.Price * x.Quantity)</c>, <c>Min/Max(x => x.A - x.B)</c>,
/// <c>Average(x => x.A + x.B)</c>. The translator must push the arithmetic
/// into the SQL aggregate (e.g. <c>SUM(Price * Quantity)</c>) rather than
/// either rejecting the call or materializing every row to compute the
/// aggregate client-side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateComputedSelectorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AcLine (Id INTEGER PRIMARY KEY, Price INTEGER NOT NULL, Quantity INTEGER NOT NULL);
            INSERT INTO AcLine VALUES (1, 10, 2), (2, 5, 4), (3, 8, 3);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Sum_with_multiplicative_selector_emits_arithmetic_in_sql()
    {
        // 10*2 + 5*4 + 8*3 = 20 + 20 + 24 = 64
        var total = await _ctx.Query<AcLine>().SumAsync(l => l.Price * l.Quantity);
        Assert.Equal(64, total);
    }

    [Fact]
    public async Task Min_with_subtractive_selector_emits_arithmetic_in_sql()
    {
        // (10-2)=8, (5-4)=1, (8-3)=5 → Min = 1
        var min = await _ctx.Query<AcLine>().MinAsync(l => l.Price - l.Quantity);
        Assert.Equal(1, min);
    }

    [Fact]
    public async Task Max_with_additive_selector_emits_arithmetic_in_sql()
    {
        // (10+2)=12, (5+4)=9, (8+3)=11 → Max = 12
        var max = await _ctx.Query<AcLine>().MaxAsync(l => l.Price + l.Quantity);
        Assert.Equal(12, max);
    }

    [Fact]
    public async Task Average_with_multiplicative_selector_emits_arithmetic_in_sql()
    {
        // (20 + 20 + 24) / 3 = 64 / 3 ≈ 21.333. Cast to double so the selector returns
        // double — keeps the arithmetic floating-point at the SQL layer and avoids the
        // integer-truncation that hides the bug we're pinning here.
        var avg = await _ctx.Query<AcLine>().AverageAsync(l => (double)l.Price * l.Quantity);
        Assert.Equal(64.0 / 3.0, avg, 3);
    }

    [Table("AcLine")]
    public sealed class AcLine
    {
        [Key] public int Id { get; set; }
        public int Price { get; set; }
        public int Quantity { get; set; }
    }
}
