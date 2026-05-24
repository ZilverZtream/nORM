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
/// Exercises every Math.* translation on a real database. Each test asserts the SQL evaluated
/// the function correctly by comparing against a literal computed from the row data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMathFunctionTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NumRow (Id INTEGER PRIMARY KEY, Val REAL NOT NULL);
            INSERT INTO NumRow VALUES
                (1, -4.0),
                (2,  0.0),
                (3,  9.0),
                (4, 16.0),
                (5,  2.0),
                (6,  3.5);
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
    public async Task Sqrt_filters_perfect_squares()
    {
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Sqrt(r.Val) == 3.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3 }, ids);
    }

    [Fact]
    public async Task Pow_filters_squared_values()
    {
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Pow(r.Val, 2.0) == 16.0).OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Id).ToArray();
        // (-4)^2 = 16 and 4^2 = 16; only 16 exists as 4^2 source for Id=4? Val=-4 -> 16, Val=16 -> 256
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Abs_filters_negative_to_positive()
    {
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Abs(r.Val) == 4.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Floor_truncates_decimal()
    {
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Floor(r.Val) == 3.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 6 }, ids);
    }

    [Fact]
    public async Task Ceiling_rounds_up()
    {
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Ceiling(r.Val) == 4.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 6 }, ids);
    }

    [Fact]
    public async Task Sign_returns_minus_one_for_negative()
    {
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Sign(r.Val) == -1).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Min_picks_smaller_of_two_values()
    {
        // Min(val, 5) == 2.0 selects the row where val=2.0.
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Min(r.Val, 5.0) == 2.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5 }, ids);
    }

    [Fact]
    public async Task Max_picks_larger_of_two_values()
    {
        // Max(val, 5) == 16.0 selects the row where val=16.0.
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Max(r.Val, 5.0) == 16.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4 }, ids);
    }

    [Fact]
    public async Task Truncate_drops_fractional_part()
    {
        // val=3.5 truncates to 3.
        var ids = (await _ctx.Query<NumRow>().Where(r => Math.Truncate(r.Val) == 3.0).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 6 }, ids);
    }

    [Table("NumRow")]
    public sealed class NumRow
    {
        [Key] public int Id { get; set; }
        public double Val { get; set; }
    }
}
