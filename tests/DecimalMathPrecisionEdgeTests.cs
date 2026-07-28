using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Edge coverage for the precision-preserving decimal Math.Abs/Floor/Ceiling/Min/Max path:
/// negatives (Floor rounds toward -inf, Ceiling toward +inf), integral values (no +/-1 adjust),
/// and ordinary money-scale values (no regression). Every result is diffed against the
/// LINQ-to-Objects oracle so the server-side SQL must match .NET exactly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DecimalMathPrecisionEdgeTests
{
    [Table("DmpRow")]
    public class DmpRow { [Key] public int Id { get; set; } public decimal V { get; set; } }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params DmpRow[] rows)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "CREATE TABLE DmpRow (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);"; cmd.ExecuteNonQuery(); }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    // Floor/Ceiling across sign x fraction: -2.5, -2.0, 2.0, 2.5, plus a large negative fractional.
    [Theory]
    [InlineData("-2.5")]
    [InlineData("-2.0")]
    [InlineData("2.0")]
    [InlineData("2.5")]
    [InlineData("-123456789012345.5")]
    [InlineData("123456789012345.5")]
    [InlineData("0.5")]
    [InlineData("-0.5")]
    public async Task Floor_and_Ceiling_match_dotnet(string literal)
    {
        var v = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);
        var seed = new[] { new DmpRow { Id = 1, V = v } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);

        var floor = ctx.Query<DmpRow>().Select(x => Math.Floor(x.V)).ToList().Single();
        Assert.Equal(Math.Floor(v), floor);

        var ceil = ctx.Query<DmpRow>().Select(x => Math.Ceiling(x.V)).ToList().Single();
        Assert.Equal(Math.Ceiling(v), ceil);
    }

    [Theory]
    [InlineData("-123.45")]
    [InlineData("123.45")]
    [InlineData("-123456789012345.67891")]
    [InlineData("0")]
    public async Task Abs_matches_dotnet(string literal)
    {
        var v = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);
        var seed = new[] { new DmpRow { Id = 1, V = v } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var abs = ctx.Query<DmpRow>().Select(x => Math.Abs(x.V)).ToList().Single();
        Assert.Equal(Math.Abs(v), abs);
    }

    // Min/Max against a bound decimal, including a negative operand and a high-precision tie-free pair.
    [Theory]
    [InlineData("-5.5", "3")]
    [InlineData("10.25", "3")]
    [InlineData("123456789012345.67891", "999999999999999999")]
    public async Task Min_and_Max_match_dotnet(string vLit, string otherLit)
    {
        var v = decimal.Parse(vLit, System.Globalization.CultureInfo.InvariantCulture);
        var other = decimal.Parse(otherLit, System.Globalization.CultureInfo.InvariantCulture);
        var seed = new[] { new DmpRow { Id = 1, V = v } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);

        var min = ctx.Query<DmpRow>().Select(x => Math.Min(x.V, other)).ToList().Single();
        Assert.Equal(Math.Min(v, other), min);

        var max = ctx.Query<DmpRow>().Select(x => Math.Max(x.V, other)).ToList().Single();
        Assert.Equal(Math.Max(v, other), max);
    }

    // Money-scale regression: ordinary 2dp values must still round correctly (no over-eager text path).
    [Fact]
    public async Task Money_scale_floor_ceiling_abs_unaffected()
    {
        var seed = new[] { new DmpRow { Id = 1, V = 10.99m }, new DmpRow { Id = 2, V = 10.01m }, new DmpRow { Id = 3, V = -3.50m } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var floors = ctx.Query<DmpRow>().OrderBy(x => x.Id).Select(x => Math.Floor(x.V)).ToList();
        Assert.Equal(new[] { 10m, 10m, -4m }, floors.ToArray());
        var ceils = ctx.Query<DmpRow>().OrderBy(x => x.Id).Select(x => Math.Ceiling(x.V)).ToList();
        Assert.Equal(new[] { 11m, 11m, -3m }, ceils.ToArray());
        var abses = ctx.Query<DmpRow>().OrderBy(x => x.Id).Select(x => Math.Abs(x.V)).ToList();
        Assert.Equal(new[] { 10.99m, 10.01m, 3.50m }, abses.ToArray());
    }
}
