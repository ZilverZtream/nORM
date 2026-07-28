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
/// Adversarial sweep of System.Math.* translation on the SQLite provider.
/// Each test asserts against the LINQ-to-Objects oracle (same lambda on a List).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MathTranslationSweepTests
{
    [Table("MathRow")]
    public class MathRow
    {
        [Key] public int Id { get; set; }
        public double D { get; set; }
        public decimal V { get; set; }
        public long N { get; set; }
    }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE MathRow (Id INTEGER PRIMARY KEY, D REAL NOT NULL, V TEXT NOT NULL, N INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static async Task<DbContext> Seed(SqliteConnection cn, double d, decimal v, long n = 0)
    {
        var ctx = NewCtx(cn);
        ctx.Add(new MathRow { Id = 1, D = d, V = v, N = n });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    // ---- Math.Round (double) : .NET default is banker's (ToEven) ----
    [Theory]
    [InlineData(2.5, 2.0)]
    [InlineData(3.5, 4.0)]
    [InlineData(0.5, 0.0)]
    [InlineData(-2.5, -2.0)]
    [InlineData(1.5, 2.0)]
    public async Task Round_double_default_is_bankers(double input, double expected)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, input, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Round(x.D)).ToList().Single();
        Assert.Equal(Math.Round(input), r);      // oracle
        Assert.Equal(expected, r);
    }

    [Fact]
    public async Task Round_double_two_digits_default_is_bankers()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 2.125, 0m);   // exactly representable
        var r = ctx.Query<MathRow>().Select(x => Math.Round(x.D, 2)).ToList().Single();
        Assert.Equal(Math.Round(2.125, 2), r);   // 2.12 banker's
        Assert.Equal(2.12, r);
    }

    [Fact]
    public async Task Round_double_two_digits_awayfromzero()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 2.125, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Round(x.D, 2, MidpointRounding.AwayFromZero)).ToList().Single();
        Assert.Equal(Math.Round(2.125, 2, MidpointRounding.AwayFromZero), r); // 2.13
        Assert.Equal(2.13, r);
    }

    // ---- Math.Round (decimal) ----
    [Theory]
    [InlineData(2.5, 2.0)]
    [InlineData(3.5, 4.0)]
    [InlineData(0.5, 0.0)]
    public async Task Round_decimal_default_is_bankers(double inputD, double expectedD)
    {
        decimal input = (decimal)inputD, expected = (decimal)expectedD;
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 0.0, input);
        var r = ctx.Query<MathRow>().Select(x => Math.Round(x.V)).ToList().Single();
        Assert.Equal(Math.Round(input), r);
        Assert.Equal(expected, r);
    }

    // ---- Math.Truncate ----
    [Theory]
    [InlineData(-1.5, -1.0)]
    [InlineData(2.9, 2.0)]
    [InlineData(-2.9, -2.0)]
    public async Task Truncate_double(double input, double expected)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, input, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Truncate(x.D)).ToList().Single();
        Assert.Equal(Math.Truncate(input), r);
        Assert.Equal(expected, r);
    }

    [Fact]
    public async Task Truncate_decimal_negative()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 0.0, -1.9m);
        var r = ctx.Query<MathRow>().Select(x => Math.Truncate(x.V)).ToList().Single();
        Assert.Equal(Math.Truncate(-1.9m), r);  // -1
        Assert.Equal(-1m, r);
    }

    // ---- Math.Floor / Ceiling ----
    [Theory]
    [InlineData(-1.5, -2.0)]
    [InlineData(1.1, 1.0)]
    public async Task Floor_double(double input, double expected)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, input, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Floor(x.D)).ToList().Single();
        Assert.Equal(Math.Floor(input), r);
        Assert.Equal(expected, r);
    }

    [Theory]
    [InlineData(-1.5, -1.0)]
    [InlineData(1.1, 2.0)]
    public async Task Ceiling_double(double input, double expected)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, input, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Ceiling(x.D)).ToList().Single();
        Assert.Equal(Math.Ceiling(input), r);
        Assert.Equal(expected, r);
    }

    [Fact]
    public async Task Floor_decimal_negative()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 0.0, -1.1m);
        var r = ctx.Query<MathRow>().Select(x => Math.Floor(x.V)).ToList().Single();
        Assert.Equal(Math.Floor(-1.1m), r);  // -2
        Assert.Equal(-2m, r);
    }

    // ---- Math.Abs ----
    [Fact]
    public async Task Abs_double_negative()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, -5.5, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Abs(x.D)).ToList().Single();
        Assert.Equal(Math.Abs(-5.5), r);
        Assert.Equal(5.5, r);
    }

    [Fact]
    public async Task Abs_decimal_negative()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 0.0, -5.5m);
        var r = ctx.Query<MathRow>().Select(x => Math.Abs(x.V)).ToList().Single();
        Assert.Equal(Math.Abs(-5.5m), r);
        Assert.Equal(5.5m, r);
    }

    // ---- Math.Sign ----
    [Theory]
    [InlineData(-7.0, -1)]
    [InlineData(0.0, 0)]
    [InlineData(7.0, 1)]
    public async Task Sign_double(double input, int expected)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, input, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Sign(x.D)).ToList().Single();
        Assert.Equal(Math.Sign(input), r);
        Assert.Equal(expected, r);
    }

    // ---- Math.Min / Max (two-arg) ----
    [Fact]
    public async Task Max_double_two_arg()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 3.0, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Max(x.D, 7.0)).ToList().Single();
        Assert.Equal(Math.Max(3.0, 7.0), r);
        Assert.Equal(7.0, r);
    }

    [Fact]
    public async Task Min_double_two_arg()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 3.0, 0m);
        var r = ctx.Query<MathRow>().Select(x => Math.Min(x.D, 7.0)).ToList().Single();
        Assert.Equal(Math.Min(3.0, 7.0), r);
        Assert.Equal(3.0, r);
    }

    [Fact]
    public async Task Max_decimal_high_precision_preserved()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345.67891m; // 20 sig digits, beyond double
        using var ctx = await Seed(cn, 0.0, big);
        var r = ctx.Query<MathRow>().Select(x => Math.Max(x.V, 1m)).ToList().Single();
        Assert.Equal(Math.Max(big, 1m), r);   // must be exactly big
        Assert.Equal(big, r);
    }

    [Fact]
    public async Task Min_decimal_high_precision_preserved()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345.67891m;
        using var ctx = await Seed(cn, 0.0, big);
        // Min(big, huge) must return big unchanged.
        var r = ctx.Query<MathRow>().Select(x => Math.Min(x.V, 999999999999999999m)).ToList().Single();
        Assert.Equal(Math.Min(big, 999999999999999999m), r);
        Assert.Equal(big, r);
    }

    [Fact]
    public async Task Max_decimal_high_precision_in_where_keeps_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345.67891m;
        using var ctx = await Seed(cn, 0.0, big);
        // Math.Max(V, 1) == big is TRUE in .NET; precision loss can drop the row.
        var ids = ctx.Query<MathRow>().Where(x => Math.Max(x.V, 1m) == big).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());
    }

    // ---- CONTROL: plain decimal projection must round-trip exactly ----
    [Fact]
    public async Task Control_plain_decimal_projection_roundtrips_high_precision()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345.67891m;
        using var ctx = await Seed(cn, 0.0, big);
        var r = ctx.Query<MathRow>().Select(x => x.V).ToList().Single();
        Assert.Equal(big, r);   // storage + materialization is exact; loss below is Math-only
    }

    // ---- sibling decimal->REAL precision probes ----
    [Fact]
    public async Task Abs_decimal_high_precision_preserved()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = -123456789012345.67891m;
        using var ctx = await Seed(cn, 0.0, big);
        var r = ctx.Query<MathRow>().Select(x => Math.Abs(x.V)).ToList().Single();
        Assert.Equal(Math.Abs(big), r);            // 123456789012345.67891
        Assert.Equal(123456789012345.67891m, r);
    }

    [Fact]
    public async Task Truncate_decimal_large_integer_part_preserved()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345678.9m; // integer part > 2^53
        using var ctx = await Seed(cn, 0.0, big);
        var r = ctx.Query<MathRow>().Select(x => Math.Truncate(x.V)).ToList().Single();
        Assert.Equal(Math.Truncate(big), r);       // 123456789012345678
        Assert.Equal(123456789012345678m, r);
    }

    [Fact]
    public async Task Floor_decimal_large_integer_part_preserved()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345678.9m; // integer part > 2^53
        using var ctx = await Seed(cn, 0.0, big);
        var r = ctx.Query<MathRow>().Select(x => Math.Floor(x.V)).ToList().Single();
        Assert.Equal(Math.Floor(big), r);          // 123456789012345678
        Assert.Equal(123456789012345678m, r);
    }

    [Fact]
    public async Task Ceiling_decimal_large_integer_part_preserved()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        decimal big = 123456789012345678.1m; // integer part > 2^53
        using var ctx = await Seed(cn, 0.0, big);
        var r = ctx.Query<MathRow>().Select(x => Math.Ceiling(x.V)).ToList().Single();
        Assert.Equal(Math.Ceiling(big), r);        // 123456789012345679
        Assert.Equal(123456789012345679m, r);
    }

    // ---- Math.Pow / Sqrt / Log (existence + value) ----
    [Fact]
    public async Task Pow_and_sqrt_and_log()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 16.0, 0m);
        var pow = ctx.Query<MathRow>().Select(x => Math.Pow(2.0, 10.0)).ToList().First();
        Assert.Equal(1024.0, pow);
        var sqrt = ctx.Query<MathRow>().Select(x => Math.Sqrt(x.D)).ToList().Single();
        Assert.Equal(4.0, sqrt);
        var log = ctx.Query<MathRow>().Select(x => Math.Log(Math.Exp(1.0))).ToList().First();
        Assert.Equal(1.0, log, 10);
    }

    // ---- WHERE predicate composition (row keep/drop) ----
    [Fact]
    public async Task Round_in_where_bankers_keeps_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, 2.5, 0m);
        // banker's Round(2.5) == 2 keeps the row; away-from-zero would give 3 and drop it.
        var ids = ctx.Query<MathRow>().Where(x => Math.Round(x.D) == 2.0).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());
    }

    [Fact]
    public async Task Floor_in_where_negative_keeps_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, -1.5, 0m);
        var ids = ctx.Query<MathRow>().Where(x => Math.Floor(x.D) == -2.0).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());
    }

    [Fact]
    public async Task Truncate_in_where_negative_keeps_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await Seed(cn, -1.9, 0m);
        // Truncate(-1.9) == -1 (toward zero). Floor semantics would give -2 and drop.
        var ids = ctx.Query<MathRow>().Where(x => Math.Truncate(x.D) == -1.0).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids.ToArray());
    }
}
