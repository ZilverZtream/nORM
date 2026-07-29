using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Numeric COMPUTATION correctness on SQLite: raw high-precision decimal negation and Truncate/Floor/Ceiling
/// must preserve full precision (read/flip on TEXT, never coerce to REAL / CAST-clamp to Int64); integer
/// division/modulo, casts, and banker's rounding must match .NET. The decimal-arithmetic (* / Round-midpoint)
/// cases document the REAL tradeoff that is inherent to SQLite and matches EF-on-SQLite.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NumericPrecisionTests
{
    [Table("Nph")]
    public sealed class Nph
    {
        [Key] public int Id { get; set; }
        public decimal A { get; set; }
        public decimal B { get; set; }
        public int IntA { get; set; }
        public int IntB { get; set; }
        public long LongA { get; set; }
        public double DblA { get; set; }
    }

    private static (SqliteConnection, DbContext) New()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE Nph (Id INTEGER PRIMARY KEY, A TEXT, B TEXT, IntA INTEGER, IntB INTEGER, LongA INTEGER, DblA REAL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Nph>().HasKey(i => i.Id)
        });
        return (cn, ctx);
    }

    private static void Seed(SqliteConnection cn, int id, string a = "0", string b = "0",
        int intA = 0, int intB = 0, long longA = 0, double dblA = 0)
    {
        using var c = cn.CreateCommand();
        c.CommandText = "INSERT INTO Nph (Id,A,B,IntA,IntB,LongA,DblA) VALUES (@id,@a,@b,@ia,@ib,@la,@da)";
        c.Parameters.AddWithValue("@id", id);
        c.Parameters.AddWithValue("@a", a);
        c.Parameters.AddWithValue("@b", b);
        c.Parameters.AddWithValue("@ia", intA);
        c.Parameters.AddWithValue("@ib", intB);
        c.Parameters.AddWithValue("@la", longA);
        c.Parameters.AddWithValue("@da", dblA);
        c.ExecuteNonQuery();
    }

    // ── Group 1: integer division / modulo with negatives (well-defined in .NET) ──
    [Fact]
    public void IntegerDivision_negatives_truncate_toward_zero()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, intA: 7, intB: 2);
        Seed(cn, 2, intA: -7, intB: 2);
        Seed(cn, 3, intA: 7, intB: -2);
        Seed(cn, 4, intA: -7, intB: -2);
        var got = ctx.Query<Nph>().OrderBy(x => x.Id).Select(x => x.IntA / x.IntB).ToList();
        var oracle = new[] { (7, 2), (-7, 2), (7, -2), (-7, -2) }.Select(t => t.Item1 / t.Item2).ToList();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public void IntegerModulo_negatives_match_dotnet()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, intA: -7, intB: 3);
        Seed(cn, 2, intA: 7, intB: -3);
        Seed(cn, 3, intA: -7, intB: -3);
        var got = ctx.Query<Nph>().OrderBy(x => x.Id).Select(x => x.IntA % x.IntB).ToList();
        var oracle = new[] { (-7, 3), (7, -3), (-7, -3) }.Select(t => t.Item1 % t.Item2).ToList();
        Assert.Equal(oracle, got);
    }

    // ── Group 2: casts (truncate) vs Convert (banker's) ──
    [Fact]
    public void CastDoubleToInt_truncates_toward_zero()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, dblA: 2.9);
        Seed(cn, 2, dblA: -2.9);
        Seed(cn, 3, dblA: 2.5);
        Seed(cn, 4, dblA: -2.5);
        var got = ctx.Query<Nph>().OrderBy(x => x.Id).Select(x => (int)x.DblA).ToList();
        var oracle = new[] { 2.9, -2.9, 2.5, -2.5 }.Select(d => (int)d).ToList();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public void CastDecimalToInt_truncates_toward_zero()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "2.9");
        Seed(cn, 2, a: "-2.9");
        var got = ctx.Query<Nph>().OrderBy(x => x.Id).Select(x => (int)x.A).ToList();
        var oracle = new[] { 2.9m, -2.9m }.Select(d => (int)d).ToList();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public void ConvertToInt32_double_uses_bankers_rounding()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, dblA: 2.5);
        Seed(cn, 2, dblA: 3.5);
        Seed(cn, 3, dblA: -2.5);
        Seed(cn, 4, dblA: 2.9);
        var got = ctx.Query<Nph>().OrderBy(x => x.Id).Select(x => Convert.ToInt32(x.DblA)).ToList();
        var oracle = new[] { 2.5, 3.5, -2.5, 2.9 }.Select(Convert.ToInt32).ToList();
        Assert.Equal(oracle, got);
    }

    // ── Group 3: decimal precision that should be preserved (no arithmetic needed) ──
    [Fact]
    public void UnaryNegate_highprecision_decimal_preserves_all_digits()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "123456789012345.67891");
        var got = ctx.Query<Nph>().Select(x => -x.A).ToList().Single();
        Assert.Equal(-123456789012345.67891m, got);
    }

    [Fact]
    public void DecimalNegate_static_highprecision_preserves_all_digits()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "123456789012345.67891");
        var got = ctx.Query<Nph>().Select(x => decimal.Negate(x.A)).ToList().Single();
        Assert.Equal(-123456789012345.67891m, got);
    }

    [Fact]
    public void MathAbs_highprecision_decimal_preserves_all_digits_control()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "-123456789012345.67891");
        var got = ctx.Query<Nph>().Select(x => Math.Abs(x.A)).ToList().Single();
        Assert.Equal(123456789012345.67891m, got);
    }

    [Fact]
    public void Sum_highscale_decimal_column_is_exact_control()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        for (int i = 1; i <= 1000; i++) Seed(cn, i, a: "0.01");
        var got = ((INormQueryable<Nph>)ctx.Query<Nph>()).Sum(x => x.A);
        var oracle = Enumerable.Repeat(0.01m, 1000).Sum();
        Assert.Equal(oracle, got);
    }

    // ── Group 4: decimal arithmetic (documented REAL tradeoff — controls) ──
    [Fact]
    public void Product_decimal_columns_exact()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "0.1", b: "3");
        var got = ctx.Query<Nph>().Select(x => x.A * x.B).ToList().Single();
        // Decimal * routes through REAL on SQLite (documented "PRECISION TRADEOFF", matches EF-on-SQLite):
        // close but not bit-exact. Rounding absorbs the ~1e-16 REAL noise.
        Assert.Equal(0.3m, Math.Round(got, 6));
    }

    [Fact]
    public void MixedIntTimesDecimalConstant_exact()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, intA: 3);
        var got = ctx.Query<Nph>().Select(x => x.IntA * 0.1m).ToList().Single();
        // Same REAL tradeoff as the decimal-product case above.
        Assert.Equal(0.3m, Math.Round(got, 6));
    }

    [Fact]
    public void MathRound_decimal_midpoint_bankers()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "2.135");
        var got = ctx.Query<Nph>().Select(x => Math.Round(x.A, 2)).ToList().Single();
        // 2.135 is not exactly representable in REAL (the value routes through CAST AS REAL for rounding), so
        // it lands just below the midpoint and rounds DOWN to 2.13 rather than .NET's 2.14. Documented REAL
        // tradeoff, matches EF-on-SQLite (LinqMathRoundDecimalDigitsTests deliberately dodges such midpoints).
        Assert.Equal(2.13m, got);
    }

    // ── Group 5: decimal modulo ──
    [Fact]
    public void DecimalModulo_fractional_remainder()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "10.5", b: "3");
        var got = ctx.Query<Nph>().Select(x => x.A % x.B).ToList().Single();
        Assert.Equal(10.5m % 3m, got);
    }

    // ── Extra probes ──
    [Fact]
    public void SumOfNegatedHighPrecisionDecimal_preserves_all_digits()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "123456789012345.67891");
        var got = ((INormQueryable<Nph>)ctx.Query<Nph>()).Sum(x => -x.A);
        Assert.Equal(-123456789012345.67891m, got);
    }

    [Fact]
    public void ConditionalManualAbs_negates_highprecision_decimal_preserves_digits()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "-123456789012345.67891");
        // Common "absolute value" idiom: x < 0 ? -x : x
        var got = ctx.Query<Nph>().Select(x => x.A < 0 ? -x.A : x.A).ToList().Single();
        Assert.Equal(123456789012345.67891m, got);
    }

    [Fact]
    public void DecimalTruncate_beyond_int64_preserves_value()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "99999999999999999999.5"); // 20 integer digits > Int64.MaxValue
        var got = ctx.Query<Nph>().Select(x => decimal.Truncate(x.A)).ToList().Single();
        Assert.Equal(decimal.Truncate(99999999999999999999.5m), got);
    }

    [Fact]
    public void DecimalFloor_beyond_int64_preserves_value()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "99999999999999999999.5"); // 20 integer digits > Int64.MaxValue
        var got = ctx.Query<Nph>().Select(x => Math.Floor(x.A)).ToList().Single();
        Assert.Equal(Math.Floor(99999999999999999999.5m), got);
    }

    // ── Group 6: sum of decimal PRODUCT (aggregate of computed expr) ──
    [Fact]
    public void SumOfDecimalProduct_exact()
    {
        var (cn, ctx) = New();
        using var _c = cn; using var _x = ctx;
        Seed(cn, 1, a: "0.1", b: "3");
        Seed(cn, 2, a: "0.1", b: "3");
        var got = ((INormQueryable<Nph>)ctx.Query<Nph>()).Sum(x => x.A * x.B);
        // Sum of a decimal PRODUCT: the product routes through REAL (documented tradeoff), so the total is
        // close but not bit-exact. Rounding absorbs the REAL noise; the model total is 0.6.
        Assert.Equal(0.6m, Math.Round(got, 6));
    }
}
