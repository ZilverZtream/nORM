using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for casts and conversions in projections/predicates: (double)/(decimal)
/// casts, integer division vs float division, Convert.ToInt32 rounding, and cast comparisons. These
/// carry subtle .NET truncation/rounding semantics that a naive SQL translation can silently diverge
/// from (integer division, banker's rounding, REAL vs INTEGER).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CastConversionProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CcpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public double D { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 20).Select(i => new Row
    {
        Id = i,
        A = i * 3,            // 3..60
        B = (i % 7) + 1,      // 1..7  (divisor, never 0)
        D = i * 1.5,          // 1.5..30.0
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CcpRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, D REAL NOT NULL);";
            foreach (var r in Rows)
                cmd.CommandText += $"INSERT INTO CcpRow VALUES ({r.Id},{r.A},{r.B},{r.D.ToString(System.Globalization.CultureInfo.InvariantCulture)});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Integer_division_truncates_like_csharp()
    {
        // C# int/int truncates toward zero. A naive SQL `/` on some providers yields a real quotient.
        var expected = Rows.Select(r => r.A / r.B).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => r.A / r.B).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Float_cast_division_matches_csharp()
    {
        // (double)A / B is real division — must NOT truncate.
        var expected = Rows.Select(r => (double)r.A / r.B).OrderBy(v => v).Select(v => Math.Round(v, 4)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => (double)r.A / r.B).OrderBy(v => v).ToList().Select(v => Math.Round(v, 4)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Cast_double_to_int_truncates_like_csharp()
    {
        // (int)D truncates toward zero (1.5 -> 1, not rounded).
        var expected = Rows.Select(r => (int)r.D).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => (int)r.D).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Convert_toint32_rounds_bankers_like_csharp()
    {
        // Convert.ToInt32 uses banker's rounding (2.5 -> 2, 3.5 -> 4). D = i*1.5 gives .0 and .5 values.
        var expected = Rows.Select(r => Convert.ToInt32(r.D)).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => Convert.ToInt32(r.D)).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Cast_comparison_in_predicate_matches_csharp()
    {
        // Predicate mixing a float cast: (double)A / B > 5.0.
        var expected = Rows.Where(r => (double)r.A / r.B > 5.0).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => (double)r.A / r.B > 5.0).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Decimal_cast_arithmetic_matches_csharp()
    {
        var expected = Rows.Select(r => (decimal)r.A / 4m).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Select(r => (decimal)r.A / 4m).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Sum_of_integer_division_matches_csharp()
    {
        // Aggregate of a truncating division — combines the int-division and Select-aggregate paths.
        var expected = Rows.Sum(r => r.A / r.B);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Sum(r => r.A / r.B);
        Assert.Equal(expected, actual);
    }
}
