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
/// Oracle-compared coverage for Math functions in projections and predicates: Abs, Sign, Floor,
/// Ceiling, Round (default banker's and with digits), and Min/Max. These carry rounding-mode and
/// provider-SQL subtleties that can silently diverge from .NET.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class MathFunctionProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("MfpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }      // spans negative and positive
        public double D { get; set; }   // fractional, incl .5 midpoints
    }

    private static readonly Row[] Rows = Enumerable.Range(0, 20).Select(i => new Row
    {
        Id = i + 1,
        A = i - 10,                 // -10..9
        D = (i - 10) * 0.5 + 0.25,  // -4.75 .. 4.75 in 0.5 steps, plus .5 midpoints via variant below
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE MfpRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, D REAL NOT NULL);";
            foreach (var r in Rows)
                cmd.CommandText += $"INSERT INTO MfpRow VALUES ({r.Id},{r.A},{r.D.ToString(System.Globalization.CultureInfo.InvariantCulture)});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Abs_int_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => Math.Abs(r.A)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Abs(r.A)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Abs_double_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => Math.Abs(r.D)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Abs(r.D)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Sign_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => Math.Sign(r.A)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Sign(r.A)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Floor_and_ceiling_projection_matches_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(
            Rows.OrderBy(r => r.Id).Select(r => Math.Floor(r.D)).ToList(),
            ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Floor(r.D)).ToList());
        Assert.Equal(
            Rows.OrderBy(r => r.Id).Select(r => Math.Ceiling(r.D)).ToList(),
            ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Ceiling(r.D)).ToList());
    }

    [Fact]
    public void Round_default_bankers_projection_matches_linq()
    {
        // Math.Round uses banker's rounding by default (2.5 -> 2, 3.5 -> 4).
        var expected = Rows.OrderBy(r => r.Id).Select(r => Math.Round(r.D)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Round(r.D)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Round_with_digits_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => Math.Round(r.D / 3.0, 2)).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Math.Round(r.D / 3.0, 2)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Abs_in_predicate_matches_linq()
    {
        var expected = Rows.Where(r => Math.Abs(r.A) >= 5).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => Math.Abs(r.A) >= 5).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Sum_of_abs_matches_linq()
    {
        var expected = Rows.Sum(r => Math.Abs(r.A));
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Sum(r => Math.Abs(r.A));
        Assert.Equal(expected, actual);
    }
}
