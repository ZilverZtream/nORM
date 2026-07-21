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
/// Oracle-compared coverage for DateTime component extraction, comparison, and arithmetic in
/// projections and predicates (Year/Month/Day/Hour, DayOfWeek, AddDays/AddMonths, date comparison),
/// plus a float-cast OrderBy key (completeness check for the widening-cast fix). DateTime carries
/// subtle provider semantics (component functions, fractional Add, epoch math) that can silently
/// diverge from .NET.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DateTimeProjectionPredicateTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DtpRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public DateTime Ts { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(0, 24).Select(i => new Row
    {
        Id = i + 1,
        // Spread across months/days/hours deterministically.
        Ts = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(i * 13).AddHours(i * 5),
        A = (i * 5) % 17 + 1,
        B = (i % 6) + 1,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DtpRow (Id INTEGER PRIMARY KEY, Ts TEXT NOT NULL, A INTEGER NOT NULL, B INTEGER NOT NULL);";
            foreach (var r in Rows)
                cmd.CommandText += $"INSERT INTO DtpRow VALUES ({r.Id},'{r.Ts:yyyy-MM-dd HH:mm:ss}',{r.A},{r.B});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Year_month_day_extraction_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => new { r.Ts.Year, r.Ts.Month, r.Ts.Day })
            .Select(x => x.Year * 10000 + x.Month * 100 + x.Day).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Ts.Year, r.Ts.Month, r.Ts.Day })
            .Select(x => x.Year * 10000 + x.Month * 100 + x.Day).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Hour_extraction_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Ts.Hour).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Ts.Hour).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Filter_by_year_matches_linq()
    {
        var expected = Rows.Where(r => r.Ts.Year == 2024).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.Ts.Year == 2024).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Filter_by_month_greater_matches_linq()
    {
        var expected = Rows.Where(r => r.Ts.Month >= 6).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.Ts.Month >= 6).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Date_comparison_predicate_matches_linq()
    {
        var cutoff = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        var expected = Rows.Where(r => r.Ts >= cutoff).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => r.Ts >= cutoff).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AddDays_projection_matches_linq()
    {
        // Project Ts.AddDays(10).Month — arithmetic then component extraction.
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.Ts.AddDays(10).Month).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.Ts.AddDays(10).Month).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void DayOfWeek_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => (int)r.Ts.DayOfWeek).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (int)r.Ts.DayOfWeek).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Orderby_float_cast_key_matches_linq()
    {
        // Completeness check for the widening integral->float cast fix: ORDER BY (double)A / B.
        var expected = Rows.OrderBy(r => (double)r.A / r.B).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => (double)r.A / r.B).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }
}
