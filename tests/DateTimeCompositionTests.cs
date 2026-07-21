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
/// Oracle-compared coverage for DateTime component / arithmetic / diff compositions across projection,
/// predicate, ordering, GroupBy-key and aggregate positions — a common real-world surface where date
/// lowering (component extraction, Add*, subtraction-to-TimeSpan, truncation) can silently diverge from
/// .NET semantics. Each case runs the IDENTICAL LINQ expression against nORM (SQLite) and LINQ-to-Objects
/// over the same seeded rows and asserts equality.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DateTimeCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DtcRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public DateTime Ts { get; set; }
        public DateTime Ts2 { get; set; }
        public int N { get; set; }
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 24).Select(i => new Row
    {
        Id = i,
        Ts = new DateTime(2020, 1, 1, 0, 0, 0).AddDays(i * 13).AddHours(i * 7).AddMinutes(i * 11),
        Ts2 = new DateTime(2019, 6, 15, 12, 0, 0).AddDays(i * 5),
        N = i % 6,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DtcRow (Id INTEGER PRIMARY KEY, Ts TEXT NOT NULL, Ts2 TEXT NOT NULL, N INTEGER NOT NULL);";
        foreach (var r in Rows)
            cmd.CommandText += $"INSERT INTO DtcRow VALUES ({r.Id},'{r.Ts:yyyy-MM-dd HH:mm:ss}','{r.Ts2:yyyy-MM-dd HH:mm:ss}',{r.N});";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Rows.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Year_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.Year));
    [Fact] public void Month_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.Month));
    [Fact] public void Day_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.Day));
    [Fact] public void Hour_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.Hour));
    [Fact] public void Minute_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.Minute));
    [Fact] public void DayOfWeek_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.Ts.DayOfWeek));
    [Fact] public void DayOfYear_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.DayOfYear));
    [Fact] public void Date_truncation_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.Date));
    [Fact] public void TimeOfDay_hours_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.TimeOfDay.Hours));

    [Fact] public void Month_predicate() => Assert_(q => q.Where(r => r.Ts.Month > 6).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Year_predicate() => Assert_(q => q.Where(r => r.Ts.Year == 2020).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void DayOfWeek_predicate() => Assert_(q => q.Where(r => r.Ts.DayOfWeek == DayOfWeek.Monday).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact] public void AddDays_then_component() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.AddDays(10).Month));
    [Fact] public void AddMonths_then_component() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.AddMonths(2).Year));
    [Fact] public void AddYears_then_component() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.AddYears(1).Year));
    [Fact] public void AddHours_then_component() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts.AddHours(30).Day));

    [Fact] public void Subtraction_total_days_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)(r.Ts - r.Ts2).TotalDays));
    [Fact] public void Subtraction_total_hours_sign_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (r.Ts - r.Ts2).TotalHours > 0));
    [Fact] public void Subtraction_total_days_predicate() => Assert_(q => q.Where(r => (r.Ts - r.Ts2).TotalDays > 100).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact] public void OrderBy_datetime() => Assert_(q => q.OrderBy(r => r.Ts).Select(r => r.Id));
    [Fact] public void Compare_two_columns_predicate() => Assert_(q => q.Where(r => r.Ts > r.Ts2).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Compare_constant_bool_projection() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Ts > new DateTime(2020, 6, 1)));

    [Fact] public void GroupBy_month_count() => Assert_(q => q.GroupBy(r => r.Ts.Month).OrderBy(g => g.Key).Select(g => g.Key * 1000 + g.Count()));
    [Fact] public void GroupBy_year_count() => Assert_(q => q.GroupBy(r => r.Ts.Year).OrderBy(g => g.Key).Select(g => g.Count()));

    [Fact] public void Max_of_component() => Assert_(q => new[] { q.Max(r => r.Ts.Month) });
    [Fact] public void Sum_of_component() => Assert_(q => new[] { q.Sum(r => r.Ts.DayOfYear) });
}
