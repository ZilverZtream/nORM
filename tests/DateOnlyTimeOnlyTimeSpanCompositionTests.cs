using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for DateOnly / TimeOnly / TimeSpan compositions across projection, predicate,
/// ordering and GroupBy-key positions — component extraction, arithmetic (Add*), DayNumber, and TimeSpan
/// totals, all TEXT-stored on SQLite. Rows are seeded through nORM's own insert path so read/write
/// round-trip, and each case runs the identical LINQ expression against nORM and LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DateOnlyTimeOnlyTimeSpanCompositionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("DottRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public DateOnly D { get; set; }
        public TimeOnly T { get; set; }
        public TimeSpan Span { get; set; }
    }

    private static readonly Row[] Seed = Enumerable.Range(1, 10).Select(i => new Row
    {
        Id = i,
        D = new DateOnly(2020, 1, 1).AddDays(i * 37).AddMonths(i % 5),
        T = new TimeOnly(6, 0, 0).Add(TimeSpan.FromMinutes(i * 97)),
        Span = TimeSpan.FromMinutes(i * 133) + TimeSpan.FromSeconds(i * 7),
    }).ToArray();

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE DottRow (Id INTEGER PRIMARY KEY, D TEXT NOT NULL, T TEXT NOT NULL, Span TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in Seed)
            await ctx.InsertAsync(new Row { Id = r.Id, D = r.D, T = r.T, Span = r.Span });
        return ctx;
    }

    private static async Task Assert_<TR>(Func<IQueryable<Row>, IEnumerable<TR>> q)
    {
        var expected = q(Seed.AsQueryable()).ToList();
        using var ctx = await CtxAsync();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    // DateOnly components
    [Fact] public Task D_Year() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.Year));
    [Fact] public Task D_Month() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.Month));
    [Fact] public Task D_Day() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.Day));
    [Fact] public Task D_DayOfWeek() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.D.DayOfWeek));
    [Fact] public Task D_DayOfYear() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.DayOfYear));
    [Fact] public Task D_DayNumber() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.DayNumber));

    // DateOnly arithmetic
    [Fact] public Task D_AddDays_Month() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.AddDays(10).Month));
    [Fact] public Task D_AddMonths_Year() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.AddMonths(2).Year));
    [Fact] public Task D_AddYears_Year() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.AddYears(1).Year));
    [Fact] public Task D_DayNumber_difference() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.D.DayNumber - new DateOnly(2020, 1, 1).DayNumber));

    // DateOnly predicate / ordering / group
    [Fact] public Task D_year_predicate() => Assert_(q => q.Where(r => r.D.Year == 2021).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public Task D_compare_predicate() => Assert_(q => q.Where(r => r.D > new DateOnly(2021, 1, 1)).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public Task D_orderby() => Assert_(q => q.OrderBy(r => r.D).Select(r => r.Id));
    [Fact] public Task D_groupby_month() => Assert_(q => q.GroupBy(r => r.D.Month).OrderBy(g => g.Key).Select(g => g.Key * 100 + g.Count()));

    // TimeOnly
    [Fact] public Task T_Hour() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.T.Hour));
    [Fact] public Task T_Minute() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.T.Minute));
    [Fact] public Task T_hour_predicate() => Assert_(q => q.Where(r => r.T.Hour >= 12).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public Task T_compare_predicate() => Assert_(q => q.Where(r => r.T > new TimeOnly(12, 0, 0)).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public Task T_orderby() => Assert_(q => q.OrderBy(r => r.T).Select(r => r.Id));

    // TimeSpan
    [Fact] public Task Span_Hours() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Span.Hours));
    [Fact] public Task Span_Minutes() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Span.Minutes));
    [Fact] public Task Span_TotalMinutes_predicate() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Span.TotalMinutes > 500));
    [Fact] public Task Span_compare_predicate() => Assert_(q => q.Where(r => r.Span > TimeSpan.FromHours(5)).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public Task Span_orderby() => Assert_(q => q.OrderBy(r => r.Span).Select(r => r.Id));
    [Fact] public Task Span_TotalSeconds_int() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.Span.TotalSeconds));
}
