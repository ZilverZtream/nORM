using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateTime.IsLeapYear(year) and DateTime.DaysInMonth(year, month) are
/// portable arithmetic predicates / lookups. Only SQLite had them; the
/// other three providers fell through to NormUnsupportedFeatureException.
///
/// IsLeapYear: standard Gregorian rule (div by 4 but not by 100, OR div by 400).
/// DaysInMonth: month-length lookup with the leap-year exception on Feb.
/// Both shapes are pure SQL CASE / arithmetic that works on every provider.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeIsLeapYearAndDaysInMonthProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_DateTime_IsLeapYear_translates_to_gregorian_rule(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(DateTime).GetMethod(nameof(DateTime.IsLeapYear), new[] { typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var year = Expression.Property(param, nameof(Row.Year));
        var call = Expression.Call(method, year);
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // The Gregorian rule splits on 4 / 100 / 400. Anchor on those.
        Assert.Contains("% 4", sql);
        Assert.Contains("% 100", sql);
        Assert.Contains("% 400", sql);
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_DateTime_DaysInMonth_translates_to_month_length_case(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(DateTime).GetMethod(nameof(DateTime.DaysInMonth), new[] { typeof(int), typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var year = Expression.Property(param, nameof(Row.Year));
        var month = Expression.Property(param, nameof(Row.Month));
        var call = Expression.Call(method, year, month);
        var body = Expression.Equal(call, Expression.Constant(31));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // Month-length case dispatches on the month integer.
        Assert.Contains("CASE", sql);
        Assert.Contains("WHEN 1 THEN 31", sql);
        Assert.Contains("WHEN 2 THEN", sql);
    }
}
