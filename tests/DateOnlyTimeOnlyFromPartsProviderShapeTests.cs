using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Companion of 8875ae5: <c>new DateOnly(year, month, day)</c> and
/// <c>new TimeOnly(hour, minute, second)</c> with COLUMN arguments fell
/// through to the base VisitNew (just visiting each arg in turn), which
/// emits the int columns juxtaposed and produces invalid SQL. Each
/// provider has a native primitive for both shapes; add the hooks.
///
/// DateOnly from parts:
///   SQL Server: DATEFROMPARTS(y, m, d)
///   PostgreSQL: MAKE_DATE(y, m, d)
///   MySQL:      DATE(STR_TO_DATE(CONCAT_WS('-', y, LPAD(m,2,'0'), LPAD(d,2,'0')), '%Y-%m-%d'))
///   SQLite:     printf('%04d-%02d-%02d', y, m, d)
///
/// TimeOnly from parts:
///   SQL Server: TIMEFROMPARTS(h, m, s, 0, 0)
///   PostgreSQL: MAKE_TIME(h, m, s)
///   MySQL:      MAKETIME(h, m, s)
///   SQLite:     printf('%02d:%02d:%02d', h, m, s)
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateOnlyTimeOnlyFromPartsProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int Hour { get; set; }
        public int Minute { get; set; }
        public int Second { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "DATEFROMPARTS")]
    [InlineData(ProviderKind.Postgres,  "MAKE_DATE")]
    [InlineData(ProviderKind.MySql,     "STR_TO_DATE")]
    [InlineData(ProviderKind.Sqlite,    "printf(")]
    public void Where_with_new_DateOnly_from_int_columns_emits_provider_from_parts(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ctor = typeof(DateOnly).GetConstructor(new[] { typeof(int), typeof(int), typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var year = Expression.Property(param, nameof(Row.Year));
        var month = Expression.Property(param, nameof(Row.Month));
        var day = Expression.Property(param, nameof(Row.Day));
        var newD = Expression.New(ctor, year, month, day);
        var body = Expression.GreaterThan(newD, Expression.Constant(new DateOnly(2026, 1, 1)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "TIMEFROMPARTS")]
    [InlineData(ProviderKind.Postgres,  "MAKE_TIME")]
    [InlineData(ProviderKind.MySql,     "MAKETIME")]
    [InlineData(ProviderKind.Sqlite,    "printf(")]
    public void Where_with_new_TimeOnly_from_int_columns_emits_provider_from_parts(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ctor = typeof(TimeOnly).GetConstructor(new[] { typeof(int), typeof(int), typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var hour = Expression.Property(param, nameof(Row.Hour));
        var minute = Expression.Property(param, nameof(Row.Minute));
        var second = Expression.Property(param, nameof(Row.Second));
        var newT = Expression.New(ctor, hour, minute, second);
        var body = Expression.GreaterThan(newT, Expression.Constant(new TimeOnly(0, 0)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }
}
