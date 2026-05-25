using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>new DateTime(year, month, day)</c> with COLUMN arguments fell
/// through to the base VisitNew which just visits each argument in
/// turn -- emitting no DATETIME construction, producing invalid SQL
/// (the three int columns juxtaposed). Per-provider date-from-parts
/// primitive needed.
///
///   SQL Server: DATETIME2FROMPARTS(y, m, d, 0, 0, 0, 0, 7)
///   PostgreSQL: MAKE_TIMESTAMP(y, m, d, 0, 0, 0)
///   MySQL:      STR_TO_DATE(CONCAT_WS('-', y, LPAD(m,2,'0'), LPAD(d,2,'0')), '%Y-%m-%d')
///   SQLite:     strftime-based concat of zero-padded text components
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeFromPartsProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "DATETIME2FROMPARTS")]
    [InlineData(ProviderKind.Postgres,  "MAKE_TIMESTAMP")]
    [InlineData(ProviderKind.MySql,     "STR_TO_DATE")]
    [InlineData(ProviderKind.Sqlite,    "strftime")]
    public void Where_with_new_DateTime_from_int_columns_emits_provider_from_parts(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ctor = typeof(DateTime).GetConstructor(new[] { typeof(int), typeof(int), typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var year = Expression.Property(param, nameof(Row.Year));
        var month = Expression.Property(param, nameof(Row.Month));
        var day = Expression.Property(param, nameof(Row.Day));
        var newDt = Expression.New(ctor, year, month, day);
        var body = Expression.GreaterThan(newDt, Expression.Constant(new DateTime(2026, 1, 1)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }
}
