using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Sister of f77ac0e (6-arg DateTime from parts). The 7-arg ctor
/// <c>new DateTime(year, month, day, hour, minute, second, millisecond)</c>
/// with column args wasn't matched and fell through. Add a 7-arg
/// overload on the per-provider hook.
///
///   SQL Server: DATETIME2FROMPARTS(y, m, d, h, mi, s, ms*10000, 7)
///               fractions are in 100ns units at precision 7.
///   PostgreSQL: MAKE_TIMESTAMP(y, m, d, h, mi, s + ms/1000.0)
///               seconds arg is double; combine s + ms/1000.0.
///   MySQL:      STR_TO_DATE with '%Y-%m-%d %H:%i:%s.%f' format string,
///               appending 3-digit millis as the fractional tail.
///   SQLite:     strftime-based text with '.fff' fractional tail.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeFromPartsSevenArgProviderShapeTests : TestBase
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
        public int Millisecond { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "DATETIME2FROMPARTS")]
    [InlineData(ProviderKind.Postgres,  "MAKE_TIMESTAMP")]
    [InlineData(ProviderKind.MySql,     "STR_TO_DATE")]
    [InlineData(ProviderKind.Sqlite,    "strftime")]
    public void Where_with_new_DateTime_seven_arg_from_int_columns_emits_provider_from_parts(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ctor = typeof(DateTime).GetConstructor(new[] {
            typeof(int), typeof(int), typeof(int),
            typeof(int), typeof(int), typeof(int),
            typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var y = Expression.Property(param, nameof(Row.Year));
        var m = Expression.Property(param, nameof(Row.Month));
        var d = Expression.Property(param, nameof(Row.Day));
        var h = Expression.Property(param, nameof(Row.Hour));
        var mi = Expression.Property(param, nameof(Row.Minute));
        var s = Expression.Property(param, nameof(Row.Second));
        var ms = Expression.Property(param, nameof(Row.Millisecond));
        var call = Expression.New(ctor, y, m, d, h, mi, s, ms);
        var body = Expression.GreaterThan(call, Expression.Constant(new DateTime(2026, 1, 1)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
        // Millisecond column reference must appear (otherwise the fractional
        // arg was silently dropped).
        Assert.Contains("Millisecond", sql);
    }
}
