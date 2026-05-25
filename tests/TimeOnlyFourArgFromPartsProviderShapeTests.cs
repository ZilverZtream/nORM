using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Companion of a1a5d55 (3-arg TimeOnly from parts) and 1faa09d (7-arg
/// DateTime with millisecond). The 4-arg
/// <c>new TimeOnly(hour, minute, second, millisecond)</c> with column
/// args fell through the const-fold path and emitted nothing. Add a
/// 4-arg overload of GetTimeOnlyFromPartsSql.
///
///   SQL Server: TIMEFROMPARTS(h, m, s, ms, 3)
///   PostgreSQL: MAKE_TIME(h, m, s + ms/1000.0)
///   MySQL:      MAKETIME(h, m, s + ms/1000.0)
///   SQLite:     printf('%02d:%02d:%02d.%03d', h, m, s, ms)
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyFourArgFromPartsProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public int Hour { get; set; }
        public int Minute { get; set; }
        public int Second { get; set; }
        public int Millisecond { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "TIMEFROMPARTS")]
    [InlineData(ProviderKind.Postgres,  "MAKE_TIME")]
    [InlineData(ProviderKind.MySql,     "MAKETIME")]
    [InlineData(ProviderKind.Sqlite,    "printf(")]
    public void Where_with_new_TimeOnly_4arg_from_int_columns_emits_provider_from_parts(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ctor = typeof(TimeOnly).GetConstructor(new[] { typeof(int), typeof(int), typeof(int), typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var h = Expression.Property(param, nameof(Row.Hour));
        var mi = Expression.Property(param, nameof(Row.Minute));
        var s = Expression.Property(param, nameof(Row.Second));
        var ms = Expression.Property(param, nameof(Row.Millisecond));
        var call = Expression.New(ctor, h, mi, s, ms);
        var body = Expression.GreaterThan(call, Expression.Constant(new TimeOnly(0, 0)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
        Assert.Contains("Millisecond", sql);
    }
}
