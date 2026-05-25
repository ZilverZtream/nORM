using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TimeOnly.Ticks returns the number of 100-nanosecond ticks since
/// midnight (0..863999999999). None of the providers translated it.
/// Per-provider primitive:
///   SQLite:    text parse -- H*36e9 + M*6e8 + S*1e7 + 7-digit fractional
///              tail as ticks.
///   SQL Server: DATEDIFF_BIG(NANOSECOND, '00:00:00', x) / 100
///   PostgreSQL: (EXTRACT(EPOCH FROM x) * 10000000)::bigint
///   MySQL:      (TIME_TO_SEC(x) * 10000000 + MICROSECOND(x) * 10)
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyTicksProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public TimeOnly T { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "substr(")]
    [InlineData(ProviderKind.SqlServer, "DATEDIFF_BIG")]
    [InlineData(ProviderKind.Postgres,  "EXTRACT(EPOCH")]
    [InlineData(ProviderKind.MySql,     "TIME_TO_SEC")]
    public void Where_with_TimeOnly_Ticks_translates_per_provider(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var t = Expression.Property(param, nameof(Row.T));
        var ticks = Expression.Property(t, nameof(TimeOnly.Ticks));
        var body = Expression.GreaterThan(ticks, Expression.Constant(0L));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }
}
