using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TimeOnly.FromDateTime(dt) and TimeOnly.FromTimeSpan(ts) only
/// translated on SQLite. The other three providers fell through and
/// threw NormUnsupportedFeatureException, blocking common shape
/// conversions like Where(p => TimeOnly.FromDateTime(p.Stamp) > cutoff).
///
/// Per-provider TIME-from-DATETIME / TIME-from-INTERVAL primitive:
///   SQL Server: CAST(dt AS TIME)
///   PostgreSQL: ({dt})::time
///   MySQL:      TIME(dt)
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyFromMethodsNonSqliteShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Duration { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "CAST(")]
    [InlineData(ProviderKind.Postgres,  "::time")]
    [InlineData(ProviderKind.MySql,     "TIME(")]
    public void Where_with_TimeOnly_FromDateTime_translates_per_provider(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(TimeOnly).GetMethod(nameof(TimeOnly.FromDateTime), new[] { typeof(DateTime) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var stamp = Expression.Property(param, nameof(Row.Stamp));
        var call = Expression.Call(method, stamp);
        var body = Expression.GreaterThan(call, Expression.Constant(new TimeOnly(12, 0)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_TimeOnly_FromTimeSpan_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(TimeOnly).GetMethod(nameof(TimeOnly.FromTimeSpan), new[] { typeof(TimeSpan) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var dur = Expression.Property(param, nameof(Row.Duration));
        var call = Expression.Call(method, dur);
        var body = Expression.GreaterThan(call, Expression.Constant(new TimeOnly(0, 30)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        // A successful Translate without throwing IS the pin -- prior state
        // was NormUnsupportedFeatureException at translation time.
        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.NotEmpty(sql);
    }
}
