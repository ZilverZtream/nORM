using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TimeOnly.IsBetween(start, end) wraps around midnight when start &gt; end:
/// IsBetween(00:30, 02:00) for value 01:00 returns true (no wrap);
/// IsBetween(22:00, 02:00) for value 23:30 also returns true (wrap).
/// Only SQLite had the CASE-based emit; the other three providers fell
/// through to NormUnsupportedFeatureException.
///
/// The emit shape is portable -- standard SQL CASE + comparison ops --
/// so each non-SQLite provider gets the same:
///   CASE WHEN start &lt;= end THEN (value &gt;= start AND value &lt; end)
///   ELSE (value &gt;= start OR value &lt; end) END
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyIsBetweenNonSqliteProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public TimeOnly T { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_TimeOnly_IsBetween_translates_to_wrap_aware_case(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(TimeOnly).GetMethod(nameof(TimeOnly.IsBetween), new[] { typeof(TimeOnly), typeof(TimeOnly) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var t = Expression.Property(param, nameof(Row.T));
        var call = Expression.Call(t, method,
            Expression.Constant(new TimeOnly(22, 0)),
            Expression.Constant(new TimeOnly(2, 0)));
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // The wrap-aware CASE makes both branches visible.
        Assert.Contains("CASE WHEN", sql);
        Assert.Contains("AND", sql);
        Assert.Contains(" OR ", sql);
    }
}
