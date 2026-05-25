using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateTime.Microsecond and TimeOnly.Microsecond (added in .NET 7) were
/// missing on every provider. The property returns the microsecond
/// component WITHIN the current millisecond (0..999), not the
/// sub-second microsecond. So a value with ticks = 1.2345678 seconds
/// has Millisecond = 234 and Microsecond = 567.
///
/// Per-provider primitive:
///   SQL Server: (DATEPART(microsecond, x) % 1000)
///   PostgreSQL: ((EXTRACT(MICROSECONDS FROM x))::bigint % 1000)
///   MySQL:      (MICROSECOND(x) % 1000)
///   SQLite:     parse digits 4..6 of the 7-digit fractional tail
/// </summary>
[Trait("Category", "Fast")]
public sealed class MicrosecondPropertyProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeOnly Tm { get; set; }
    }

    public static IEnumerable<object[]> Providers()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
            yield return new object[] { k };
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_DateTime_Microsecond_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var stamp = Expression.Property(param, nameof(Row.Stamp));
        var us = Expression.Property(stamp, nameof(DateTime.Microsecond));
        var body = Expression.Equal(us, Expression.Constant(500));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                // SQLite parses the fractional substring (digits 4..6 of the
                // 7-digit tail). No modulo needed -- substr already isolates
                // the microsecond-within-millisecond digits.
                Assert.Contains("substr(", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("DATEPART(microsecond", sql);
                Assert.Contains("% 1000", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("MICROSECONDS", sql);
                Assert.Contains("% 1000", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("MICROSECOND(", sql);
                Assert.Contains("% 1000", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_TimeOnly_Microsecond_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var tm = Expression.Property(param, nameof(Row.Tm));
        var us = Expression.Property(tm, nameof(TimeOnly.Microsecond));
        var body = Expression.Equal(us, Expression.Constant(500));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("substr(", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("DATEPART(microsecond", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("MICROSECONDS", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("MICROSECOND(", sql);
                break;
        }
    }
}
