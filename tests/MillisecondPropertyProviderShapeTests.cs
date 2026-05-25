using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <see cref="TimeOnly.Millisecond"/> wasn't in any provider's TimeOnly
/// switch -- common sub-second filters threw NormUnsupportedFeatureException.
/// <see cref="DateTime.Millisecond"/> only translated on SQLite; the other
/// three providers had AddMilliseconds covered but not the property read.
///
/// Per-provider Millisecond primitive:
///   SQL Server: DATEPART(millisecond, x)
///   PostgreSQL: ((EXTRACT(MILLISECONDS FROM x))::int % 1000)
///   MySQL:      (MICROSECOND(x) DIV 1000)
///   SQLite:     parse trailing 'fffffff' fractional digits / 10000
/// </summary>
[Trait("Category", "Fast")]
public sealed class MillisecondPropertyProviderShapeTests : TestBase
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
    [InlineData(ProviderKind.SqlServer, "DATEPART(millisecond")]
    [InlineData(ProviderKind.Postgres,  "MILLISECONDS")]
    [InlineData(ProviderKind.MySql,     "MICROSECOND(")]
    public void Where_with_DateTime_Millisecond_translates_on_non_sqlite(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var stamp = Expression.Property(param, nameof(Row.Stamp));
        var ms = Expression.Property(stamp, nameof(DateTime.Millisecond));
        var body = Expression.Equal(ms, Expression.Constant(500));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_TimeOnly_Millisecond_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var tm = Expression.Property(param, nameof(Row.Tm));
        var ms = Expression.Property(tm, nameof(TimeOnly.Millisecond));
        var body = Expression.Equal(ms, Expression.Constant(500));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("substr(", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("DATEPART(millisecond", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("MILLISECONDS", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("MICROSECOND(", sql);
                break;
        }
    }
}
