using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateTime.Nanosecond and TimeOnly.Nanosecond (.NET 7+) return the
/// nanosecond WITHIN the current microsecond. Since .NET's tick is 100ns,
/// the value is 0..900 in increments of 100. None of the providers
/// translated it.
///
/// Per-provider primitive:
///   SQL Server (DATETIME2(7) has 100ns precision):
///     (DATEPART(nanosecond, x) % 1000) -- DATEPART returns 0..999999900;
///     modulo 1000 yields the 100ns-within-microsecond.
///   PostgreSQL (timestamp has microsecond precision):
///     0 -- no sub-microsecond resolution.
///   MySQL (DATETIME(6) microsecond precision):
///     0 -- no sub-microsecond resolution.
///   SQLite: parse digit 7 of the 7-digit fractional tail, multiplied by 100.
/// </summary>
[Trait("Category", "Fast")]
public sealed class NanosecondPropertyProviderShapeTests : TestBase
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
    public void Where_with_DateTime_Nanosecond_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var stamp = Expression.Property(param, nameof(Row.Stamp));
        var ns = Expression.Property(stamp, nameof(DateTime.Nanosecond));
        var body = Expression.Equal(ns, Expression.Constant(0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.SqlServer:
                Assert.Contains("DATEPART(nanosecond", sql);
                break;
            case ProviderKind.Postgres:
            case ProviderKind.MySql:
                // Postgres / MySQL have no sub-microsecond resolution --
                // emit the literal 0 sentinel so the comparison still parses.
                Assert.StartsWith("(0 ", sql);
                break;
            case ProviderKind.Sqlite:
                Assert.Contains("substr(", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_TimeOnly_Nanosecond_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var tm = Expression.Property(param, nameof(Row.Tm));
        var ns = Expression.Property(tm, nameof(TimeOnly.Nanosecond));
        var body = Expression.Equal(ns, Expression.Constant(0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.SqlServer:
                Assert.Contains("DATEPART(nanosecond", sql);
                break;
            case ProviderKind.Postgres:
            case ProviderKind.MySql:
                Assert.StartsWith("(0 ", sql);
                break;
            case ProviderKind.Sqlite:
                Assert.Contains("substr(", sql);
                break;
        }
    }
}
