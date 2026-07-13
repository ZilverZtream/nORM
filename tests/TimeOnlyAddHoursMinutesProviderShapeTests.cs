using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TimeOnly.Add(TimeSpan) already routes through AddSecondsToTimeOnlySql
/// (120682d / 8aa8f14). The sister scalar overloads
/// <see cref="TimeOnly.AddHours(double)"/> and
/// <see cref="TimeOnly.AddMinutes(double)"/> were unwired -- they share
/// the receiver / return type but take a double argument, which the
/// existing branch's TimeSpan-arg check rejects. Lower these to the
/// same per-provider hook by converting hours / minutes to seconds at
/// translation time.
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyAddHoursMinutesProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public TimeOnly T { get; set; }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
        {
            yield return new object[] { k, nameof(TimeOnly.AddHours) };
            yield return new object[] { k, nameof(TimeOnly.AddMinutes) };
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Where_with_TimeOnly_AddHours_or_AddMinutes_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(TimeOnly).GetMethod(methodName, new[] { typeof(double) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var t = Expression.Property(param, nameof(Row.T));
        var call = Expression.Call(t, method, Expression.Constant(2.0));
        var body = Expression.GreaterThan(call, Expression.Constant(new TimeOnly(10, 0)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // A successful translate IS the pin -- prior state was
        // NormUnsupportedFeatureException at translation time. Per-provider
        // anchors:
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("strftime", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("DATEADD(", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("interval", sql, StringComparison.OrdinalIgnoreCase);
                break;
            case ProviderKind.MySql:
                // .NET TimeOnly arithmetic wraps around midnight; MySQL's ADDTIME
                // does not, so the emit folds seconds through a positive modulo.
                Assert.Contains("SEC_TO_TIME(MOD(", sql);
                break;
        }
    }
}
