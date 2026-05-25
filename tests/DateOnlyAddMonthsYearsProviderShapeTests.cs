using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateOnly.AddDays already routes through AddDaysToDateOnlySql across
/// all four providers (b554e1f). The sister methods AddMonths and
/// AddYears were never wired up and currently throw
/// NormUnsupportedFeatureException at translation time. Add two parallel
/// provider hooks (AddMonthsToDateOnlySql, AddYearsToDateOnlySql) and
/// route ETSV / SCV through them.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateOnlyAddMonthsYearsProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public DateOnly D { get; set; }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
        {
            yield return new object[] { k, nameof(DateOnly.AddMonths) };
            yield return new object[] { k, nameof(DateOnly.AddYears) };
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Where_with_DateOnly_AddMonths_or_AddYears_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(DateOnly).GetMethod(methodName, new[] { typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var d = Expression.Property(param, nameof(Row.D));
        var call = Expression.Call(d, method, Expression.Constant(3));
        var body = Expression.GreaterThan(call, Expression.Constant(new DateOnly(2026, 1, 1)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        // A successful Translate without throwing IS the pin -- the prior
        // state was NormUnsupportedFeatureException.
        var (sql, _) = Translate<Row>(lambda, connection, provider);

        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("strftime", sql);
                Assert.Contains(methodName == nameof(DateOnly.AddMonths) ? "months" : "years", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("DATEADD(", sql);
                Assert.Contains(methodName == nameof(DateOnly.AddMonths) ? "MONTH" : "YEAR", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("INTERVAL", sql);
                Assert.Contains(methodName == nameof(DateOnly.AddMonths) ? "month" : "year", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("DATE_ADD(", sql);
                Assert.Contains(methodName == nameof(DateOnly.AddMonths) ? "MONTH" : "YEAR", sql);
                break;
        }
    }
}
