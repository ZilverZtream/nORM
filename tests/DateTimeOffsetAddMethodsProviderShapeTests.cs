using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateTimeOffset.AddDays/AddMonths/AddYears/AddHours/AddMinutes/AddSeconds/
/// AddMilliseconds/AddTicks must translate per provider. DATETIMEOFFSET /
/// TIMESTAMP WITH TIME ZONE accept the same DATEADD / interval arithmetic
/// as DATETIME, so the SQL emit mirrors the DateTime equivalents.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeOffsetAddMethodsProviderShapeTests : TestBase
{
    private sealed class DtoaItem
    {
        public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }

    public static IEnumerable<object[]> AllProvidersAndMethods()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
        {
            yield return new object[] { kind, "AddDays" };
            yield return new object[] { kind, "AddMonths" };
            yield return new object[] { kind, "AddYears" };
            yield return new object[] { kind, "AddHours" };
            yield return new object[] { kind, "AddMinutes" };
            yield return new object[] { kind, "AddSeconds" };
            yield return new object[] { kind, "AddMilliseconds" };
            yield return new object[] { kind, "AddTicks" };
        }
    }

    [Theory]
    [MemberData(nameof(AllProvidersAndMethods))]
    public void Where_with_DateTimeOffset_Add_method_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var param = Expression.Parameter(typeof(DtoaItem), "e");
        var stamp = Expression.Property(param, nameof(DtoaItem.Stamp));
        // AddMonths/AddYears take int; AddTicks takes long; everything else takes double.
        Expression argExpr = methodName switch
        {
            "AddTicks" => Expression.Constant(5L),
            "AddMonths" or "AddYears" => Expression.Constant(5),
            _ => Expression.Constant(5.0)
        };
        var method = typeof(DateTimeOffset).GetMethod(methodName, new[] { argExpr.Type })!;
        var call = Expression.Call(stamp, method, argExpr);
        var gt = Expression.GreaterThan(call, Expression.Constant(threshold));
        var lambda = Expression.Lambda<Func<DtoaItem, bool>>(gt, param);

        var (sql, _) = Translate<DtoaItem>(lambda, connection, provider);
        Assert.Contains("@", sql);
    }
}
