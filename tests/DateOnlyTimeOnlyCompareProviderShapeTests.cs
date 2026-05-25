using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateOnly.Compare / CompareTo and TimeOnly.Compare / CompareTo were
/// missing on every provider. The DateTime.Compare / CompareTo and
/// TimeSpan.Compare / CompareTo emits (b6f3fe3) use the standard
/// signed CASE-WHEN sentinel and the same shape is portable for these
/// new sibling types -- DATE / TIME values compare natively via
/// less-than / greater-than across every SQL engine.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateOnlyTimeOnlyCompareProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public DateOnly D1 { get; set; }
        public DateOnly D2 { get; set; }
        public TimeOnly T1 { get; set; }
        public TimeOnly T2 { get; set; }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
        {
            // .NET DateOnly / TimeOnly only expose CompareTo (instance);
            // no static Compare like DateTime / TimeSpan.
            yield return new object[] { k, "DateOnly" };
            yield return new object[] { k, "TimeOnly" };
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Where_with_CompareTo_emits_signed_CASE(ProviderKind providerKind, string typeName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        Expression call;
        if (typeName == "DateOnly")
        {
            var a = Expression.Property(param, nameof(Row.D1));
            var b = Expression.Property(param, nameof(Row.D2));
            call = Expression.Call(a, typeof(DateOnly).GetMethod(nameof(DateOnly.CompareTo), new[] { typeof(DateOnly) })!, b);
        }
        else
        {
            var a = Expression.Property(param, nameof(Row.T1));
            var b = Expression.Property(param, nameof(Row.T2));
            call = Expression.Call(a, typeof(TimeOnly).GetMethod(nameof(TimeOnly.CompareTo), new[] { typeof(TimeOnly) })!, b);
        }
        var body = Expression.GreaterThan(call, Expression.Constant(0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("CASE WHEN", sql);
    }
}
