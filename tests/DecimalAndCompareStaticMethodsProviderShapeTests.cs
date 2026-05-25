using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Across the three non-SQLite providers (SqlServer / Postgres / MySQL),
/// the entire <c>decimal</c> static-method family was untranslated
/// (Truncate / Floor / Ceiling / Abs / Add / Subtract / Multiply /
/// Divide / Remainder / Negate). DateTime.Compare, DateTime.CompareTo,
/// and TimeSpan.Compare were also missing -- all three return the
/// signed comparison int (-1 / 0 / 1) common to .NET CompareTo
/// contracts.
///
/// SQLite already implements all of these. This pin covers the missing
/// 39-entry matrix.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DecimalAndCompareStaticMethodsProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public decimal A { get; set; }
        public decimal B { get; set; }
        public DateTime D1 { get; set; }
        public DateTime D2 { get; set; }
        public TimeSpan T1 { get; set; }
        public TimeSpan T2 { get; set; }
    }

    public static IEnumerable<object[]> DecimalCases()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(decimal.Truncate) };
            yield return new object[] { k, nameof(decimal.Floor) };
            yield return new object[] { k, nameof(decimal.Ceiling) };
            yield return new object[] { k, nameof(decimal.Abs) };
            yield return new object[] { k, nameof(decimal.Add) };
            yield return new object[] { k, nameof(decimal.Subtract) };
            yield return new object[] { k, nameof(decimal.Multiply) };
            yield return new object[] { k, nameof(decimal.Divide) };
            yield return new object[] { k, nameof(decimal.Remainder) };
            yield return new object[] { k, nameof(decimal.Negate) };
        }
    }

    [Theory]
    [MemberData(nameof(DecimalCases))]
    public void Where_with_decimal_static_method_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var a = Expression.Property(param, nameof(Row.A));
        var b = Expression.Property(param, nameof(Row.B));
        Expression call;
        switch (methodName)
        {
            case nameof(decimal.Truncate):
            case nameof(decimal.Floor):
            case nameof(decimal.Ceiling):
            case nameof(decimal.Abs):
            case nameof(decimal.Negate):
                call = Expression.Call(typeof(decimal).GetMethod(methodName, new[] { typeof(decimal) })!, a);
                break;
            case nameof(decimal.Add):
            case nameof(decimal.Subtract):
            case nameof(decimal.Multiply):
            case nameof(decimal.Divide):
            case nameof(decimal.Remainder):
                call = Expression.Call(typeof(decimal).GetMethod(methodName, new[] { typeof(decimal), typeof(decimal) })!, a, b);
                break;
            default:
                throw new NotSupportedException(methodName);
        }
        var body = Expression.GreaterThan(call, Expression.Constant(0m));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        // A successful Translate without throwing IS the pin -- the prior
        // state was NormUnsupportedFeatureException at translation time.
        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.NotEmpty(sql);
    }

    public static IEnumerable<object[]> CompareCases()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, "DateTime", nameof(DateTime.Compare) };
            yield return new object[] { k, "DateTime", nameof(DateTime.CompareTo) };
            yield return new object[] { k, "TimeSpan", nameof(TimeSpan.Compare) };
        }
    }

    [Theory]
    [MemberData(nameof(CompareCases))]
    public void Where_with_compare_static_method_translates_per_provider(ProviderKind providerKind, string typeName, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        Expression call;
        if (typeName == "DateTime")
        {
            var d1 = Expression.Property(param, nameof(Row.D1));
            var d2 = Expression.Property(param, nameof(Row.D2));
            if (methodName == nameof(DateTime.Compare))
            {
                call = Expression.Call(typeof(DateTime).GetMethod(methodName, new[] { typeof(DateTime), typeof(DateTime) })!, d1, d2);
            }
            else // CompareTo (instance)
            {
                call = Expression.Call(d1, typeof(DateTime).GetMethod(methodName, new[] { typeof(DateTime) })!, d2);
            }
        }
        else // TimeSpan
        {
            var t1 = Expression.Property(param, nameof(Row.T1));
            var t2 = Expression.Property(param, nameof(Row.T2));
            call = Expression.Call(typeof(TimeSpan).GetMethod(methodName, new[] { typeof(TimeSpan), typeof(TimeSpan) })!, t1, t2);
        }
        var body = Expression.GreaterThan(call, Expression.Constant(0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // Compare emits a -1/0/1 sign via CASE WHEN.
        Assert.Contains("CASE WHEN", sql);
    }
}
