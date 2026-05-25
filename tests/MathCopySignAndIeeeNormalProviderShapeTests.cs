using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Three specialty IEEE 754 / sign-related methods previously threw
/// NormUnsupportedFeatureException on every provider:
///   * Math.CopySign(x, y)  -- returns x with the sign of y
///   * double.IsNormal(x)   -- finite, non-zero, not subnormal
///   * double.IsSubnormal(x) -- non-zero and |x| less than min normal
///
/// All three have a portable SQL emit:
///   CopySign(x, y)  -> (ABS(x) * SIGN(y))
///   IsNormal(x)     -> (IsFinite AND x != 0 AND ABS(x) >= 2.225e-308)
///   IsSubnormal(x)  -> (x != 0 AND ABS(x) < 2.225e-308)
///
/// 2.2250738585072014E-308 is the minimum normal positive double per
/// IEEE 754. Below that the value is subnormal (the leading bit of the
/// mantissa is implied zero rather than one).
/// </summary>
[Trait("Category", "Fast")]
public sealed class MathCopySignAndIeeeNormalProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
            yield return new object[] { k };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_Math_CopySign_emits_abs_times_sign(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(nameof(Math.CopySign), new[] { typeof(double), typeof(double) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var y = Expression.Property(param, nameof(Row.Y));
        var call = Expression.Call(method, x, y);
        var body = Expression.GreaterThan(call, Expression.Constant(0.0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("ABS(", sql);
        Assert.Contains("SIGN(", sql);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_double_IsNormal_emits_finite_and_above_min_normal(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(double).GetMethod(nameof(double.IsNormal), new[] { typeof(double) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x);
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("ABS(", sql);
        Assert.Contains("2.2250738585072014E-308", sql);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_double_IsSubnormal_emits_nonzero_below_min_normal(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(double).GetMethod(nameof(double.IsSubnormal), new[] { typeof(double) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x);
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("ABS(", sql);
        Assert.Contains("2.2250738585072014E-308", sql);
        Assert.Contains("!= 0", sql);
    }
}
