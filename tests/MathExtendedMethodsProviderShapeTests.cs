using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Six Math methods exist on SQLite via the 3.35+ math extension or
/// inline emit but were absent from the other three providers' switches:
///
///   Math.Cbrt(double)             cube root
///   Math.Log2(double)             base-2 logarithm
///   Math.MaxMagnitude(d, d)       returns whichever argument has the larger |x|
///   Math.MinMagnitude(d, d)       returns whichever argument has the smaller |x|
///   Math.ScaleB(double, int)      x * 2^n
///   Math.BigMul(int, int) -> long widening multiply (must cast to avoid 32-bit overflow)
///
/// None of the three non-SQLite providers had native primitives for
/// MaxMagnitude / MinMagnitude / ScaleB / BigMul, so each requires an
/// identity emit. Cbrt and Log2 derive from POW / LOG. This pin covers
/// the 18-entry matrix (6 methods x 3 providers).
/// </summary>
[Trait("Category", "Fast")]
public sealed class MathExtendedMethodsProviderShapeTests : TestBase
{
    private sealed class Vec
    {
        public int Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
        public int Ai { get; set; }
        public int Bi { get; set; }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(Math.Cbrt) };
            yield return new object[] { k, nameof(Math.Log2) };
            yield return new object[] { k, nameof(Math.MaxMagnitude) };
            yield return new object[] { k, nameof(Math.MinMagnitude) };
            yield return new object[] { k, nameof(Math.ScaleB) };
            yield return new object[] { k, nameof(Math.BigMul) };
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Where_with_extended_math_method_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Vec), "v");
        Expression call;
        switch (methodName)
        {
            case nameof(Math.Cbrt):
            case nameof(Math.Log2):
                call = Expression.Call(typeof(Math).GetMethod(methodName, new[] { typeof(double) })!,
                    Expression.Property(param, nameof(Vec.X)));
                break;
            case nameof(Math.MaxMagnitude):
            case nameof(Math.MinMagnitude):
                call = Expression.Call(typeof(Math).GetMethod(methodName, new[] { typeof(double), typeof(double) })!,
                    Expression.Property(param, nameof(Vec.X)),
                    Expression.Property(param, nameof(Vec.Y)));
                break;
            case nameof(Math.ScaleB):
                call = Expression.Call(typeof(Math).GetMethod(methodName, new[] { typeof(double), typeof(int) })!,
                    Expression.Property(param, nameof(Vec.X)),
                    Expression.Constant(3));
                break;
            case nameof(Math.BigMul):
                call = Expression.Call(typeof(Math).GetMethod(methodName, new[] { typeof(int), typeof(int) })!,
                    Expression.Property(param, nameof(Vec.Ai)),
                    Expression.Property(param, nameof(Vec.Bi)));
                break;
            default:
                throw new NotSupportedException(methodName);
        }
        // Compare against a literal of the call's type.
        Expression rhs = call.Type == typeof(long) ? Expression.Constant(0L) : Expression.Constant(0.0);
        var body = Expression.GreaterThan(Expression.Convert(call, rhs.Type), rhs);
        var lambda = Expression.Lambda<Func<Vec, bool>>(body, param);

        var (sql, _) = Translate<Vec>(lambda, connection, provider);

        // Per-method emit anchors. The shape must include the dominant
        // primitive each derivation uses; this rejects fall-through paths
        // (no-op identity, wrong function name, missed widening).
        switch (methodName)
        {
            case nameof(Math.Cbrt):
                // Cbrt = POW(x, 1/3) on every non-SQLite provider.
                Assert.Contains(providerKind == ProviderKind.SqlServer ? "POWER(" : "POW(", sql);
                break;
            case nameof(Math.Log2):
                // Log2 = LOG(2, x) on SqlServer (2-arg LOG), LOG(2, x) on Postgres/MySQL.
                Assert.Contains("LOG(", sql);
                break;
            case nameof(Math.MaxMagnitude):
            case nameof(Math.MinMagnitude):
                Assert.Contains("ABS(", sql);
                Assert.Contains("CASE WHEN", sql);
                break;
            case nameof(Math.ScaleB):
                Assert.Contains(providerKind == ProviderKind.SqlServer ? "POWER(" : "POW(", sql);
                break;
            case nameof(Math.BigMul):
                // Must widen to 64-bit before multiplying or 2^31 overflows.
                Assert.Contains("CAST", sql);
                break;
        }
    }
}
