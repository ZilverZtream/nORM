using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Basic trigonometry (Sin/Cos/Tan/Asin/Acos/Atan/Atan2) was missing from
/// every provider's TranslateFunction switch. Each function maps to native
/// SQL in all four providers, with the lone notable wrinkle that SQL Server
/// spells two-argument arctangent ATN2 (not ATAN2). This pin covers the
/// 28-entry matrix so future refactoring of the math switches can't
/// silently drop a function.
/// </summary>
[Trait("Category", "Fast")]
public sealed class MathBasicTrigProviderShapeTests : TestBase
{
    private sealed class Vec
    {
        public int Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
    }

    public static IEnumerable<object[]> ProvidersAndUnary()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
        {
            yield return new object[] { k, nameof(Math.Sin) };
            yield return new object[] { k, nameof(Math.Cos) };
            yield return new object[] { k, nameof(Math.Tan) };
            yield return new object[] { k, nameof(Math.Asin) };
            yield return new object[] { k, nameof(Math.Acos) };
            yield return new object[] { k, nameof(Math.Atan) };
        }
    }

    [Theory]
    [MemberData(nameof(ProvidersAndUnary))]
    public void Where_with_unary_trig_method_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(methodName, new[] { typeof(double) })!;
        var param = Expression.Parameter(typeof(Vec), "v");
        var x = Expression.Property(param, nameof(Vec.X));
        var call = Expression.Call(method, x);
        var body = Expression.GreaterThan(call, Expression.Constant(0.5));
        var lambda = Expression.Lambda<Func<Vec, bool>>(body, param);

        var (sql, _) = Translate<Vec>(lambda, connection, provider);
        // The function name maps 1:1 in all providers; assert the
        // upper-case name appears verbatim in the emitted SQL.
        Assert.Contains(methodName.ToUpperInvariant() + "(", sql);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite, "ATAN2")]
    [InlineData(ProviderKind.SqlServer, "ATN2")]   // T-SQL spells this ATN2.
    [InlineData(ProviderKind.Postgres, "ATAN2")]
    [InlineData(ProviderKind.MySql, "ATAN2")]
    public void Where_with_Atan2_translates_per_provider_with_correct_spelling(ProviderKind providerKind, string expectedFn)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(nameof(Math.Atan2), new[] { typeof(double), typeof(double) })!;
        var param = Expression.Parameter(typeof(Vec), "v");
        var y = Expression.Property(param, nameof(Vec.Y));
        var x = Expression.Property(param, nameof(Vec.X));
        var call = Expression.Call(method, y, x);
        var body = Expression.GreaterThan(call, Expression.Constant(0.0));
        var lambda = Expression.Lambda<Func<Vec, bool>>(body, param);

        var (sql, _) = Translate<Vec>(lambda, connection, provider);
        Assert.Contains(expectedFn + "(", sql);
    }
}
