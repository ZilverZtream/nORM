using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Hyperbolic (Sinh/Cosh/Tanh) and inverse hyperbolic (Asinh/Acosh/Atanh)
/// trig functions exist as built-ins on SQLite (math extension) and
/// PostgreSQL, but SQL Server and MySQL have no native equivalents. The
/// translator must emit the algebraic identities via exp / ln so the LINQ
/// expressions still translate uniformly:
///   sinh(x) = (exp(x) - exp(-x)) / 2
///   cosh(x) = (exp(x) + exp(-x)) / 2
///   tanh(x) = (exp(2x) - 1) / (exp(2x) + 1)
///   asinh(x) = ln(x + sqrt(x^2 + 1))
///   acosh(x) = ln(x + sqrt(x^2 - 1))
///   atanh(x) = 0.5 * ln((1+x) / (1-x))
///
/// This pin covers the 18-entry matrix (6 methods x 3 non-SQLite
/// providers) so the chosen emit shape is locked in.
/// </summary>
[Trait("Category", "Fast")]
public sealed class MathHyperbolicTrigProviderShapeTests : TestBase
{
    private sealed class Vec
    {
        public int Id { get; set; }
        public double X { get; set; }
    }

    public static IEnumerable<object[]> NonSqliteProvidersAndMethods()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(Math.Sinh) };
            yield return new object[] { k, nameof(Math.Cosh) };
            yield return new object[] { k, nameof(Math.Tanh) };
            yield return new object[] { k, nameof(Math.Asinh) };
            yield return new object[] { k, nameof(Math.Acosh) };
            yield return new object[] { k, nameof(Math.Atanh) };
        }
    }

    [Theory]
    [MemberData(nameof(NonSqliteProvidersAndMethods))]
    public void Where_with_hyperbolic_method_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(methodName, new[] { typeof(double) })!;
        var param = Expression.Parameter(typeof(Vec), "v");
        var x = Expression.Property(param, nameof(Vec.X));
        var call = Expression.Call(method, x);
        var body = Expression.GreaterThan(call, Expression.Constant(0.0));
        var lambda = Expression.Lambda<Func<Vec, bool>>(body, param);

        var (sql, _) = Translate<Vec>(lambda, connection, provider);

        switch (providerKind)
        {
            case ProviderKind.Postgres:
                // PostgreSQL: native function names exist with .NET-matching names.
                Assert.Contains(methodName.ToUpperInvariant() + "(", sql);
                break;
            case ProviderKind.SqlServer:
            case ProviderKind.MySql:
                // No native hyperbolic functions; the emit must reduce to EXP /
                // LN / SQRT for the algebraic identities.
                switch (methodName)
                {
                    case nameof(Math.Sinh):
                    case nameof(Math.Cosh):
                    case nameof(Math.Tanh):
                        Assert.Contains("EXP(", sql);
                        break;
                    case nameof(Math.Asinh):
                    case nameof(Math.Acosh):
                        Assert.Contains("LOG(", sql); // SqlServer LOG; MySQL LN/LOG -- both at least mention LOG
                        Assert.Contains("SQRT(", sql);
                        break;
                    case nameof(Math.Atanh):
                        Assert.Contains("LOG(", sql);
                        break;
                }
                break;
        }
    }
}
