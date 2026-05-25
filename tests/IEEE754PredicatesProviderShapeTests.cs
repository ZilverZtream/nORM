using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// IEEE 754 predicates -- double.IsNaN, IsInfinity, IsFinite,
/// IsPositiveInfinity, IsNegativeInfinity -- were only implemented on
/// SQLite. The non-SQLite providers fell through and threw
/// NormUnsupportedFeatureException, blocking common float-safety
/// patterns like <c>Where(x => !double.IsNaN(x.Score))</c>.
///
/// Add portable emit per provider:
/// * PostgreSQL: native isnan(x) for IsNaN; CAST('Infinity' AS float8)
///   comparison for IsInfinity (PostgreSQL is the only provider that
///   stores +/-Infinity in DOUBLE PRECISION columns).
/// * SQL Server / MySQL: emit the algebraic (x != x) for IsNaN. Those
///   engines reject NaN/Inf at insert by default so the predicate is
///   typically false for stored values; the emit is still correct for
///   computed expressions (e.g. division-by-zero columns).
/// </summary>
[Trait("Category", "Fast")]
public sealed class IEEE754PredicatesProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public double X { get; set; }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(double.IsNaN) };
            yield return new object[] { k, nameof(double.IsInfinity) };
            yield return new object[] { k, nameof(double.IsFinite) };
            yield return new object[] { k, nameof(double.IsPositiveInfinity) };
            yield return new object[] { k, nameof(double.IsNegativeInfinity) };
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Where_with_double_predicate_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(double).GetMethod(methodName, new[] { typeof(double) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x);
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);

        // The shape anchor: every provider must emit SOMETHING (no throw)
        // and the SQL must not literally contain `IsNaN(` (which would mean
        // the generic fallback path emitted the .NET method name verbatim).
        Assert.NotEmpty(sql);
        Assert.DoesNotContain(methodName + "(", sql);

        // Per-provider anchor for IsNaN:
        if (methodName == nameof(double.IsNaN))
        {
            switch (providerKind)
            {
                case ProviderKind.Postgres:
                    Assert.Contains("isnan(", sql);
                    break;
                case ProviderKind.SqlServer:
                case ProviderKind.MySql:
                    // Algebraic form: (x != x) is the only IEEE value that
                    // doesn't equal itself.
                    Assert.Contains("!=", sql);
                    break;
            }
        }
    }
}
