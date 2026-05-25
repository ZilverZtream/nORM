using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>Math.Round(x, MidpointRounding)</c> and <c>decimal.Round(x,
/// MidpointRounding)</c> were silently routed to the integer-digits arm
/// on every provider except SQLite -- the enum's underlying int value
/// would be passed as the rounding-digits argument, producing
/// <c>ROUND(x, 1)</c> for AwayFromZero (numeric 1) or <c>ROUND(x, 0)</c>
/// for ToEven, neither matching .NET semantics.
///
/// Add per-provider TranslateMethodCall overrides on SqlServer /
/// Postgres / MySQL that explicitly dispatch on the MidpointRounding
/// value the same way SQLite already does.
/// </summary>
[Trait("Category", "Fast")]
public sealed class MathRoundMidpointRoundingProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public double X { get; set; }
        public decimal D { get; set; }
    }

    public static IEnumerable<object[]> AwayFromZeroCases()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
            yield return new object[] { k };
    }

    [Theory]
    [MemberData(nameof(AwayFromZeroCases))]
    public void Where_with_Math_Round_AwayFromZero_emits_native_round(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // Math.Round(row.X, MidpointRounding.AwayFromZero) == 1.0
        var method = typeof(Math).GetMethod(nameof(Math.Round), new[] { typeof(double), typeof(MidpointRounding) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x, Expression.Constant(MidpointRounding.AwayFromZero));
        var body = Expression.Equal(call, Expression.Constant(1.0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // All three non-SQLite providers default ROUND(x [, n]) to
        // AwayFromZero -- assert ROUND( appears and the enum integer
        // value (1) is NOT present as a digit argument (which would
        // mean the silent-wrongness arm fired).
        Assert.Contains("ROUND(", sql);
        Assert.DoesNotContain("ROUND(\"r\".\"X\", 1)", sql);
    }

    [Theory]
    [MemberData(nameof(AwayFromZeroCases))]
    public void Where_with_Math_Round_ToZero_truncates_toward_zero(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(nameof(Math.Round), new[] { typeof(double), typeof(MidpointRounding) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x, Expression.Constant(MidpointRounding.ToZero));
        var body = Expression.Equal(call, Expression.Constant(1.0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // ToZero = truncate-toward-zero -- per provider:
        //   SqlServer ROUND(x, 0, 1)  (truncate flag)
        //   Postgres  TRUNC(x)
        //   MySQL     TRUNCATE(x, 0)
        switch (providerKind)
        {
            case ProviderKind.SqlServer:
                Assert.Contains("ROUND(", sql);
                Assert.Contains(", 0, 1", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("TRUNC(", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("TRUNCATE(", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(AwayFromZeroCases))]
    public void Where_with_Math_Round_ToNegativeInfinity_emits_floor(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(nameof(Math.Round), new[] { typeof(double), typeof(MidpointRounding) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x, Expression.Constant(MidpointRounding.ToNegativeInfinity));
        var body = Expression.Equal(call, Expression.Constant(1.0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("FLOOR(", sql);
    }

    [Theory]
    [MemberData(nameof(AwayFromZeroCases))]
    public void Where_with_Math_Round_ToPositiveInfinity_emits_ceiling(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Math).GetMethod(nameof(Math.Round), new[] { typeof(double), typeof(MidpointRounding) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var x = Expression.Property(param, nameof(Row.X));
        var call = Expression.Call(method, x, Expression.Constant(MidpointRounding.ToPositiveInfinity));
        var body = Expression.Equal(call, Expression.Constant(1.0));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // SqlServer spells it CEILING; Postgres / MySQL spell CEILING too,
        // SQLite uses CEIL. Either form is acceptable per provider.
        Assert.True(sql.Contains("CEILING(") || sql.Contains("CEIL("),
            $"Expected CEILING( or CEIL( in SQL but got: {sql}");
    }
}
