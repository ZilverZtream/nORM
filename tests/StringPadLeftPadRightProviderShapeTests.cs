using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// string.PadLeft / PadRight (both width-only and width+char overloads)
/// were untranslated on SqlServer / Postgres / MySQL. PostgreSQL and MySQL
/// expose LPAD / RPAD natively; SQL Server has no LPAD primitive and
/// must construct via REPLICATE + concatenation, gated by a CASE so the
/// .NET "never truncates" semantics hold for inputs longer than the
/// target width.
/// </summary>
[Trait("Category", "Fast")]
public sealed class StringPadLeftPadRightProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string S { get; set; } = string.Empty;
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(string.PadLeft),  /*hasChar*/ false };
            yield return new object[] { k, nameof(string.PadLeft),  /*hasChar*/ true };
            yield return new object[] { k, nameof(string.PadRight), /*hasChar*/ false };
            yield return new object[] { k, nameof(string.PadRight), /*hasChar*/ true };
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Where_with_pad_method_translates_per_provider(ProviderKind providerKind, string methodName, bool hasChar)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = hasChar
            ? typeof(string).GetMethod(methodName, new[] { typeof(int), typeof(char) })!
            : typeof(string).GetMethod(methodName, new[] { typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var s = Expression.Property(param, nameof(Row.S));
        Expression call = hasChar
            ? Expression.Call(s, method, Expression.Constant(10), Expression.Constant('-'))
            : Expression.Call(s, method, Expression.Constant(10));
        var body = Expression.Equal(call, Expression.Constant("0000000abc"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);

        switch (providerKind)
        {
            case ProviderKind.Postgres:
            case ProviderKind.MySql:
                Assert.Contains(methodName == nameof(string.PadLeft) ? "LPAD(" : "RPAD(", sql);
                break;
            case ProviderKind.SqlServer:
                // No native LPAD: must construct via REPLICATE + concat.
                Assert.Contains("REPLICATE(", sql);
                break;
        }
    }
}
