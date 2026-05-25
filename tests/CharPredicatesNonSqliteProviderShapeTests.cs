using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The static <see cref="char"/> classification + conversion methods
/// (IsDigit / IsLetter / IsLetterOrDigit / IsWhiteSpace / IsUpper /
/// IsLower / IsPunctuation / IsSymbol / IsControl / GetNumericValue +
/// ToUpper / ToLower) only translated on SQLite. The non-SQLite
/// providers fell through and threw NormUnsupportedFeatureException,
/// blocking common ASCII-classification patterns.
///
/// Each predicate is portable across providers when expressed via
/// codepoint arithmetic (BETWEEN ranges) or single-char-equality OR
/// chains. The provider-side <c>GetCharCodeSql</c> hook already
/// abstracts the codepoint primitive
/// (SQLite/SqlServer UNICODE, Postgres ascii, MySQL ORD); reuse it
/// so the emit shape is portable.
/// </summary>
[Trait("Category", "Fast")]
public sealed class CharPredicatesNonSqliteProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public char C { get; set; }
    }

    public static IEnumerable<object[]> Predicates()
    {
        foreach (var k in new[] { ProviderKind.SqlServer, ProviderKind.Postgres, ProviderKind.MySql })
        {
            yield return new object[] { k, nameof(char.IsDigit) };
            yield return new object[] { k, nameof(char.IsLetter) };
            yield return new object[] { k, nameof(char.IsLetterOrDigit) };
            yield return new object[] { k, nameof(char.IsWhiteSpace) };
            yield return new object[] { k, nameof(char.IsUpper) };
            yield return new object[] { k, nameof(char.IsLower) };
            yield return new object[] { k, nameof(char.IsControl) };
        }
    }

    [Theory]
    [MemberData(nameof(Predicates))]
    public void Where_with_char_predicate_translates_per_provider(ProviderKind providerKind, string methodName)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(char).GetMethod(methodName, new[] { typeof(char) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var c = Expression.Property(param, nameof(Row.C));
        var call = Expression.Call(method, c);
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // The emit MUST NOT contain the .NET method name verbatim
        // (which would mean the generic fallback emitted `IsDigit(...)`).
        Assert.DoesNotContain(methodName + "(", sql);
        Assert.NotEmpty(sql);
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_char_ToUpper_emits_provider_upper(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(char).GetMethod(nameof(char.ToUpper), new[] { typeof(char) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var c = Expression.Property(param, nameof(Row.C));
        var call = Expression.Call(method, c);
        var body = Expression.Equal(call, Expression.Constant('A'));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("UPPER(", sql);
    }
}
