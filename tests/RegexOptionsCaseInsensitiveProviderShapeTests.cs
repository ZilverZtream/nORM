using System;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Regex.IsMatch (ea42706) and Regex.Replace (7efbf88) only matched the
/// 2-arg / 3-arg static overloads. The <see cref="RegexOptions"/> overloads
/// (<c>Regex.IsMatch(input, pattern, options)</c> and
/// <c>Regex.Replace(input, pattern, repl, options)</c>) fell through and
/// threw NormUnsupportedFeatureException for everyone, including providers
/// with a native case-insensitive regex primitive.
///
/// Add per-provider case-insensitive routing for <c>RegexOptions.IgnoreCase</c>:
/// PostgreSQL has <c>~*</c> for case-insensitive match; MySQL / SQLite emit
/// <c>LOWER(input) REGEXP LOWER(pattern)</c> (the portable shape that works
/// regardless of the REGEXP UDF's case-sensitivity); SqlServer continues to
/// throw -- no T-SQL regex primitive of any kind.
/// </summary>
[Trait("Category", "Fast")]
public sealed class RegexOptionsCaseInsensitiveProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string Text { get; set; } = string.Empty;
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,   "LOWER(")]
    [InlineData(ProviderKind.Postgres, "~*")]
    [InlineData(ProviderKind.MySql,    "LOWER(")]
    public void Where_with_Regex_IsMatch_IgnoreCase_emits_case_insensitive_form(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.IsMatch),
            new[] { typeof(string), typeof(string), typeof(RegexOptions) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method,
            text,
            Expression.Constant("^[a-z]"),
            Expression.Constant(RegexOptions.IgnoreCase));
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,   "LOWER(")]
    [InlineData(ProviderKind.Postgres, "'gi'")]   // native flag preserves non-matched case
    [InlineData(ProviderKind.MySql,    "LOWER(")]
    public void Where_with_Regex_Replace_IgnoreCase_emits_case_insensitive_form(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.Replace),
            new[] { typeof(string), typeof(string), typeof(string), typeof(RegexOptions) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method,
            text,
            Expression.Constant("[0-9]+"),
            Expression.Constant("#"),
            Expression.Constant(RegexOptions.IgnoreCase));
        var body = Expression.Equal(call, Expression.Constant("price: #"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Fact]
    public void Where_with_Regex_IsMatch_IgnoreCase_on_SqlServer_still_throws()
    {
        var setup = CreateProvider(ProviderKind.SqlServer);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.IsMatch),
            new[] { typeof(string), typeof(string), typeof(RegexOptions) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method,
            text,
            Expression.Constant("^[a-z]"),
            Expression.Constant(RegexOptions.IgnoreCase));
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var ex = Assert.ThrowsAny<Exception>(() => Translate<Row>(lambda, connection, provider));
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException! : ex;
        Assert.IsType<NormUnsupportedFeatureException>(inner);
    }
}
