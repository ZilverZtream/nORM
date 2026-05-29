using System;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Sister of Regex.IsMatch (ea42706): <c>Regex.Replace(input, pattern,
/// replacement)</c> lowers to the provider's regex-replace primitive.
/// PostgreSQL has <c>regexp_replace</c>, MySQL has <c>REGEXP_REPLACE</c>
/// (8.0+), SQLite uses the deterministic managed <c>regexp_replace</c>
/// function registered by <see cref="SqliteProvider"/>. SQL Server has
/// no native regex primitive; nORM lowers literal-pattern replacement to
/// T-SQL REPLACE and surfaces a clear unsupported-feature exception for
/// regex constructs that cannot be preserved.
/// </summary>
[Trait("Category", "Fast")]
public sealed class RegexReplaceProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string Text { get; set; } = string.Empty;
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,   "regexp_replace(")]
    [InlineData(ProviderKind.Postgres, "regexp_replace(")]
    [InlineData(ProviderKind.MySql,    "REGEXP_REPLACE(")]
    [InlineData(ProviderKind.SqlServer, "REPLACE(")]
    public void Where_with_Regex_Replace_emits_provider_function(ProviderKind providerKind, string expectedFn)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.Replace),
            new[] { typeof(string), typeof(string), typeof(string) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method, text, Expression.Constant("alpha"), Expression.Constant("#"));
        var body = Expression.Equal(call, Expression.Constant("price: #"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFn, sql);
    }

    [Fact]
    public void Where_with_complex_Regex_Replace_on_SqlServer_throws_with_clear_message()
    {
        var setup = CreateProvider(ProviderKind.SqlServer);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.Replace),
            new[] { typeof(string), typeof(string), typeof(string) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method, text, Expression.Constant("[0-9]+"), Expression.Constant("#"));
        var body = Expression.Equal(call, Expression.Constant("price: #"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var ex = Assert.ThrowsAny<Exception>(() => Translate<Row>(lambda, connection, provider));
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException! : ex;
        Assert.IsType<NormUnsupportedFeatureException>(inner);
        Assert.Contains("SQL Server", inner.Message);
    }

    [Fact]
    public void Where_with_literal_Regex_Replace_IgnoreCase_on_SqlServer_lowers_to_REPLACE_with_case_insensitive_collation()
    {
        var setup = CreateProvider(ProviderKind.SqlServer);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.Replace),
            new[] { typeof(string), typeof(string), typeof(string), typeof(RegexOptions) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method,
            text,
            Expression.Constant("alpha"),
            Expression.Constant("#"),
            Expression.Constant(RegexOptions.IgnoreCase));
        var body = Expression.Equal(call, Expression.Constant("price: #"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("REPLACE(", sql);
        Assert.Contains("Latin1_General_CI_AS", sql);
    }
}
