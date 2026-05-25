using System;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>Regex.IsMatch(col, pattern)</c> on a column was untranslated.
/// PostgreSQL has the <c>~</c> regex-match operator natively; MySQL has
/// <c>REGEXP</c>; SQLite supports <c>REGEXP</c> when a user-defined
/// function is registered (Microsoft.Data.Sqlite doesn't ship one by
/// default, but the operator syntax is still the right emit). SQL Server
/// has no native regex primitive without a CLR extension, so it must
/// throw a clear NormUnsupportedFeatureException rather than emit broken
/// SQL or silently fall back to LIKE.
/// </summary>
[Trait("Category", "Fast")]
public sealed class RegexIsMatchProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string Text { get; set; } = string.Empty;
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite, "REGEXP")]
    [InlineData(ProviderKind.Postgres, "~")]
    [InlineData(ProviderKind.MySql, "REGEXP")]
    public void Where_with_Regex_IsMatch_emits_provider_regex_operator(ProviderKind providerKind, string expectedOp)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.IsMatch), new[] { typeof(string), typeof(string) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method, text, Expression.Constant("^[A-Z]"));
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedOp, sql);
    }

    [Fact]
    public void Where_with_Regex_IsMatch_on_SqlServer_throws_with_clear_message()
    {
        var setup = CreateProvider(ProviderKind.SqlServer);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Regex).GetMethod(nameof(Regex.IsMatch), new[] { typeof(string), typeof(string) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var text = Expression.Property(param, nameof(Row.Text));
        var call = Expression.Call(method, text, Expression.Constant("^[A-Z]"));
        var lambda = Expression.Lambda<Func<Row, bool>>(call, param);

        var ex = Assert.ThrowsAny<Exception>(() => Translate<Row>(lambda, connection, provider));
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException! : ex;
        Assert.IsType<NormUnsupportedFeatureException>(inner);
        Assert.Contains("SQL Server", inner.Message);
    }
}
