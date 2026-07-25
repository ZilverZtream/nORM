using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The provider string-literal escaper (defense-in-depth for the few structural sites that must inline a
/// string rather than parameterize it) must be MySQL-aware: MySQL treats backslash as a string-literal escape
/// under its default sql_mode, so a value such as <c>\'</c> defeats plain single-quote doubling. MySQL doubles
/// the backslash as well; the other providers do not need to.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ProviderStringLiteralEscapeTests
{
    [Fact]
    public void MySql_doubles_backslash_and_quote()
    {
        // Input \' (backslash, quote). MySQL: \ -> \\ then ' -> '' , wrapped -> '\\'''
        Assert.Equal(@"'\\'''", new MySqlProvider().EscapeStringLiteral(@"\'"));
    }

    [Theory]
    [InlineData("a'b", "'a''b'")] // quote always doubled
    [InlineData(@"\'", @"'\'''")] // backslash left literal (safe: these providers don't treat it as an escape)
    public void Non_mysql_providers_double_only_the_quote(string input, string expected)
    {
        Assert.Equal(expected, new SqliteProvider().EscapeStringLiteral(input));
        Assert.Equal(expected, new SqlServerProvider().EscapeStringLiteral(input));
        Assert.Equal(expected, new PostgresProvider().EscapeStringLiteral(input));
    }
}
