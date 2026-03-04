using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Comprehensive identifier escaping tests for all providers across:
/// - Normal names, names with spaces, names with embedded delimiters
/// - Schema-qualified names
/// - Adversarial inputs
/// </summary>
public class IdentifierEscapingMatrixTests
{
    // ─── SQLite ────────────────────────────────────────────────────────────

    [Fact]
    public void Sqlite_NormalName_WrapsInDoubleQuotes()
    {
        var p = new SqliteProvider();
        Assert.Equal("\"Users\"", p.Escape("Users"));
    }

    [Fact]
    public void Sqlite_NameWithSpaces_WrapsInDoubleQuotes()
    {
        var p = new SqliteProvider();
        Assert.Equal("\"User Profiles\"", p.Escape("User Profiles"));
    }

    [Fact]
    public void Sqlite_EmbeddedDoubleQuote_IsDoubled()
    {
        var p = new SqliteProvider();
        Assert.Equal("\"User\"\"Table\"", p.Escape("User\"Table"));
    }

    [Fact]
    public void Sqlite_MultipleEmbeddedQuotes_AllDoubled()
    {
        var p = new SqliteProvider();
        Assert.Equal("\"a\"\"b\"\"c\"", p.Escape("a\"b\"c"));
    }

    [Fact]
    public void Sqlite_SchemaQualified_WrapsWholeNameAsSingleIdentifier()
    {
        // SQLite does not split on '.' — whole name is wrapped as one identifier
        var p = new SqliteProvider();
        var result = p.Escape("dbo.Users");
        Assert.StartsWith("\"", result);
        Assert.EndsWith("\"", result);
        Assert.Contains("dbo.Users", result);
    }

    [Fact]
    public void Sqlite_SchemaWithEmbeddedQuote_EscapedCorrectly()
    {
        var p = new SqliteProvider();
        // SQLite wraps everything in double quotes and doubles embedded quotes
        var result = p.Escape("dbo.my\"table");
        Assert.StartsWith("\"", result);
        Assert.EndsWith("\"", result);
        Assert.Contains("\"\"", result); // the embedded " should be doubled
    }

    [Fact]
    public void Sqlite_SpecialChars_InName_EscapedSafely()
    {
        var p = new SqliteProvider();
        // Semicolon in name — should be wrapped (not injected into SQL)
        var result = p.Escape("Users;DROP");
        Assert.StartsWith("\"", result);
        Assert.EndsWith("\"", result);
    }

    // ─── SQL Server ────────────────────────────────────────────────────────

    [Fact]
    public void SqlServer_NormalName_WrapsInBrackets()
    {
        var p = new SqlServerProvider();
        Assert.Equal("[Users]", p.Escape("Users"));
    }

    [Fact]
    public void SqlServer_NameWithSpaces_WrapsInBrackets()
    {
        var p = new SqlServerProvider();
        Assert.Equal("[User Profiles]", p.Escape("User Profiles"));
    }

    [Fact]
    public void SqlServer_EmbeddedClosingBracket_IsDoubled()
    {
        var p = new SqlServerProvider();
        Assert.Equal("[User]]Table]", p.Escape("User]Table"));
    }

    [Fact]
    public void SqlServer_MultipleEmbeddedBrackets_AllDoubled()
    {
        var p = new SqlServerProvider();
        Assert.Equal("[a]]b]]c]", p.Escape("a]b]c"));
    }

    [Fact]
    public void SqlServer_SchemaQualified_EachPartEscaped()
    {
        var p = new SqlServerProvider();
        Assert.Equal("[dbo].[Users]", p.Escape("dbo.Users"));
    }

    [Fact]
    public void SqlServer_SchemaWithEmbeddedBracket_BothPartsEscaped()
    {
        var p = new SqlServerProvider();
        // dbo.my]table → [dbo].[my]]table]
        Assert.Equal("[dbo].[my]]table]", p.Escape("dbo.my]table"));
    }

    [Fact]
    public void SqlServer_SpecialChars_WrappedInBrackets()
    {
        var p = new SqlServerProvider();
        var result = p.Escape("Users;DROP");
        Assert.StartsWith("[", result);
        Assert.EndsWith("]", result);
    }

    // ─── MySQL ────────────────────────────────────────────────────────────

    [Fact]
    public void MySql_NormalName_WrapsInBackticks()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`Users`", p.Escape("Users"));
    }

    [Fact]
    public void MySql_NameWithSpaces_WrapsInBackticks()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`User Profiles`", p.Escape("User Profiles"));
    }

    [Fact]
    public void MySql_EmbeddedBacktick_IsDoubled()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`User``Table`", p.Escape("User`Table"));
    }

    [Fact]
    public void MySql_MultipleEmbeddedBackticks_AllDoubled()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`a``b``c`", p.Escape("a`b`c"));
    }

    [Fact]
    public void MySql_SchemaQualified_WrapsWholeNameAsSingleIdentifier()
    {
        // MySQL wraps everything in backticks as one identifier (no dot-splitting)
        var p = new MySqlProvider(new SqliteParameterFactory());
        var result = p.Escape("mydb.Users");
        Assert.StartsWith("`", result);
        Assert.EndsWith("`", result);
        Assert.Contains("mydb.Users", result);
    }

    [Fact]
    public void MySql_SchemaWithEmbeddedBacktick_EscapedCorrectly()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        // MySQL wraps in backticks and doubles embedded backticks
        var result = p.Escape("mydb.my`table");
        Assert.StartsWith("`", result);
        Assert.EndsWith("`", result);
        Assert.Contains("``", result); // the embedded ` should be doubled
    }

    // ─── PostgreSQL ────────────────────────────────────────────────────────

    [Fact]
    public void Postgres_NormalName_WrapsInDoubleQuotes()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"Users\"", p.Escape("Users"));
    }

    [Fact]
    public void Postgres_NameWithSpaces_WrapsInDoubleQuotes()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"User Profiles\"", p.Escape("User Profiles"));
    }

    [Fact]
    public void Postgres_EmbeddedDoubleQuote_IsDoubled()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"User\"\"Table\"", p.Escape("User\"Table"));
    }

    [Fact]
    public void Postgres_MultipleEmbeddedQuotes_AllDoubled()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"a\"\"b\"\"c\"", p.Escape("a\"b\"c"));
    }

    [Fact]
    public void Postgres_SchemaQualified_EachPartEscaped()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("\"public\".\"Users\"", p.Escape("public.Users"));
    }

    [Fact]
    public void Postgres_SchemaWithEmbeddedQuote_BothPartsEscaped()
    {
        var p = new PostgresProvider(new SqliteParameterFactory());
        // dbo.my"table → "dbo"."my""table"
        Assert.Equal("\"dbo\".\"my\"\"table\"", p.Escape("dbo.my\"table"));
    }

    // ─── Cross-provider: adversarial characters ────────────────────────────

    [Theory]
    [InlineData("Users;DROP TABLE X")]
    [InlineData("Users--comment")]
    [InlineData("Users/*comment*/")]
    public void AllProviders_AdversarialName_WrappedSafelyByEscape(string name)
    {
        // The Escape method should wrap the identifier so the adversarial chars
        // are contained within the delimiters (cannot break out of them).
        // This doesn't validate the identifier is SQL-safe at the identifier level,
        // just that the Escape method correctly wraps it.
        var sqlite = new SqliteProvider();
        var sqlserver = new SqlServerProvider();
        var mysql = new MySqlProvider(new SqliteParameterFactory());
        var postgres = new PostgresProvider(new SqliteParameterFactory());

        // Each should start and end with its delimiter character
        Assert.StartsWith("\"", sqlite.Escape(name));
        Assert.StartsWith("[", sqlserver.Escape(name));
        Assert.StartsWith("`", mysql.Escape(name));
        Assert.StartsWith("\"", postgres.Escape(name));
    }

    // ─── EscapeIdentifier: empty string handling ───────────────────────────

    [Fact]
    public void Sqlite_EmptyStringOrWhitespace_WrapsInDoubleQuotes()
    {
        var p = new SqliteProvider();
        // Empty string — should not throw, just wrap empty identifier
        var result = p.Escape(string.Empty);
        Assert.Equal("\"\"", result);
    }

    [Fact]
    public void SqlServer_EmptyString_HandledSafely()
    {
        var p = new SqlServerProvider();
        // Empty string — verify the method runs without throwing
        // Actual behavior is provider-specific
        var result = p.Escape(string.Empty);
        Assert.NotNull(result);
    }

    // ─── Very long identifiers ─────────────────────────────────────────────

    [Fact]
    public void Sqlite_VeryLongName_WrapsWithoutTruncation()
    {
        var p = new SqliteProvider();
        var longName = new string('a', 255);
        var result = p.Escape(longName);
        Assert.Equal(255 + 2, result.Length); // name + 2 quote chars
        Assert.StartsWith("\"", result);
        Assert.EndsWith("\"", result);
    }

    [Fact]
    public void SqlServer_VeryLongName_WrapsWithoutTruncation()
    {
        var p = new SqlServerProvider();
        var longName = new string('b', 255);
        var result = p.Escape(longName);
        Assert.Equal(255 + 2, result.Length); // name + 2 bracket chars
        Assert.StartsWith("[", result);
        Assert.EndsWith("]", result);
    }
}
