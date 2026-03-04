using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// ID-7: Verifies that each provider correctly escapes embedded delimiter characters
/// in table/column names to prevent SQL injection through identifiers.
/// </summary>
public class IdentifierEscapingTests
{
    [Fact]
    public void Sqlite_PlainIdentifier_WrapsInDoubleQuotes()
    {
        var provider = new SqliteProvider();
        Assert.Equal("\"MyTable\"", provider.Escape("MyTable"));
    }

    [Fact]
    public void Sqlite_EmbeddedDoubleQuote_IsDoubled()
    {
        var provider = new SqliteProvider();
        // A name like na"me should become "na""me"
        Assert.Equal("\"na\"\"me\"", provider.Escape("na\"me"));
    }

    [Fact]
    public void Sqlite_MultipleEmbeddedQuotes_AllDoubled()
    {
        var provider = new SqliteProvider();
        // "a"b"c" should become "a""b""c"
        Assert.Equal("\"a\"\"b\"\"c\"", provider.Escape("a\"b\"c"));
    }

    [Fact]
    public void SqlServer_PlainIdentifier_WrapsInBrackets()
    {
        var provider = new SqlServerProvider();
        Assert.Equal("[MyTable]", provider.Escape("MyTable"));
    }

    [Fact]
    public void SqlServer_EmbeddedClosingBracket_IsDoubled()
    {
        var provider = new SqlServerProvider();
        // A name like na]me should become [na]]me]
        Assert.Equal("[na]]me]", provider.Escape("na]me"));
    }

    [Fact]
    public void SqlServer_SchemaQualified_EachPartEscaped()
    {
        var provider = new SqlServerProvider();
        // schema.table should become [schema].[table]
        Assert.Equal("[schema].[table]", provider.Escape("schema.table"));
    }

    [Fact]
    public void SqlServer_SchemaQualified_WithEmbeddedBracket_EachPartEscaped()
    {
        var provider = new SqlServerProvider();
        // dbo.my]table should become [dbo].[my]]table]
        Assert.Equal("[dbo].[my]]table]", provider.Escape("dbo.my]table"));
    }

    [Fact]
    public void MySql_PlainIdentifier_WrapsInBackticks()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("`MyTable`", provider.Escape("MyTable"));
    }

    [Fact]
    public void MySql_EmbeddedBacktick_IsDoubled()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        // A name like na`me should become `na``me`
        Assert.Equal("`na``me`", provider.Escape("na`me"));
    }

    [Fact]
    public void MySql_MultipleEmbeddedBackticks_AllDoubled()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        // a`b`c should become `a``b``c`
        Assert.Equal("`a``b``c`", provider.Escape("a`b`c"));
    }
}
