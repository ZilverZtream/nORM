using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using nORM.Providers;
using Xunit;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;

namespace nORM.Tests;

//<summary>
//Verifies that each provider correctly escapes embedded delimiter characters
//in table/column names to prevent SQL injection through identifiers.
//</summary>
[Xunit.Trait("Category", "Fast")]
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

 // PostgreSQL identifier escaping tests

    [Fact]
    public void Postgres_PlainIdentifier_WrapsInDoubleQuotes()
    {
        var provider = CreatePostgresProvider();
        Assert.Equal("\"Users\"", provider.Escape("Users"));
    }

    [Fact]
    public void Postgres_EmbeddedDoubleQuote_IsDoubled()
    {
        var provider = CreatePostgresProvider();
 // na"me should become "na""me"
        Assert.Equal("\"na\"\"me\"", provider.Escape("na\"me"));
    }

    [Fact]
    public void Postgres_MultipleEmbeddedDoubleQuotes_AllDoubled()
    {
        var provider = CreatePostgresProvider();
 // a"b"c should become "a""b""c"
        Assert.Equal("\"a\"\"b\"\"c\"", provider.Escape("a\"b\"c"));
    }

    [Fact]
    public void Postgres_SchemaQualified_EachPartEscaped()
    {
        var provider = CreatePostgresProvider();
 // dbo.Users should become "dbo"."Users"
        Assert.Equal("\"dbo\".\"Users\"", provider.Escape("dbo.Users"));
    }

    [Fact]
    public void Postgres_SchemaQualified_WithEmbeddedQuote_EachPartEscaped()
    {
        var provider = CreatePostgresProvider();
 // dbo.my"table should become "dbo"."my""table"
        Assert.Equal("\"dbo\".\"my\"\"table\"", provider.Escape("dbo.my\"table"));
    }

    [Fact]
    public void MappingMetadata_LiteralDottedTableAndColumnNames_AreEscapedAsSingleIdentifiers()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE "audit.events" (Id INTEGER PRIMARY KEY, "value.part" TEXT NOT NULL);
                INSERT INTO "audit.events" (Id, "value.part") VALUES (1, 'mapped');
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(connection, new SqliteProvider());

        var row = ctx.Query<DottedIdentifierEntity>().Single();

        Assert.Equal("mapped", row.Value);
    }

 //<summary>
 //Creates a PostgresProvider without needing a real Npgsql connection.
 //We use a SqliteParameterFactory as a stand-in since we only test Escape().
 //</summary>
    private static PostgresProvider CreatePostgresProvider()
        => new PostgresProvider(new SqliteParameterFactory());

    [Table("audit.events")]
    private sealed class DottedIdentifierEntity
    {
        [Key]
        public int Id { get; set; }

        [Column("value.part")]
        public string Value { get; set; } = string.Empty;
    }
}
