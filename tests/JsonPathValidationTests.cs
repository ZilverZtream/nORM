using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Regression tests for Q1: JSON path validation was over-restrictive,
/// rejecting valid RFC-7159 property names that contain hyphens (e.g., "order-items"),
/// wildcard selectors ($.*), and slash-separated JSON Pointer paths.
///
/// Root cause: the invalid-character set included '-', '/', and '*' which are
/// legitimate in well-formed JSON paths but pose no SQL-injection risk because
/// they cannot break out of the enclosing SQL string literal.
///
/// True injection chars (still rejected): ' " ; \
/// </summary>
public class JsonPathValidationTests : TestBase
{
    // ── Entity for translation tests ──────────────────────────────────────────

    private class JsonEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string? Data { get; set; }
    }

    private static (SqliteConnection cn, DbContext ctx) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE JsonEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Data TEXT);";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    // ── Q1-1: Hyphenated property names ───────────────────────────────────────

    [Theory]
    [InlineData("$.order-items")]
    [InlineData("$.order-items[0].id")]
    [InlineData("$.first-name")]
    [InlineData("$.a-b-c-d")]
    [InlineData("$.hyphen-in-key.nested-key")]
    public void HyphenatedJsonPath_DoesNotThrow(string path)
    {
        // Before the Q1 fix, '-' was in the invalid char set and all these paths threw.
        var (cn, ctx) = Build();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<JsonEntity>().Where(e => Json.Value<string>(e.Data!, path) == "x").ToString());
            Assert.Null(ex);
        }
    }

    // ── Q1-2: Wildcard paths ──────────────────────────────────────────────────

    [Theory]
    [InlineData("$.*")]
    [InlineData("$.items[*]")]
    [InlineData("$.a.*.b")]
    public void WildcardJsonPath_DoesNotThrow(string path)
    {
        var (cn, ctx) = Build();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<JsonEntity>().Where(e => Json.Value<string>(e.Data!, path) == "x").ToString());
            Assert.Null(ex);
        }
    }

    // ── Q1-3: Slash-separated paths ────────────────────────────────────────────

    [Theory]
    [InlineData("$.a/b")]
    [InlineData("$/items/0")]
    public void SlashJsonPath_DoesNotThrow(string path)
    {
        var (cn, ctx) = Build();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<JsonEntity>().Where(e => Json.Value<string>(e.Data!, path) == "x").ToString());
            Assert.Null(ex);
        }
    }

    // ── Q1-4: Standard valid paths still work ─────────────────────────────────

    [Theory]
    [InlineData("$.name")]
    [InlineData("$.address.city")]
    [InlineData("$[0].id")]
    [InlineData("$.items[2].price")]
    [InlineData("$._private")]
    public void StandardJsonPath_DoesNotThrow(string path)
    {
        var (cn, ctx) = Build();
        using (cn) using (ctx)
        {
            var ex = Record.Exception(() =>
                ctx.Query<JsonEntity>().Where(e => Json.Value<string>(e.Data!, path) == "x").ToString());
            Assert.Null(ex);
        }
    }

    // ── Q1-5: SQL-injection chars are still rejected ──────────────────────────

    [Theory]
    [InlineData("$.a'b")]          // single-quote — can break string literal
    [InlineData("$.a\"b")]         // double-quote — identifier escape
    [InlineData("$.a;DROP TABLE")] // semicolon — statement terminator
    [InlineData("$.a\\b")]         // backslash — escape sequence
    public void InjectionCharsInJsonPath_Throws(string path)
    {
        var (cn, ctx) = Build();
        using (cn) using (ctx)
        {
            Assert.Throws<NormQueryException>(() =>
                ctx.Query<JsonEntity>().Where(e => Json.Value<string>(e.Data!, path) == "x").ToString());
        }
    }

    // ── Q1-6: Null/empty still rejected ───────────────────────────────────────

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void NullOrWhitespaceJsonPath_Throws(string path)
    {
        var (cn, ctx) = Build();
        using (cn) using (ctx)
        {
            Assert.Throws<NormQueryException>(() =>
                ctx.Query<JsonEntity>().Where(e => Json.Value<string>(e.Data!, path) == "x").ToString());
        }
    }

    // ── Q1-7: Multi-provider SQL shape for hyphenated path ────────────────────

    [Theory]
    [InlineData(ProviderKind.Sqlite, "json_extract")]
    [InlineData(ProviderKind.SqlServer, "JSON_VALUE")]
    [InlineData(ProviderKind.MySql, "JSON_EXTRACT")]
    [InlineData(ProviderKind.Postgres, "jsonb_extract_path_text")]
    public void HyphenatedPath_TranslatesCorrectly_AllProviders(ProviderKind kind, string expectedFn)
    {
        var (cn, provider) = CreateProvider(kind);
        using (cn)
        {
            var (sql, _) = Translate<JsonEntity>(
                e => Json.Value<string>(e.Data!, "$.order-items[0].id") == "x",
                cn, provider);
            Assert.Contains(expectedFn, sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("order-items", sql, StringComparison.Ordinal);
        }
    }

    // ── Q2: PostgreSQL root-only path "$" produces valid SQL ─────────────────

    [Fact]
    public void PostgresProvider_RootOnlyPath_ProducesValidSql()
    {
        // Q2 fix: TranslateJsonPathAccess(col, "$") previously emitted
        // jsonb_extract_path_text(col, ) — missing required path argument.
        // Bug 21 fix: root-only path uses col #>> '{}' which strips JSON quotes,
        // unlike (col)::text which preserves them for JSONB string values.
        var factory = new SqliteParameterFactory();
        var pg = new PostgresProvider(factory);
        var result = pg.TranslateJsonPathAccess("\"Data\"", "$");
        Assert.DoesNotContain("jsonb_extract_path_text", result);
        Assert.Contains("#>> '{}'", result);
        // Must be syntactically valid — no trailing comma or empty args
        Assert.DoesNotContain(", )", result);
    }

    [Fact]
    public void PostgresProvider_DollarDotPath_StillUsesJsonbExtract()
    {
        // Non-root paths still use jsonb_extract_path_text
        var factory = new SqliteParameterFactory();
        var pg = new PostgresProvider(factory);
        var result = pg.TranslateJsonPathAccess("\"Data\"", "$.name");
        Assert.Contains("jsonb_extract_path_text", result);
        Assert.Contains("'name'", result);
    }

    [Fact]
    public void PostgresProvider_EmptyPath_ProducesValidSql()
    {
        // Empty string path should also fall through to cast
        var factory = new SqliteParameterFactory();
        var pg = new PostgresProvider(factory);
        var result = pg.TranslateJsonPathAccess("\"Data\"", "");
        Assert.DoesNotContain(", )", result);
    }
}

/// <summary>
/// Tests that all four providers' <c>TranslateJsonPathAccess</c> public API rejects
/// SQL-injectable characters in the JSON path argument.
///
/// Root cause: the public API accepted raw path strings without validation, allowing
/// single-quotes, semicolons, backslashes, and control characters to be embedded
/// directly into the generated SQL, enabling SQL injection.
/// </summary>
public class ProviderJsonPathInjectionTests
{
    // ── SQLite ────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("$.name'")]
    [InlineData("$.' OR '1'='1")]
    [InlineData("$.id; DROP TABLE t--")]
    [InlineData("$.foo\\bar")]
    [InlineData("$.\"; DELETE")]
    public void SQLite_InjectionPath_ThrowsArgumentException(string path)
    {
        var provider = new SqliteProvider();
        Assert.Throws<ArgumentException>(() => provider.TranslateJsonPathAccess("col", path));
    }

    [Theory]
    [InlineData("$.name")]
    [InlineData("$.order.items[0].id")]
    [InlineData("$.*")]
    [InlineData("$.foo/bar")]
    public void SQLite_ValidPath_DoesNotThrow(string path)
    {
        var provider = new SqliteProvider();
        var sql = provider.TranslateJsonPathAccess("col", path);
        Assert.NotEmpty(sql);
    }

    // ── SQL Server ────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("$.name'")]
    [InlineData("$.' OR '1'='1")]
    [InlineData("$.id; DROP TABLE t--")]
    [InlineData("$.foo\\bar")]
    [InlineData("$.\"; DELETE")]
    public void SqlServer_InjectionPath_ThrowsArgumentException(string path)
    {
        var provider = new SqlServerProvider();
        Assert.Throws<ArgumentException>(() => provider.TranslateJsonPathAccess("col", path));
    }

    [Theory]
    [InlineData("$.name")]
    [InlineData("$.order.items[0].id")]
    [InlineData("$.*")]
    public void SqlServer_ValidPath_DoesNotThrow(string path)
    {
        var provider = new SqlServerProvider();
        var sql = provider.TranslateJsonPathAccess("col", path);
        Assert.Contains("JSON_VALUE", sql);
    }

    // ── MySQL ─────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("$.name'")]
    [InlineData("$.id; DROP TABLE t--")]
    [InlineData("$.foo\\bar")]
    public void MySQL_InjectionPath_ThrowsArgumentException(string path)
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        Assert.Throws<ArgumentException>(() => provider.TranslateJsonPathAccess("col", path));
    }

    [Theory]
    [InlineData("$.name")]
    [InlineData("$.a.b.c")]
    public void MySQL_ValidPath_DoesNotThrow(string path)
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var sql = provider.TranslateJsonPathAccess("col", path);
        Assert.Contains("JSON_EXTRACT", sql);
    }

    // ── PostgreSQL ────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("$.name'")]
    [InlineData("$.id; DROP TABLE t--")]
    [InlineData("$.foo\\bar")]
    public void Postgres_InjectionPath_ThrowsArgumentException(string path)
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        Assert.Throws<ArgumentException>(() => provider.TranslateJsonPathAccess("col", path));
    }

    [Theory]
    [InlineData("$.name")]
    [InlineData("$.a.b.c")]
    [InlineData("$")]
    public void Postgres_ValidPath_DoesNotThrow(string path)
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sql = provider.TranslateJsonPathAccess("col", path);
        Assert.NotEmpty(sql);
    }

    // ── Length limit: all providers ───────────────────────────────────────────

    [Fact]
    public void AllProviders_PathExceedsMaxLength_ThrowsArgumentException()
    {
        var longPath = "$." + new string('a', 500);
        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };
        foreach (var p in providers)
            Assert.Throws<ArgumentException>(() => p.TranslateJsonPathAccess("col", longPath));
    }

    // ── Null guards: all providers ────────────────────────────────────────────

    [Fact]
    public void AllProviders_NullPath_ThrowsArgumentNullException()
    {
        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };
        foreach (var p in providers)
            Assert.Throws<ArgumentNullException>(() => p.TranslateJsonPathAccess("col", null!));
    }

    [Fact]
    public void AllProviders_NullColumn_ThrowsArgumentNullException()
    {
        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };
        foreach (var p in providers)
            Assert.Throws<ArgumentNullException>(() => p.TranslateJsonPathAccess(null!, "$.x"));
    }

    // ── Adversarial: control characters ──────────────────────────────────────

    [Fact]
    public void AllProviders_ControlCharInPath_Rejected()
    {
        var path = "$.a\x00b"; // null byte embedded in path
        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new MySqlProvider(new SqliteParameterFactory()),
            new PostgresProvider(new SqliteParameterFactory()),
        };
        foreach (var p in providers)
            Assert.Throws<ArgumentException>(() => p.TranslateJsonPathAccess("col", path));
    }
}
