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
}
