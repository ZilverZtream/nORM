using System;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.8 → 4.0 — Multipart identifier escaping per provider
//
// P1 audit finding: SQLite and MySQL wrapped the entire identifier in one set
// of delimiters ("schema.table" → `schema.table`), producing invalid SQL for
// schema-qualified names. SQL Server and PostgreSQL already split on dot.
//
// Fix: all four providers now split on '.' and escape each segment independently.
// ══════════════════════════════════════════════════════════════════════════════

public class MultipartIdentifierEscapingTests
{
    // ── Simple single-part identifiers (regression: must not regress) ─────────

    [Theory]
    [InlineData("sqlite",    "col",         "\"col\"")]
    [InlineData("sqlserver", "col",         "[col]")]
    [InlineData("mysql",     "col",         "`col`")]
    [InlineData("postgres",  "col",         "\"col\"")]
    public void Escape_SimpleIdentifier_CorrectDelimiter(
        string provider, string id, string expected)
    {
        Assert.Equal(expected, MakeProvider(provider).Escape(id));
    }

    // ── Two-part schema.table identifiers ────────────────────────────────────

    [Theory]
    [InlineData("sqlite",    "dbo.Users",     "\"dbo\".\"Users\"")]
    [InlineData("sqlserver", "dbo.Users",     "[dbo].[Users]")]
    [InlineData("mysql",     "mydb.orders",   "`mydb`.`orders`")]
    [InlineData("postgres",  "public.orders", "\"public\".\"orders\"")]
    public void Escape_SchemaTable_EachPartEscapedSeparately(
        string provider, string id, string expected)
    {
        Assert.Equal(expected, MakeProvider(provider).Escape(id));
    }

    // ── Three-part identifiers (e.g. catalog.schema.table) ───────────────────

    [Theory]
    [InlineData("sqlite",    "db.dbo.T",  "\"db\".\"dbo\".\"T\"")]
    [InlineData("sqlserver", "db.dbo.T",  "[db].[dbo].[T]")]
    [InlineData("mysql",     "db.sch.T",  "`db`.`sch`.`T`")]
    [InlineData("postgres",  "db.pub.T",  "\"db\".\"pub\".\"T\"")]
    public void Escape_ThreePart_EachSegmentEscapedSeparately(
        string provider, string id, string expected)
    {
        Assert.Equal(expected, MakeProvider(provider).Escape(id));
    }

    // ── The old (broken) behaviour must NOT appear ────────────────────────────

    [Theory]
    [InlineData("sqlite",   "schema.table")]
    [InlineData("mysql",    "schema.table")]
    public void Escape_SchemaTable_DoesNotWrapWholeName(string provider, string id)
    {
        // Before the fix, SQLite produced "schema.table" (dot inside quotes) and
        // MySQL produced `schema.table` (dot inside backticks) — both invalid.
        var result = MakeProvider(provider).Escape(id);
        // The dot must be OUTSIDE the delimiters in the result.
        Assert.Contains(".", result);
        // Verify the dot is between two closing+opening delimiter pairs.
        var dot = result.IndexOf('.');
        Assert.True(dot > 0 && dot < result.Length - 1,
            $"Dot must be in the middle of the escaped string, got: {result}");
        // The character before the dot must be a closing delimiter.
        char before = result[dot - 1];
        Assert.True(before == '"' || before == '`' || before == ']',
            $"Character before dot must be a closing delimiter, got '{before}' in: {result}");
    }

    // ── Injection safety: embedded delimiter characters are escaped ───────────

    [Theory]
    [InlineData("sqlite",    "bad\"name",    "\"bad\"\"name\"")]
    [InlineData("sqlserver", "bad]name",     "[bad]]name]")]
    [InlineData("mysql",     "bad`name",     "`bad``name`")]
    [InlineData("postgres",  "bad\"name",    "\"bad\"\"name\"")]
    public void Escape_EmbeddedDelimiter_IsDoubled(
        string provider, string id, string expected)
    {
        Assert.Equal(expected, MakeProvider(provider).Escape(id));
    }

    // ── Injection safety: embedded delimiter in multipart identifier ──────────

    [Theory]
    [InlineData("sqlite",    "sch.bad\"col", "\"sch\".\"bad\"\"col\"")]
    [InlineData("sqlserver", "sch.bad]col",  "[sch].[bad]]col]")]
    [InlineData("mysql",     "sch.bad`col",  "`sch`.`bad``col`")]
    [InlineData("postgres",  "sch.bad\"col", "\"sch\".\"bad\"\"col\"")]
    public void Escape_Multipart_EmbeddedDelimiterInSecondSegment_IsDoubled(
        string provider, string id, string expected)
    {
        Assert.Equal(expected, MakeProvider(provider).Escape(id));
    }

    // ── Empty / whitespace identifiers are returned as-is (null-safe guard) ──

    [Theory]
    [InlineData("sqlite",    "")]
    [InlineData("sqlserver", "")]
    [InlineData("mysql",     "")]
    [InlineData("postgres",  "")]
    public void Escape_EmptyString_ReturnsEmpty(string provider, string id)
    {
        var result = MakeProvider(provider).Escape(id);
        // Either empty or whitespace is returned unchanged per provider null-guard.
        Assert.True(string.IsNullOrWhiteSpace(result) || result == id,
            $"Expected empty/whitespace pass-through, got: {result}");
    }

    // ── Consistent: dot outside identifier = SQL separator, not escaped ───────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public void Escape_Multipart_DotAppearsOnceUnescaped(string provider)
    {
        var result = MakeProvider(provider).Escape("schema.table");
        // Exactly one dot must appear in the output (the separator between segments).
        var dotCount = 0;
        foreach (var c in result) if (c == '.') dotCount++;
        Assert.Equal(1, dotCount);
    }

    // ── Cross-provider: SQL Server and Postgres already handled dots ──────────
    // Verify they still produce the correct output (no regression from prior fix).

    [Fact]
    public void SqlServer_Multipart_AlreadyCorrect_NoRegression()
    {
        Assert.Equal("[dbo].[MyTable]", new SqlServerProvider().Escape("dbo.MyTable"));
    }

    [Fact]
    public void Postgres_Multipart_AlreadyCorrect_NoRegression()
    {
        Assert.Equal("\"public\".\"my_table\"", new PostgresProvider(new SqliteParameterFactory()).Escape("public.my_table"));
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    private static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"    => new SqliteProvider(),
        "sqlserver" => new SqlServerProvider(),
        "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
        "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };
}
