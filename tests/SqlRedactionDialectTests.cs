using System;
using System.Reflection;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// S1 — Log redaction dialect gap (Gate 3.8→4.0 provisional, 4.5→5.0 full)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that <c>QueryExecutor.RedactSqlForLogging</c> strips sensitive literal
/// values from SQL for all supported provider dialects before they are written to logs.
///
/// S1 root cause: regex <c>'(?:[^']|'')*'</c> only covered ANSI single-quoted literals.
/// SQL Server's N'...' national string literals and PostgreSQL's $$...$$ dollar-quoted
/// blocks were not redacted, leaking sensitive values to log sinks.
///
/// Fix: extended to <c>N?'(?:[^']|'')*'</c> for single/N-prefixed strings, plus
/// <c>\$\$.*?\$\$</c> for dollar-quoted blocks.
/// </summary>
public class SqlRedactionDialectTests
{
    // ── Reflection accessor for private static RedactSqlForLogging ────────────

    private static readonly MethodInfo _redact = typeof(nORM.Query.QueryExecutor)
        .GetMethod("RedactSqlForLogging", BindingFlags.Static | BindingFlags.NonPublic)!;

    private static string Redact(string sql)
        => (string)_redact.Invoke(null, new object[] { sql })!;

    // ── ANSI single-quoted literals (baseline) ────────────────────────────────

    [Fact]
    public void Redact_AnsiSingleQuotedString_IsRedacted()
    {
        var result = Redact("SELECT * FROM T WHERE Name = 'alice'");
        Assert.Equal("SELECT * FROM T WHERE Name = '[redacted]'", result);
    }

    [Fact]
    public void Redact_MultipleAnsiLiterals_AllRedacted()
    {
        var result = Redact("INSERT INTO T (A, B) VALUES ('secret1', 'secret2')");
        Assert.Equal("INSERT INTO T (A, B) VALUES ('[redacted]', '[redacted]')", result);
    }

    [Fact]
    public void Redact_EscapedSingleQuoteInLiteral_HandledCorrectly()
    {
        // SQL escaped single quote: 'it''s a test'
        var result = Redact("WHERE Name = 'it''s a test'");
        Assert.Equal("WHERE Name = '[redacted]'", result);
    }

    [Fact]
    public void Redact_EmptyAnsiLiteral_IsRedacted()
    {
        var result = Redact("WHERE Name = ''");
        Assert.Equal("WHERE Name = '[redacted]'", result);
    }

    // ── SQL Server N'...' national string literals ────────────────────────────

    [Fact]
    public void Redact_NationalStringLiteral_IsRedacted()
    {
        var result = Redact("WHERE Name = N'alice'");
        Assert.Equal("WHERE Name = '[redacted]'", result);
    }

    [Fact]
    public void Redact_NationalStringLiteralUnicode_IsRedacted()
    {
        var result = Redact("WHERE Name = N'日本語テスト'");
        Assert.Equal("WHERE Name = '[redacted]'", result);
    }

    [Fact]
    public void Redact_NationalStringLiteralWithEscapedQuote_IsRedacted()
    {
        var result = Redact("WHERE Bio = N'she''s famous'");
        Assert.Equal("WHERE Bio = '[redacted]'", result);
    }

    [Fact]
    public void Redact_MultipleNationalStringLiterals_AllRedacted()
    {
        var result = Redact("INSERT INTO Users (Name, Bio) VALUES (N'Alice', N'secret bio')");
        Assert.Equal("INSERT INTO Users (Name, Bio) VALUES ('[redacted]', '[redacted]')", result);
    }

    [Fact]
    public void Redact_MixedAnsiAndNationalLiterals_AllRedacted()
    {
        // Mix of N'...' and '...' in the same statement
        var result = Redact("SELECT * FROM T WHERE A = N'unicode val' AND B = 'ansi val'");
        Assert.Equal("SELECT * FROM T WHERE A = '[redacted]' AND B = '[redacted]'", result);
    }

    // ── PostgreSQL $$...$$ dollar-quoted blocks ───────────────────────────────

    [Fact]
    public void Redact_DollarQuotedBlock_IsRedacted()
    {
        var result = Redact("INSERT INTO Procs (Body) VALUES ($$BEGIN RETURN 1; END$$)");
        Assert.Equal("INSERT INTO Procs (Body) VALUES ('[redacted]')", result);
    }

    [Fact]
    public void Redact_DollarQuotedBlockWithNewlines_IsRedacted()
    {
        var sql = "INSERT INTO Funcs (Body) VALUES ($$\nBEGIN\n  RETURN secret_value;\nEND\n$$)";
        var result = Redact(sql);
        Assert.Equal("INSERT INTO Funcs (Body) VALUES ('[redacted]')", result);
    }

    [Fact]
    public void Redact_EmptyDollarQuotedBlock_IsRedacted()
    {
        var result = Redact("SELECT $$$$");
        Assert.Equal("SELECT '[redacted]'", result);
    }

    [Fact]
    public void Redact_MultipleDollarQuotedBlocks_AllRedacted()
    {
        var result = Redact("SELECT $$secret1$$, $$secret2$$");
        Assert.Equal("SELECT '[redacted]', '[redacted]'", result);
    }

    // ── Mixed dialects ────────────────────────────────────────────────────────

    [Fact]
    public void Redact_AllThreeDialects_AllRedacted()
    {
        // A degenerate query mixing all three literal forms (realistically impossible,
        // but exercises all three redaction paths).
        var result = Redact("SELECT 'ansi', N'national', $$dollar$$");
        Assert.Equal("SELECT '[redacted]', '[redacted]', '[redacted]'", result);
    }

    // ── Identifiers and parameter placeholders are NOT redacted ───────────────

    [Fact]
    public void Redact_ParameterPlaceholder_NotRedacted()
    {
        var sql = "SELECT * FROM T WHERE Id = @id AND Name = @name";
        Assert.Equal(sql, Redact(sql));
    }

    [Fact]
    public void Redact_TableAndColumnNames_NotRedacted()
    {
        var sql = "SELECT [Users].[Id], [Users].[Name] FROM [Users]";
        Assert.Equal(sql, Redact(sql));
    }

    [Fact]
    public void Redact_SqlKeywords_NotRedacted()
    {
        var sql = "SELECT Id FROM Users WHERE IsActive = 1 ORDER BY Name";
        Assert.Equal(sql, Redact(sql));
    }

    [Fact]
    public void Redact_NullOrEmpty_ReturnsInput()
    {
        Assert.Equal("", Redact(""));
        Assert.Null(Redact(null!));
    }

    // ── Realistic full-query redaction ────────────────────────────────────────

    [Fact]
    public void Redact_RealisticInsertWithLiteral_CredentialsNotLeaked()
    {
        var sql = "INSERT INTO Users (Name, PasswordHash) VALUES ('admin', 'p@ssw0rd!')";
        var result = Redact(sql);
        Assert.DoesNotContain("admin", result);
        Assert.DoesNotContain("p@ssw0rd", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_SqlServerInsertWithNationalLiteral_CredentialsNotLeaked()
    {
        var sql = "INSERT INTO Users (Name) VALUES (N'密码12345')";
        var result = Redact(sql);
        Assert.DoesNotContain("密码", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_PostgresFunctionBody_SensitiveCodeNotLeaked()
    {
        var sql = "CREATE FUNCTION log_secret() RETURNS void AS $$\nRAISE NOTICE 'secret_key=abc123';\n$$ LANGUAGE plpgsql";
        var result = Redact(sql);
        Assert.DoesNotContain("secret_key", result);
        Assert.DoesNotContain("abc123", result);
    }

    [Fact]
    public void Redact_LiteralFollowedByParameter_BothHandledCorrectly()
    {
        // Literal value is redacted; parameter placeholder is preserved.
        var sql = "WHERE Status = 'active' AND UserId = @id";
        var result = Redact(sql);
        Assert.Equal("WHERE Status = '[redacted]' AND UserId = @id", result);
    }

    // ── Regression: parameters containing N or $ should not be falsely redacted ──

    [Fact]
    public void Redact_ParameterNameStartingWithN_NotRedacted()
    {
        var sql = "WHERE Name = @name AND Count = @num";
        Assert.Equal(sql, Redact(sql));
    }

    [Fact]
    public void Redact_DollarInIdentifier_NotRedacted()
    {
        // PostgreSQL table names with $ are unusual but possible; lone $ is not $$...$$.
        var sql = "SELECT col FROM \"schema$table\" WHERE id = @id";
        Assert.Equal(sql, Redact(sql));
    }

    // ── S1 fix: PostgreSQL tagged dollar-quoted literals ($tag$...$tag$) ──────

    [Fact]
    public void Redact_TaggedDollarQuotedLiteral_IsRedacted()
    {
        // $func$...$func$ is a common PostgreSQL form for function bodies.
        var result = Redact("CREATE FUNCTION f() RETURNS void AS $func$ BEGIN END $func$ LANGUAGE plpgsql");
        Assert.DoesNotContain("BEGIN END", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_TaggedDollarQuotedWithSensitiveValue_SensitiveValueNotLeaked()
    {
        var sql = "SELECT $secret$top_secret_password$secret$";
        var result = Redact(sql);
        Assert.DoesNotContain("top_secret_password", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_TaggedDollarQuotedMultiline_IsRedacted()
    {
        var sql = "INSERT INTO Logs (Body) VALUES ($body$\nline1\nline2\n$body$)";
        var result = Redact(sql);
        Assert.DoesNotContain("line1", result);
        Assert.DoesNotContain("line2", result);
        Assert.Equal("INSERT INTO Logs (Body) VALUES ('[redacted]')", result);
    }

    [Fact]
    public void Redact_MultipleTaggedDollarQuotedBlocks_AllRedacted()
    {
        var sql = "SELECT $a$secret_a$a$, $b$secret_b$b$";
        var result = Redact(sql);
        Assert.DoesNotContain("secret_a", result);
        Assert.DoesNotContain("secret_b", result);
        Assert.Equal("SELECT '[redacted]', '[redacted]'", result);
    }

    [Fact]
    public void Redact_MixedBareAndTaggedDollarQuoted_AllRedacted()
    {
        // Mix of bare $$ and tagged $tag$ forms.
        var sql = "SELECT $$bare_secret$$, $tagged$tagged_secret$tagged$";
        var result = Redact(sql);
        Assert.DoesNotContain("bare_secret", result);
        Assert.DoesNotContain("tagged_secret", result);
        Assert.Equal("SELECT '[redacted]', '[redacted]'", result);
    }

    [Fact]
    public void Redact_TaggedDollarQuotedAdversarial_DifferentTagsDoNotCross()
    {
        // $a$...$b$ should NOT be redacted — mismatched tags mean this is not a valid
        // dollar-quoted literal; the backreference \1 prevents tag-crossing.
        var sql = "SELECT $a$content$b$";
        var result = Redact(sql);
        // No redaction should occur for mismatched delimiters.
        Assert.Equal(sql, result);
    }

    [Fact]
    public void Redact_AllFourDialects_AllRedacted()
    {
        // ANSI, N'', bare $$, tagged $tag$ all in one statement.
        var result = Redact("SELECT 'ansi', N'national', $$bare$$, $t$tagged$t$");
        Assert.Equal("SELECT '[redacted]', '[redacted]', '[redacted]', '[redacted]'", result);
    }
}
