using System;
using System.Reflection;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Regression tests for S1/Q1: HasTopLevelOrderBy in SqlServerProvider now skips
/// string literals, double-quoted identifiers, bracket-quoted identifiers, AND
/// SQL comments (-- line comments and /* block comments */) so that an ORDER BY
/// appearing inside a comment is not mistaken for a real ORDER BY clause.
///
/// Q1 fix: add comment-token lexing to HasTopLevelOrderBy.
/// S1 fix: add literal/identifier lexing (prior fix).
/// </summary>
public class SqlServerOrderByDetectionTests
{
    // Invoke the private static HasTopLevelOrderBy via reflection.
    private static bool HasTopLevelOrderBy(string sql)
    {
        var method = typeof(SqlServerProvider)
            .GetMethod("HasTopLevelOrderBy",
                BindingFlags.NonPublic | BindingFlags.Static)!;
        return (bool)method.Invoke(null, new object[] { sql })!;
    }

    // ── S1-1: Genuine top-level ORDER BY is detected ──────────────────────────

    [Theory]
    [InlineData("SELECT * FROM T ORDER BY Id")]
    [InlineData("SELECT * FROM T ORDER BY Id DESC")]
    [InlineData("select * from t order by id")]         // lowercase
    [InlineData("SELECT * FROM T\r\nORDER BY Id")]
    [InlineData("SELECT a,b FROM T ORDER BY a, b")]
    public void RealOrderBy_IsDetected(string sql)
        => Assert.True(HasTopLevelOrderBy(sql));

    // ── S1-2: ORDER BY inside a string literal is NOT detected ───────────────

    [Theory]
    [InlineData("SELECT * FROM T WHERE name = 'sort ORDER BY price'")]
    [InlineData("SELECT * FROM T WHERE note = 'no ORDER BY here'")]
    [InlineData("SELECT 'ORDER BY fake' FROM T")]
    [InlineData("SELECT * FROM T WHERE x = 'abc''s ORDER BY trick'")]  // '' escape inside
    public void OrderByInsideStringLiteral_IsNotDetected(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── S1-3: ORDER BY inside subquery is NOT detected ────────────────────────

    [Theory]
    [InlineData("SELECT * FROM (SELECT * FROM T ORDER BY Id) sub")]
    [InlineData("SELECT Id, (SELECT TOP 1 Name FROM R ORDER BY Name) AS N FROM T")]
    [InlineData("SELECT * FROM T WHERE Id IN (SELECT Id FROM S ORDER BY Id)")]
    public void OrderByInsideSubquery_IsNotDetected(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── S1-4: ORDER BY inside double-quoted identifier NOT detected ───────────

    [Theory]
    [InlineData("SELECT \"ORDER BY col\" FROM T")]
    [InlineData("SELECT * FROM T WHERE \"x ORDER BY\" = 1")]
    public void OrderByInsideDoubleQuote_IsNotDetected(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── S1-5: ORDER BY inside bracket-quoted identifier NOT detected ──────────

    [Theory]
    [InlineData("SELECT [ORDER BY col] FROM T")]
    [InlineData("SELECT * FROM [ORDER BY TABLE]")]
    public void OrderByInsideBrackets_IsNotDetected(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── S1-6: Absent ORDER BY ─────────────────────────────────────────────────

    [Theory]
    [InlineData("SELECT * FROM T")]
    [InlineData("SELECT * FROM T WHERE Id = 1")]
    [InlineData("")]
    public void NoOrderBy_ReturnsFalse(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── S1-7: Combination — string literal followed by real ORDER BY ──────────

    [Fact]
    public void StringLiteralThenRealOrderBy_IsDetected()
    {
        // The literal should be skipped; the trailing real ORDER BY must be found.
        var sql = "SELECT * FROM T WHERE name = 'ORDER BY inside' ORDER BY Id";
        Assert.True(HasTopLevelOrderBy(sql));
    }

    // ── S1-8: ApplyPaging emits synthetic ORDER BY when none present ──────────

    [Fact]
    public void ApplyPaging_EmitsSyntheticOrderBy_WhenLiteralContainsOrderBy()
    {
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T WHERE label = 'sort ORDER BY column'");

        provider.ApplyPaging(sb, limit: 10, offset: 0,
            limitParameterName: "@lim", offsetParameterName: "@off");

        var result = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH", result, StringComparison.OrdinalIgnoreCase);
    }

    // ── Q1-1: ORDER BY inside line comment (--) is NOT detected ──────────────

    [Theory]
    [InlineData("SELECT * FROM T -- ORDER BY Id")]
    [InlineData("SELECT * FROM T -- this order by should be ignored\nWHERE Id = 1")]
    [InlineData("-- ORDER BY Id\nSELECT * FROM T")]
    [InlineData("SELECT * FROM T -- ORDER BY Id\r\nWHERE x = 1")]
    public void OrderByInsideLineComment_IsNotDetected(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── Q1-2: ORDER BY inside block comment (/* */) is NOT detected ──────────

    [Theory]
    [InlineData("SELECT * FROM T /* ORDER BY Id */")]
    [InlineData("/* ORDER BY fake */ SELECT * FROM T")]
    [InlineData("SELECT * FROM T /* multi\nline\nORDER BY x */ WHERE Id = 1")]
    [InlineData("SELECT /* ORDER BY col */ Id FROM T")]
    public void OrderByInsideBlockComment_IsNotDetected(string sql)
        => Assert.False(HasTopLevelOrderBy(sql));

    // ── Q1-3: Real ORDER BY after a line comment IS detected ─────────────────

    [Fact]
    public void LineCommentThenRealOrderBy_IsDetected()
    {
        // The line comment should be skipped; the trailing real ORDER BY must be found.
        var sql = "SELECT * FROM T -- no order here\nORDER BY Id";
        Assert.True(HasTopLevelOrderBy(sql));
    }

    // ── Q1-4: Real ORDER BY after a block comment IS detected ────────────────

    [Fact]
    public void BlockCommentThenRealOrderBy_IsDetected()
    {
        // The block comment should be skipped; the trailing real ORDER BY must be found.
        var sql = "SELECT * FROM T /* ORDER BY Id in comment */ ORDER BY Name";
        Assert.True(HasTopLevelOrderBy(sql));
    }

    // ── Q1-5: Nested mix — line comment + literal, then real ORDER BY ─────────

    [Fact]
    public void MixedCommentAndLiteralThenRealOrderBy_IsDetected()
    {
        var sql = "SELECT * FROM T -- comment ORDER BY fake\nWHERE x = 'also ORDER BY in literal'\nORDER BY Id";
        Assert.True(HasTopLevelOrderBy(sql));
    }

    // ── Q1-6: ApplyPaging emits synthetic ORDER BY for line-comment input ─────

    [Fact]
    public void ApplyPaging_WithLineCommentContainingOrderBy_EmitsSyntheticOrderBy()
    {
        // Q1 scenario: line comment contains ORDER BY — must still emit synthetic ORDER BY (SELECT NULL).
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T -- ORDER BY Id here\nWHERE x = 1");

        provider.ApplyPaging(sb, limit: 5, offset: 0,
            limitParameterName: "@lim", offsetParameterName: "@off");

        var result = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH", result, StringComparison.OrdinalIgnoreCase);
    }

    // ── Q1-7: ApplyPaging emits synthetic ORDER BY for block-comment input ────

    [Fact]
    public void ApplyPaging_WithBlockCommentContainingOrderBy_EmitsSyntheticOrderBy()
    {
        // Q1 scenario: block comment contains ORDER BY — must still emit synthetic ORDER BY (SELECT NULL).
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T /* ORDER BY Id */ WHERE x = 1");

        provider.ApplyPaging(sb, limit: 5, offset: 0,
            limitParameterName: "@lim", offsetParameterName: "@off");

        var result = sb.ToString();
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH", result, StringComparison.OrdinalIgnoreCase);
    }

    // ── Q1-8: Unclosed block comment handled gracefully ───────────────────────

    [Fact]
    public void UnclosedBlockComment_DoesNotThrow_ReturnsFalse()
    {
        // Malformed SQL: unclosed /* — scanner must reach EOF without exception.
        var sql = "SELECT * FROM T /* unclosed comment ORDER BY Id";
        var ex = Record.Exception(() => HasTopLevelOrderBy(sql));
        Assert.Null(ex);
        Assert.False(HasTopLevelOrderBy(sql)); // ORDER BY inside unclosed comment
    }

    // ── Q1-9: Escaped ]] inside bracket-quoted identifiers ──────────────────

    [Fact]
    public void EscapedBracketIdentifier_OrderByInsideIdentifier_ReturnsFalse()
    {
        // Q1 fix: [a]]ORDER BYb] is a single identifier containing literal ] + "ORDER BYb"
        // The old parser stopped at the first ], exposing "ORDER BYb]" to the scanner.
        var sql = "SELECT [a]]ORDER BYb] FROM T";
        Assert.False(HasTopLevelOrderBy(sql));
    }

    [Fact]
    public void EscapedBracketIdentifier_OrderByAfterIdentifier_ReturnsTrue()
    {
        // Real ORDER BY after an identifier with ]]
        var sql = "SELECT [col]] ] FROM T ORDER BY Id";
        Assert.True(HasTopLevelOrderBy(sql));
    }

    [Fact]
    public void EscapedBracketIdentifier_MultipleEscapedBrackets_ReturnsFalse()
    {
        // Identifier with multiple ]] sequences: [a]]b]]c]
        var sql = "SELECT [a]]b]]c] FROM T";
        Assert.False(HasTopLevelOrderBy(sql));
    }

    [Fact]
    public void EscapedBracketIdentifier_EmptyBrackets_NoOrderBy_ReturnsFalse()
    {
        // Empty bracket identifier [] followed by no ORDER BY
        var sql = "SELECT [] FROM T";
        Assert.False(HasTopLevelOrderBy(sql));
    }

    [Fact]
    public void EscapedBracketIdentifier_TrailingOrderByText_ReturnsFalse()
    {
        // The identifier [x]]ORDER BY] contains ]] + "ORDER BY" as part of the name
        var sql = "SELECT [x]]ORDER BY] FROM T";
        Assert.False(HasTopLevelOrderBy(sql));
    }
}
