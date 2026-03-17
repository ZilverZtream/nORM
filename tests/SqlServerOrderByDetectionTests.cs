using System;
using System.Reflection;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Regression tests for S1: HasTopLevelOrderBy in SqlServerProvider did not skip
/// string literals, so ORDER BY inside a quoted literal was incorrectly treated as
/// a real top-level ORDER BY clause, causing ApplyPaging to omit the synthetic
/// ORDER BY (SELECT NULL) and produce malformed OFFSET…FETCH SQL.
///
/// Fix: the scanner now skips single-quoted literals ('...' with '' escape),
/// double-quoted identifiers, and bracket-quoted identifiers ([...]).
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
        // Build the kind of SQL string that would be generated for a query like
        // SELECT * FROM T WHERE label = 'price ORDER BY desc' OFFSET 0 ROWS FETCH ...
        // Before the fix, HasTopLevelOrderBy returned true for this input and the
        // synthetic ORDER BY (SELECT NULL) was omitted — producing invalid TSQL.
        var provider = new SqlServerProvider();
        var sb = new OptimizedSqlBuilder();
        sb.Append("SELECT * FROM T WHERE label = 'sort ORDER BY column'");

        provider.ApplyPaging(sb, limit: 10, offset: 0,
            limitParameterName: "@lim", offsetParameterName: "@off");

        var result = sb.ToString();
        // Must contain ORDER BY (SELECT NULL) because the real top-level query has none
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH", result, StringComparison.OrdinalIgnoreCase);
    }
}
