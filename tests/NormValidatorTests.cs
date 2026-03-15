using System.Collections.Generic;
using System.Reflection;
using nORM.Core;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for NormValidator — parameter marker counting and raw SQL injection detection.
/// </summary>
public class NormValidatorTests
{
    // CountParameterMarkers is internal; use reflection to reach it.
    private static int CountMarkers(string sql)
    {
        var method = typeof(NormValidator).GetMethod(
            "CountParameterMarkers",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (int)method.Invoke(null, new object[] { sql })!;
    }

    private static void AssertAccepted(string sql) =>
        NormValidator.ValidateRawSql(sql, null);

    private static void AssertRejected(string sql) =>
        Assert.Throws<NormUsageException>(() => NormValidator.ValidateRawSql(sql, null));

    // ── Parameter marker counting ──────────────────────────────────────────

    [Fact]
    public void DoubleColon_TypeCast_IsNotCountedAsParameterMarker()
    {
        Assert.Equal(0, CountMarkers("SELECT '2026-01-01'::date"));
        Assert.Equal(0, CountMarkers("SELECT col::int FROM t"));
        Assert.Equal(0, CountMarkers("SELECT col::varchar(255) FROM t"));
        Assert.Equal(0, CountMarkers("SELECT col::text, col2::int FROM t"));
    }

    [Fact]
    public void PositionalParam_WithTypeCast_CountsAsOneMarker()
    {
        Assert.Equal(1, CountMarkers("SELECT $1::text"));
        Assert.Equal(1, CountMarkers("WHERE id = $1::int"));
    }

    [Fact]
    public void MultiplePositionalParams_WithTypeCasts_CountsAll()
    {
        Assert.Equal(2, CountMarkers("WHERE a = $1::text AND b = $2::int"));
        Assert.Equal(3, CountMarkers("SELECT $1::int, $2::text, $3::date FROM t"));
    }

    [Fact]
    public void NamedColonParam_IsCountedAsMarker()
    {
        Assert.Equal(1, CountMarkers("WHERE id = :id"));
        Assert.Equal(2, CountMarkers("WHERE a = :first AND b = :second"));
    }

    [Fact]
    public void DoubleColonInsideStringLiteral_IsNotCountedAsMarker()
    {
        Assert.Equal(0, CountMarkers("SELECT '::text' FROM t"));
        Assert.Equal(0, CountMarkers("WHERE note = 'cast::expression'"));
    }

    [Fact]
    public void DoubleColonAtStartOfString_IsNotCounted()
    {
        Assert.Equal(0, CountMarkers("::text"));
        Assert.Equal(0, CountMarkers("::int"));
    }

    // ── Semicolon detection ────────────────────────────────────────────────

    [Fact]
    public void SemicolonInsideStringLiteral_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE note = 'Hello; world'");
        AssertAccepted("SELECT * FROM t WHERE a = 'first; part' AND b = 'second; part'");
    }

    [Fact]
    public void SemicolonAndDdlInsideStringLiteral_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE a = 'foo; bar' AND b = '; DROP TABLE users'");
        AssertAccepted("SELECT * FROM t WHERE log_entry = 'Query: SELECT x; DROP TABLE t'");
    }

    [Fact]
    public void SemicolonInsideLineComment_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = 1 -- semicolon here; ignored");
        AssertAccepted("SELECT * FROM t -- first; second;\nWHERE id = 1");
    }

    [Fact]
    public void SemicolonInsideBlockComment_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t /* this has; a semicolon */ WHERE id = 1");
        AssertAccepted("SELECT * FROM t /* first; second; */ WHERE id = 1");
    }

    [Fact]
    public void TrailingSemicolon_SingleStatement_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = 1;");
    }

    [Fact]
    public void MultiStatement_WithDdl_IsRejected()
    {
        AssertRejected("SELECT * FROM t; SELECT 1; DROP TABLE t");
        AssertRejected("SELECT * FROM t; SELECT 1; ALTER TABLE t ADD COLUMN x INT");
        AssertRejected("SELECT * FROM t; SELECT 1; EXEC sp_dangerous");
    }

    // ── Adversarial SQL patterns ───────────────────────────────────────────

    [Fact]
    public void AtSignParam_SqlServer_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = @id");
        AssertAccepted("SELECT * FROM t WHERE a = @p1 AND b = @p2");
    }

    [Fact]
    public void PositionalParam_WithCast_PostgresDialect_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = $1");
        AssertAccepted("SELECT * FROM t WHERE name = $1::text AND age = $2::int");
    }

    [Fact]
    public void DoubleQuotedIdentifiers_AreLexedCorrectly()
    {
        AssertAccepted("SELECT \"col@name\" FROM t WHERE id = 1");
        AssertAccepted("SELECT \"col;name\" FROM t WHERE id = 1");
    }

    [Fact]
    public void TypeCastWithCommentContainingSemicolon_IsAccepted()
    {
        AssertAccepted("SELECT col::int FROM t WHERE id = 1 -- note: end; of query");
    }

    [Fact]
    public void MultiStatement_DropTable_IsRejected()
    {
        AssertRejected("SELECT * FROM t WHERE id = 1; DROP TABLE users");
    }

    [Fact]
    public void TypeCastWithDdlLookingName_IsAccepted()
    {
        AssertAccepted("SELECT col::text FROM t WHERE id = 1");
    }
}
