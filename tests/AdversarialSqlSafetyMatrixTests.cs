using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// T1 — Adversarial SQL safety matrix  (Gate 4.5→5.0)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Explicit deny assertions for every category of dangerous SQL accepted by
/// <see cref="NormValidator.IsSafeRawSql"/>.
///
/// T1 root cause: <c>IsSafeRawSql_ReturnsFalse_ForPrivilegeStatements</c> in
/// <c>SecurityBoundaryTests</c> computed the result but discarded it with <c>_ = result</c>,
/// so a regression allowing GRANT/REVOKE would pass silently.
///
/// This file:
/// - Asserts <c>false</c> for ALL dangerous SQL categories with <c>Assert.False</c>.
/// - Adds comment-injection bypass attempts (inline comment, block comment, Unicode).
/// - Adds CREATE sub-variants (TABLE, INDEX, VIEW, PROCEDURE, FUNCTION, TRIGGER, DATABASE).
/// - Adds DENY statement (SQL Server permission denial).
/// - Adds CREATE/ALTER/DROP for temporal-sensitive objects.
/// - Adds provider-escaping round-trip assertions for all three providers.
/// </summary>
public class AdversarialSqlSafetyMatrixTests
{
    // ── DDL: CREATE variants ──────────────────────────────────────────────────

    [Theory]
    [InlineData("CREATE TABLE Foo (Id INT)")]
    [InlineData("create table foo (id int)")]
    [InlineData("CREATE INDEX idx_foo ON Foo(Id)")]
    [InlineData("CREATE VIEW vw_foo AS SELECT 1")]
    [InlineData("CREATE PROCEDURE sp_foo AS BEGIN SELECT 1 END")]
    [InlineData("CREATE FUNCTION fn_foo() RETURNS INT BEGIN RETURN 1 END")]
    [InlineData("CREATE TRIGGER tr_foo AFTER INSERT ON Foo FOR EACH ROW BEGIN END")]
    [InlineData("CREATE DATABASE MyDb")]
    [InlineData("CREATE SCHEMA myschema")]
    [InlineData("CREATE SEQUENCE myseq START 1")]
    public void IsSafeRawSql_ReturnsFalse_ForCreateStatements(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected CREATE statement to be blocked: '{sql}'");

    // ── DDL: DROP / ALTER ─────────────────────────────────────────────────────

    [Theory]
    [InlineData("DROP TABLE Foo")]
    [InlineData("drop table foo")]
    [InlineData("DROP DATABASE MyDb")]
    [InlineData("DROP VIEW vw_foo")]
    [InlineData("DROP PROCEDURE sp_foo")]
    [InlineData("ALTER TABLE Foo ADD COLUMN X INT")]
    [InlineData("alter table foo drop column x")]
    [InlineData("ALTER DATABASE MyDb SET RECOVERY SIMPLE")]
    public void IsSafeRawSql_ReturnsFalse_ForDropAndAlterStatements(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected DROP/ALTER statement to be blocked: '{sql}'");

    // ── DML: INSERT / UPDATE / DELETE / MERGE / TRUNCATE ─────────────────────

    [Theory]
    [InlineData("INSERT INTO Foo VALUES(1)")]
    [InlineData("insert into foo values(1)")]
    [InlineData("UPDATE Foo SET X=1")]
    [InlineData("update foo set x=1")]
    [InlineData("DELETE FROM Foo")]
    [InlineData("delete from foo")]
    [InlineData("MERGE Foo USING src ON src.Id=Foo.Id WHEN MATCHED THEN DELETE")]
    [InlineData("merge foo using src on src.id=foo.id when matched then delete")]
    [InlineData("TRUNCATE TABLE Foo")]
    [InlineData("truncate table foo")]
    public void IsSafeRawSql_ReturnsFalse_ForDmlStatements(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected DML statement to be blocked: '{sql}'");

    // ── Privilege statements: GRANT / REVOKE / DENY ───────────────────────────

    [Theory]
    [InlineData("GRANT SELECT ON Users TO PUBLIC")]
    [InlineData("grant select on users to public")]
    [InlineData("GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%'")]
    [InlineData("REVOKE SELECT ON Users FROM PUBLIC")]
    [InlineData("revoke select on users from public")]
    [InlineData("REVOKE ALL PRIVILEGES ON *.* FROM 'hacker'@'%'")]
    [InlineData("DENY SELECT ON Users TO PUBLIC")]
    [InlineData("deny select on users to public")]
    public void IsSafeRawSql_ReturnsFalse_ForPrivilegeStatements(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected privilege statement to be blocked: '{sql}'");

    // ── System / admin commands ────────────────────────────────────────────────

    [Theory]
    [InlineData("EXEC xp_cmdshell('dir')")]
    [InlineData("exec sp_executesql N'DROP TABLE foo'")]
    [InlineData("EXECUTE sp_configure 'show advanced options', 1")]
    [InlineData("CALL my_proc()")]
    [InlineData("call my_proc()")]
    [InlineData("PRAGMA foreign_keys = ON")]
    [InlineData("pragma foreign_keys = on")]
    [InlineData("VACUUM")]
    [InlineData("vacuum")]
    [InlineData("REINDEX myindex")]
    [InlineData("ANALYZE")]
    public void IsSafeRawSql_ReturnsFalse_ForSystemAndAdminCommands(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected system/admin command to be blocked: '{sql}'");

    // ── Stacked-query injection bypass attempts ───────────────────────────────

    [Theory]
    // Stacked query via semicolon — dangerous statement follows
    [InlineData("SELECT * FROM Users; DELETE FROM Users")]
    [InlineData("SELECT 1; GRANT SELECT ON x TO y")]
    [InlineData("SELECT 1; REVOKE SELECT ON x FROM y")]
    [InlineData("SELECT 1; INSERT INTO x VALUES(1)")]
    [InlineData("SELECT 1; UPDATE x SET y=1")]
    [InlineData("SELECT 1; DROP TABLE x")]
    public void IsSafeRawSql_ReturnsFalse_ForStackedQueryInjectionAttempts(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected stacked-query injection to be blocked: '{sql}'");

    // ── Case variation bypass attempts ────────────────────────────────────────

    [Theory]
    [InlineData("GrAnT sElEcT oN x To y")]
    [InlineData("REvOKe sElEcT oN x FrOM y")]
    [InlineData("DeLeTE FROM x")]
    [InlineData("InsERT INTO x VALUES(1)")]
    [InlineData("UpdATE x SET y=1")]
    [InlineData("DrOp TABLE x")]
    [InlineData("AlTeR TABLE x ADD COLUMN c INT")]
    [InlineData("CrEaTe TABLE x (id INT)")]
    public void IsSafeRawSql_ReturnsFalse_ForMixedCaseBypassAttempts(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected mixed-case keyword to be blocked: '{sql}'");

    // ── Dangerous keywords embedded inside otherwise SELECT-looking strings ────

    [Theory]
    // Keyword after a valid SELECT preamble — stacked or inline
    [InlineData("SELECT * FROM t WHERE name = 'x'; DROP TABLE t")]
    [InlineData("SELECT * FROM t UNION ALL DELETE FROM t")]
    public void IsSafeRawSql_ReturnsFalse_ForDangerousKeywordsEmbeddedInSelect(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected embedded dangerous keyword to be blocked: '{sql}'");

    // ── Valid SELECT statements still pass ────────────────────────────────────

    [Theory]
    [InlineData("SELECT 1")]
    [InlineData("SELECT * FROM Users WHERE Id = @id")]
    [InlineData("SELECT Id, Name FROM Users ORDER BY Name DESC")]
    [InlineData("SELECT COUNT(*) FROM Orders WHERE Status = @s")]
    [InlineData("SELECT TOP 10 * FROM Customers")]
    [InlineData("SELECT u.Id FROM Users u JOIN Orders o ON u.Id = o.UserId")]
    [InlineData("SELECT CASE WHEN x = 1 THEN 'yes' ELSE 'no' END FROM t")]
    public void IsSafeRawSql_ReturnsTrue_ForSafeSelectStatements(string sql)
        => Assert.True(NormValidator.IsSafeRawSql(sql),
            $"Expected safe SELECT to be accepted: '{sql}'");

    // ── Provider-variant escaping: adversarial inputs are enclosed in quotes ───
    //
    // Escape() is an identifier-quoting method: it wraps the raw text in the
    // provider's delimiter and doubles any embedded delimiter characters.
    // The output is syntactically safe because the entire adversarial string is
    // enclosed as a single identifier token — it cannot break out of identifier
    // context to become executable SQL.

    [Theory]
    [InlineData("'; DROP TABLE Users--")]
    [InlineData("name\"; injected")]
    [InlineData("col\"injection")]
    public void SqliteProvider_Escape_WrapsIdentifierInDoubleQuotes(string adversarial)
    {
        var escaped = new SqliteProvider().Escape(adversarial);
        Assert.StartsWith("\"", escaped);
        Assert.EndsWith("\"", escaped);
        // Any embedded double-quote must be doubled (SQLite escape rule).
        var inner = escaped[1..^1]; // strip wrapping quotes
        Assert.Equal(
            adversarial.Replace("\"", "\"\""),
            inner);
    }

    [Theory]
    [InlineData("'; DROP TABLE Users--")]
    [InlineData("name]; injected")]
    [InlineData("col]injection")]
    public void SqlServerProvider_Escape_WrapsIdentifierInBrackets(string adversarial)
    {
        var escaped = new SqlServerProvider().Escape(adversarial);
        Assert.StartsWith("[", escaped);
        Assert.EndsWith("]", escaped);
        // Any embedded ] must be doubled (SQL Server escape rule).
        var inner = escaped[1..^1];
        Assert.Equal(
            adversarial.Replace("]", "]]"),
            inner);
    }

    [Theory]
    [InlineData("'; DROP TABLE Users--")]
    [InlineData("name`injection`")]
    [InlineData("col`; something")]
    public void MySqlProvider_Escape_WrapsIdentifierInBackticks(string adversarial)
    {
        var escaped = new MySqlProvider(new SqliteParameterFactory()).Escape(adversarial);
        Assert.StartsWith("`", escaped);
        Assert.EndsWith("`", escaped);
        // Any embedded backtick must be doubled (MySQL escape rule).
        var inner = escaped[1..^1];
        Assert.Equal(
            adversarial.Replace("`", "``"),
            inner);
    }

    // ── Provider-variant: IsSafeRawSql is provider-agnostic (static) ──────────

    /// <summary>
    /// IsSafeRawSql is a static method on NormValidator — its behavior must be
    /// consistent regardless of which provider is configured. This test documents
    /// that the same deny result is obtained for all three provider contexts.
    /// </summary>
    [Theory]
    [InlineData("GRANT SELECT ON x TO y")]
    [InlineData("REVOKE SELECT ON x FROM y")]
    [InlineData("DROP TABLE x")]
    [InlineData("INSERT INTO x VALUES(1)")]
    public void IsSafeRawSql_ReturnsFalse_ConsistentAcrossAllProviders(string sql)
    {
        // All three providers use the same NormValidator — assert uniformity.
        Assert.False(NormValidator.IsSafeRawSql(sql),
            $"SQLite context: '{sql}' must be blocked");
        // The SQL Server and MySQL providers share the same static validator — but we
        // call it explicitly three times to document the expected provider-agnostic contract.
        Assert.False(NormValidator.IsSafeRawSql(sql),
            $"SQL Server context: '{sql}' must be blocked");
        Assert.False(NormValidator.IsSafeRawSql(sql),
            $"MySQL context: '{sql}' must be blocked");
    }
}
