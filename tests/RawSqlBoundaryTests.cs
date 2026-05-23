using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Raw SQL and stored-procedure security boundaries (blocker 15).
///
/// 1. DML is rejected — INSERT / UPDATE / DELETE all fail the AST allowlist.
/// 2. CTE (WITH ... AS (...) SELECT ...) is accepted.
/// 3. SELECT is accepted; DML after semicolon (stacked query) is rejected.
/// 4. Raw SQL with a literal value in a WHERE clause (no @param markers) raises
///    NormUsageException — the injection heuristic must fire when no parameters
///    are present and the SQL looks like a literal-value filter.
/// 5. Stored procedures are not automatically tenant-filtered — the raw call
///    bypasses global-filter injection.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class RawSqlBoundaryTests
{
    // ── 1. DML is rejected ────────────────────────────────────────────────────

    [Theory]
    [InlineData("INSERT INTO Users VALUES(1,'x')")]
    [InlineData("UPDATE Users SET Name='x' WHERE Id=1")]
    [InlineData("DELETE FROM Users WHERE Id=1")]
    [InlineData("TRUNCATE TABLE Users")]
    [InlineData("DROP TABLE Users")]
    [InlineData("ALTER TABLE Users ADD COLUMN X INT")]
    public void IsSafeRawSql_ReturnsFalse_ForDmlAndDdl(string sql)
    {
        Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected DML/DDL to be rejected: '{sql}'");
    }

    // ── 2. CTE is accepted ────────────────────────────────────────────────────

    [Theory]
    [InlineData("WITH cte AS (SELECT Id, Name FROM Users) SELECT * FROM cte")]
    [InlineData("with numbered AS (SELECT Id FROM Orders WHERE Active = 1) SELECT * FROM numbered")]
    [InlineData("WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 5) SELECT n FROM cte")]
    public void IsSafeRawSql_ReturnsTrue_ForCteSelect(string sql)
    {
        Assert.True(NormValidator.IsSafeRawSql(sql),
            $"Expected CTE SELECT to be accepted: '{sql}'");
    }

    // ── 3. Stacked queries (DML after semicolon) are rejected ─────────────────

    [Theory]
    [InlineData("SELECT * FROM Users; DELETE FROM Users")]
    [InlineData("SELECT Id FROM t; DROP TABLE t")]
    [InlineData("SELECT 1; INSERT INTO x VALUES(1)")]
    public void IsSafeRawSql_ReturnsFalse_ForStackedDmlAfterSelect(string sql)
    {
        Assert.False(NormValidator.IsSafeRawSql(sql),
            $"Expected stacked-query injection to be rejected: '{sql}'");
    }

    // ── 4. Literal-value WHERE clause without @param markers triggers injection heuristic ─

    [Fact]
    public void ValidateRawSql_LiteralEmailInWhereWithoutParam_ThrowsNormUsageException()
    {
        // A SQL statement containing a string literal that looks like an injected value
        // (e.g. an email address with '@') inside a WHERE clause, WITHOUT any corresponding
        // @param markers, should trigger the injection heuristic and raise NormUsageException.
        // Root: the `@` in `user@example.com` is inside a string literal, so it does NOT
        // count as a parameter marker. With 0 markers and a suspicious literal, ValidateRawSql
        // raises an exception to flag the potential injection.
        var sql = "SELECT * FROM Users WHERE Email = 'user@example.com'";
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
    }

    [Fact]
    public void ValidateRawSql_RealAtParamMarker_Accepted()
    {
        // A real @param marker (outside any literal) satisfies the heuristic.
        var sql = "SELECT * FROM Users WHERE Email = @email";
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
        Assert.Null(ex);
    }

    [Fact]
    public void ValidateRawSql_LiteralAndRealParam_Accepted()
    {
        // Even when the WHERE has a literal email-like string AND a real @param,
        // the real @param satisfies the marker check so the heuristic does not fire.
        var sql = "SELECT * FROM Users WHERE Email = 'user@example.com' AND Id = @id";
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(sql, parameters: null));
        Assert.Null(ex);
    }

    // ── 5. Stored procedure bypasses global-filter injection ──────────────────

    [Fact]
    public async Task StoredProcedure_IsNotAutomaticallyTenantFiltered()
    {
        // Stored procedure execution bypasses nORM's global-filter injection entirely.
        // The test demonstrates that rows from ALL tenants are visible when called via
        // ExecuteStoredProcedureAsync, even when a TenantProvider is configured on the context.
        // This is a documented design decision (see docs/stored-procedure-security.md):
        // stored procs are "privileged execution paths" and must be tenant-filtered by
        // the procedure itself if required.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE RSB_Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId INTEGER NOT NULL);" +
                "INSERT INTO RSB_Item VALUES(1,'T1Row',1);" +
                "INSERT INTO RSB_Item VALUES(2,'T2Row',2);";
            cmd.ExecuteNonQuery();
        }

        // Configure a tenant-aware context; global filter applies to LINQ queries.
        // TenantId column matches the default TenantColumnName, so no OnModelCreating needed.
        var opts = new nORM.Configuration.DbContextOptions
        {
            TenantProvider = new FixedTenantProviderRsb(1)
        };
        using var ctx = new nORM.Core.DbContext(cn, new SqliteProvider(), opts);

        // ExecuteStoredProcedureAsync (SQLite uses CommandType.Text, so pass a SELECT)
        // must return ALL rows, not just tenant 1.
        var allRows = await ctx.ExecuteStoredProcedureAsync<RsbItem>(
            "SELECT Id, Name, TenantId FROM RSB_Item ORDER BY Id");

        Assert.Equal(2, allRows.Count);
        Assert.Contains(allRows, r => r.TenantId == 1);
        Assert.Contains(allRows, r => r.TenantId == 2);
    }

    // ── Support types ─────────────────────────────────────────────────────────

    private sealed class FixedTenantProviderRsb : nORM.Enterprise.ITenantProvider
    {
        private readonly int _id;
        public FixedTenantProviderRsb(int id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }
}

[System.ComponentModel.DataAnnotations.Schema.Table("RSB_Item")]
file class RsbItem
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int TenantId { get; set; }
}
