using System;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that <c>IsSafeRawSql</c> correctly handles provider-specific syntax.
/// Non-SQL-Server providers skip the TSQL AST parser to avoid false negatives for
/// dialect-specific statements like SQLite PRAGMA.
/// Also validates that side-effect commands are blocked for all providers.
/// </summary>
public class SqlSafetyProviderAwareTests
{
    private static readonly DatabaseProvider Sqlite    = new SqliteProvider();
    private static readonly DatabaseProvider SqlServer = new SqlServerProvider();
    private static readonly DatabaseProvider Postgres  = new PostgresProvider();
    private static readonly DatabaseProvider MySql     = new MySqlProvider();

    // Plain SELECT is safe for all providers
    [Theory]
    [InlineData("SELECT 1")]
    [InlineData("SELECT Id, Name FROM Users")]
    [InlineData("SELECT * FROM Orders WHERE Id = 1")]
    public void PlainSelect_IsSafe_ForAllProviders(string sql)
    {
        Assert.True(NormValidator.IsSafeRawSql(sql, Sqlite),    $"SQLite: expected safe for: {sql}");
        Assert.True(NormValidator.IsSafeRawSql(sql, SqlServer), $"SqlServer: expected safe for: {sql}");
        Assert.True(NormValidator.IsSafeRawSql(sql, null),      $"null provider: expected safe for: {sql}");
    }

    // PRAGMA must be blocked for all providers (keyword denylist applies first)
    [Theory]
    [InlineData("PRAGMA foreign_keys = ON")]
    [InlineData("pragma journal_mode=WAL")]
    public void Pragma_IsUnsafe_ForAllProviders(string sql)
    {
        Assert.False(NormValidator.IsSafeRawSql(sql, Sqlite),    $"SQLite should block: {sql}");
        Assert.False(NormValidator.IsSafeRawSql(sql, SqlServer), $"SqlServer should block: {sql}");
        Assert.False(NormValidator.IsSafeRawSql(sql, null),      $"null provider should block: {sql}");
    }

    // non-SQL-Server dialect syntax (e.g. WITH RECURSIVE) should be allowed
    // by the SQLite provider (bypasses TSql parser) but may fail TSql parse path.
    [Fact]
    public void SqliteDialect_WithRecursiveCte_IsSafe_ForSqliteProvider()
    {
        const string sql = "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n<5) SELECT n FROM cte";
        // SQLite provider skips TSql parser — should pass keyword check only
        Assert.True(NormValidator.IsSafeRawSql(sql, Sqlite));
    }
    [Theory]
    [InlineData("SELECT Id, Name FROM Users WHERE Id = @p0")]
    [InlineData("WITH recent AS (SELECT Id FROM Users) SELECT Id FROM recent")]
    public void ValidateRawQuerySql_AcceptsReadOnlyQueries_ForAllProviders(string sql)
    {
        NormValidator.ValidateRawQuerySql(sql, Sqlite);
        NormValidator.ValidateRawQuerySql(sql, SqlServer);
        NormValidator.ValidateRawQuerySql(sql, Postgres);
        NormValidator.ValidateRawQuerySql(sql, MySql);
    }

    [Theory]
    [InlineData("UPDATE Users SET Name = @p0 WHERE Id = @p1")]
    [InlineData("SELECT 1; DROP TABLE Users")]
    [InlineData("EXEC dbo.GetUsers")]
    public void ValidateRawQuerySql_RejectsPrivilegedStatements_ForAllProviders(string sql)
    {
        foreach (var provider in new[] { Sqlite, SqlServer, Postgres, MySql })
        {
            var ex = Assert.Throws<NormUsageException>(() =>
                NormValidator.ValidateRawQuerySql(sql, provider));

            Assert.Contains("read-only SELECT or CTE", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
