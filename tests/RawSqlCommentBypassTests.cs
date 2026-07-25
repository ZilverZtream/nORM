using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression (F3/F4 — raw-SQL validator comment-handling differentials): the SQL normalizer must
/// strip comments the way the TARGET engine does, or it analyzes different SQL than the server executes.
/// F3: MySQL/MariaDB EXECUTE the body of a <c>/*! ... */</c> conditional comment — nORM stripped it as inert.
/// F4: MySQL/SQLite do NOT nest block comments (they close at the first <c>*/</c>) — nORM's nesting swallowed
/// text those engines run. Both were validator bypasses on MySQL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class RawSqlCommentBypassTests
{
    // ── F3: MySQL executable /*! ... */ comments must be analyzed, not stripped ──
    [Theory]
    [InlineData("SELECT id FROM t /*!00000;DROP TABLE users*/")]
    [InlineData("SELECT id FROM t /*!50000;UPDATE users SET is_admin=1*/")]
    [InlineData("SELECT id FROM t /*!00000;GRANT ALL ON *.* TO 'x'@'%'*/")]
    public void MySql_versioned_comment_stacked_statement_is_rejected(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql, new MySqlProvider()),
            $"MySQL executable /*! comment must be analyzed and rejected: {sql}");

    [Fact]
    public void MySql_versioned_comment_stacked_statement_is_rejected_for_nonquery()
    {
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawNonQuerySql("UPDATE t SET a=1 /*!00000;DROP TABLE users*/", new MySqlProvider()));
    }

    // ── F4: non-nesting engines must see text after the first */ ──
    [Fact]
    public void MySql_non_nesting_block_comment_no_longer_hides_trailing_sql()
    {
        // MySQL closes the comment at the first */, executing the UNION. The normalizer must now SEE it
        // (differential closed) rather than swallowing it as a nested comment body.
        var normalized = NormValidator.NormalizeSql(
            "SELECT id FROM t /* /* */ UNION SELECT password FROM users -- */", new MySqlProvider());
        Assert.Contains("union select password", normalized);
    }

    // ── Controls: on engines that DO nest / DON'T execute /*!, behavior is unchanged ──
    [Fact]
    public void Postgres_still_nests_block_comments()
    {
        // PostgreSQL nests, so the whole /* /* */ */ is one comment — the inner text is genuinely inert there.
        var normalized = NormValidator.NormalizeSql(
            "SELECT id FROM t /* /* inner */ still outer */ WHERE x=1", new PostgresProvider());
        Assert.DoesNotContain("still outer", normalized);
        Assert.Contains("where x=1", normalized);
    }

    [Fact]
    public void Non_mysql_treats_versioned_comment_as_inert()
    {
        // On SQLite /*! ... */ is an ordinary (inert) comment — stripped, so a plain SELECT stays valid.
        Assert.True(NormValidator.IsSafeRawSql("SELECT id FROM t /*!00000 ORDER BY id*/", new SqliteProvider()));
    }
}
