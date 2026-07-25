using System;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regressions for the raw-SQL denylist and identifier validator (audit F5/F6/F8).
/// F5: SQLite dangerous functions (load_extension/readfile/writefile/pragma_*) are single '_' tokens that the
///     word-level keyword checks missed. F6: IsSafeIdentifier's \s class admitted newlines/tabs (statement
///     breaks the threat model claims to reject). F8: the "SCRIPT" dangerous-pattern was a raw substring,
///     rejecting legitimate identifiers like script_id / transcript / description.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class RawSqlDenylistAndIdentifierTests
{
    // ── F5: dangerous SQLite functions are denied ──
    [Theory]
    [InlineData("SELECT load_extension('/tmp/evil.so')")]
    [InlineData("SELECT readfile('/etc/passwd')")]
    [InlineData("SELECT writefile('/tmp/x', 'y')")]
    [InlineData("SELECT * FROM pragma_table_list")]
    [InlineData("SELECT * FROM pragma_table_info('users')")]
    public void Dangerous_sqlite_functions_are_denied(string sql)
        => Assert.False(NormValidator.IsSafeRawSql(sql, new SqliteProvider()), $"must be denied: {sql}");

    [Fact]
    public void Legitimate_column_named_like_a_function_prefix_is_allowed()
    {
        // 'loaded' / 'usage' contain denied keywords as substrings but are distinct tokens — must pass.
        Assert.True(NormValidator.IsSafeRawSql("SELECT loaded, usage_count FROM reports", new SqliteProvider()));
    }

    // ── F6: IsSafeIdentifier rejects embedded statement breaks ──
    [Theory]
    [InlineData("Users\nDROP")]
    [InlineData("Users\tDROP")]
    [InlineData("Users\rDROP")]
    public void IsSafeIdentifier_rejects_embedded_whitespace_breaks(string ident)
        => Assert.False(DbContext.IsSafeIdentifier(ident), $"must reject embedded break: {ident:l}");

    [Theory]
    [InlineData("Order Details")] // a literal space is still allowed (bracketed identifiers)
    [InlineData("Valid_Name")]
    [InlineData("Users")]
    public void IsSafeIdentifier_still_accepts_wellformed_names(string ident)
        => Assert.True(DbContext.IsSafeIdentifier(ident), $"must accept: {ident}");

    // ── F8: SCRIPT dangerous-pattern is word-bounded, not a substring ──
    [Theory]
    [InlineData("SELECT script_id FROM t")]
    [InlineData("SELECT transcript, description_script FROM t")]
    public void Identifiers_containing_script_substring_are_not_flagged(string sql)
    {
        NormValidator.ValidateRawSql(sql); // must not throw
    }

    [Fact]
    public void Standalone_dangerous_patterns_still_rejected()
    {
        Assert.Throws<ArgumentException>(() => NormValidator.ValidateRawSql("EXEC('a'); SCRIPT foo"));
        Assert.Throws<ArgumentException>(() => NormValidator.ValidateRawSql("SELECT * INTO OUTFILE '/tmp/x' FROM t"));
    }
}
