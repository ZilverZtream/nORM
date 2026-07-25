using System;
using System.Diagnostics;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression (ReDoS): the SQLite <c>regexp</c> / <c>regexp_replace</c> UDFs run a .NET Regex per row
/// over an application-supplied pattern (reachable from LINQ <c>Regex.IsMatch(col, pattern)</c>). Without a
/// MatchTimeout a catastrophic-backtracking pattern such as <c>(a+)+$</c> spins the SQLite worker thread
/// effectively forever. The provider now compiles both UDFs with a bounded MatchTimeout, so a pathological
/// pattern FAILS LOUD within ~1s instead of hanging. A legitimate per-row match completes in microseconds, so
/// the bound never trips in normal use.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class SqliteRegexpUdfTimeoutTests
{
    [Theory]
    [InlineData("SELECT regexp(@pat, @input)")]
    [InlineData("SELECT regexp_replace(@input, @pat, '')")]
    public void Regexp_udfs_bound_catastrophic_backtracking_instead_of_hanging(string sql)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        new SqliteProvider().InitializeConnection(cn); // registers the timeout-bounded regexp UDFs

        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        // Classic exponential-backtracking pattern with a non-matching tail: without a MatchTimeout this
        // backtracks ~2^n steps (n = number of 'a's) — minutes-to-forever at n=30.
        cmd.Parameters.AddWithValue("@pat", "(a+)+$");
        cmd.Parameters.AddWithValue("@input", new string('a', 30) + "!");

        var sw = Stopwatch.StartNew();
        var ex = Record.Exception(() => cmd.ExecuteScalar());
        sw.Stop();

        // With the fix: the UDF's Regex throws RegexMatchTimeoutException (surfaced through SQLite) within ~1s.
        // Without it: ExecuteScalar would not return within the ceiling below — the test would hang/fail.
        Assert.NotNull(ex);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"regexp UDF did not fail fast — MatchTimeout not applied? elapsed={sw.Elapsed}");
    }

    [Fact]
    public void Regexp_udf_still_matches_normal_patterns()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        new SqliteProvider().InitializeConnection(cn);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT regexp(@pat, @input)";
        cmd.Parameters.AddWithValue("@pat", "^h.*o$");
        cmd.Parameters.AddWithValue("@input", "hello");
        Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}
