using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// N2 (CVE-2025-1094 class): a runtime string operand in a projection string-match must be PARAMETERIZED, not
/// inlined — so it never reaches SQL text. Previously a runtime Contains/StartsWith/EndsWith term was inlined,
/// and an embedded U+0000 truncated the SQLite statement (SqliteException "unrecognized token"). It is now a
/// bound parameter (the LIKE escaping + %-wrapping happen in SQL), so NUL round-trips correctly. Where a value
/// MUST be a literal (a GROUP_CONCAT/string.Join separator, which MySQL requires be a literal), a NUL is
/// rejected loud rather than silently truncating.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ProjectionLikeParameterizationTests
{
    [Table("PlpRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Grp { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PlpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Grp INTEGER NOT NULL DEFAULT 1);" +
                              "INSERT INTO PlpRow (Id,Name,Grp) VALUES (1,'alice',1),(2,'bob',1);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Theory]
    [InlineData("ali", true)]   // matches 'alice'
    [InlineData("x\0y", false)] // NUL byte — previously truncated the statement; now a bound value, matches nothing
    [InlineData("z", false)]
    public void Contains_projection_with_runtime_term_is_parameterized(string term, bool aliceMatches)
    {
        using var ctx = NewCtx();
        var hit = ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Name.Contains(term)).First();
        Assert.Equal(aliceMatches, hit);
    }

    [Fact]
    public void StartsWith_and_EndsWith_runtime_terms_are_correct()
    {
        using var ctx = NewCtx();
        // Captured locals are closures (parameterized), not compile-time constants.
        string al = "al", ob = "ob", ice = "ice", pct = "a%e";
        Assert.True(ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Name.StartsWith(al)).First());
        Assert.False(ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Name.StartsWith(ob)).First());
        Assert.True(ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Name.EndsWith(ice)).First());
        // a LIKE metacharacter in the term must be treated literally (escaped at runtime), not as a wildcard
        Assert.False(ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Name.Contains(pct)).First());
    }

    [Fact]
    public void Runtime_separator_with_nul_is_rejected_loud()
    {
        using var ctx = NewCtx();
        var sep = "a\0b"; // a GROUP_CONCAT separator must be a literal — a NUL cannot be encoded there
        Assert.Throws<NormQueryException>(() =>
            ctx.Query<Row>().GroupBy(r => r.Grp)
               .Select(g => string.Join(sep, g.Select(x => x.Name)))
               .ToList());
    }
}
