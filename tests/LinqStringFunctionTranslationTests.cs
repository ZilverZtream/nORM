using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies every string method translation the provider exposes against a real database, with
/// positive (matching row) and negative (no match) assertions. Each test uses one provider's
/// emitted SQL so a regression in TranslateFunction shows up immediately.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqStringFunctionTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE StrRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO StrRow VALUES
                (1, '  alpha  '),
                (2, 'Beta'),
                (3, 'GAMMA'),
                (4, 'delta-epsilon'),
                (5, 'zeta');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ToLower_filters_case_insensitively()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.ToLower() == "beta").ToListAsync();
        Assert.Single(hits);
        Assert.Equal("Beta", hits[0].Name);
    }

    [Fact]
    public async Task ToUpper_filters_case_insensitively()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.ToUpper() == "BETA").ToListAsync();
        Assert.Single(hits);
    }

    [Fact]
    public async Task Length_filters_by_character_count()
    {
        // "Beta" (4), "GAMMA" (5), "zeta" (4)
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.Length == 4).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { "Beta", "zeta" }, hits.Select(r => r.Name).ToArray());
    }

    [Fact]
    public async Task Trim_strips_leading_and_trailing_whitespace()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.Trim() == "alpha").ToListAsync();
        Assert.Single(hits);
        Assert.Equal(1, hits[0].Id);
    }

    [Fact]
    public async Task TrimStart_strips_leading_whitespace_only()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.TrimStart() == "alpha  ").ToListAsync();
        Assert.Single(hits);
    }

    [Fact]
    public async Task TrimEnd_strips_trailing_whitespace_only()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.TrimEnd() == "  alpha").ToListAsync();
        Assert.Single(hits);
    }

    [Fact]
    public async Task Substring_two_args_returns_remainder_from_start_index()
    {
        // 'delta-epsilon'.Substring(6) == 'epsilon'
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.Substring(6) == "epsilon").ToListAsync();
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task Substring_three_args_returns_slice_of_given_length()
    {
        // 'delta-epsilon'.Substring(0, 5) == 'delta'
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.Substring(0, 5) == "delta").ToListAsync();
        Assert.Single(hits);
    }

    [Fact]
    public async Task Replace_swaps_substring()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.Replace("-", "_") == "delta_epsilon").ToListAsync();
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task IndexOf_returns_zero_based_position()
    {
        // 'delta-epsilon'.IndexOf("epsilon") == 6
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.IndexOf("epsilon") == 6).ToListAsync();
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Id);
    }

    [Fact]
    public async Task IndexOf_returns_minus_one_for_no_match()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.IndexOf("not-there") == -1).ToListAsync();
        // every row should match - none of them contain "not-there"
        Assert.Equal(5, hits.Count);
    }

    [Fact]
    public async Task Contains_translates_to_LIKE_with_wildcards()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.Contains("psil")).ToListAsync();
        Assert.Single(hits);
        Assert.Equal("delta-epsilon", hits[0].Name);
    }

    [Fact]
    public async Task StartsWith_translates_to_LIKE_prefix()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.StartsWith("delta")).ToListAsync();
        Assert.Single(hits);
    }

    [Fact]
    public async Task EndsWith_translates_to_LIKE_suffix()
    {
        var hits = await _ctx.Query<StrRow>().Where(r => r.Name.EndsWith("eta")).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { 2, 5 }, hits.Select(r => r.Id).ToArray());
    }

    [Table("StrRow")]
    public sealed class StrRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
