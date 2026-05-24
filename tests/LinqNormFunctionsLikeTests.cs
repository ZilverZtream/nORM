using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// NormFunctions.Like(value, pattern) emits a raw SQL LIKE comparison so the caller controls
/// the wildcard pattern without nORM's automatic LIKE-pattern escaping that string.Contains /
/// StartsWith perform under the hood. The translation goes through the [SqlFunction] attribute
/// path on the static method.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNormFunctionsLikeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LkRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO LkRow VALUES
                (1, 'alpha'),
                (2, 'bravo'),
                (3, 'gamma'),
                (4, 'delta'),
                (5, 'alabama');
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
    public async Task Like_with_percent_wildcard_matches_prefix()
    {
        var ids = (await _ctx.Query<LkRow>()
            .Where(r => NormFunctions.Like(r.Name, "al%"))
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 5 }, ids);
    }

    [Fact]
    public async Task Like_with_underscore_wildcard_matches_single_char()
    {
        var ids = (await _ctx.Query<LkRow>()
            .Where(r => NormFunctions.Like(r.Name, "_amma"))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3 }, ids);
    }

    [Fact]
    public async Task Like_with_no_wildcards_acts_as_equality()
    {
        var ids = (await _ctx.Query<LkRow>()
            .Where(r => NormFunctions.Like(r.Name, "delta"))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4 }, ids);
    }

    [Fact]
    public async Task ILike_matches_uppercase_pattern_against_lowercase_value()
    {
        // ILike is case-insensitive. Pattern 'AL%' should match both 'alpha' and 'alabama'.
        var ids = (await _ctx.Query<LkRow>()
            .Where(r => NormFunctions.ILike(r.Name, "AL%"))
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 5 }, ids);
    }

    [Fact]
    public async Task Like_pattern_is_bound_as_parameter_not_inlined()
    {
        // Pattern containing SQL syntax that would break the statement if it were inlined.
        // Captured local forces parameter binding via the closure-capture path.
        var dangerousPattern = "a%' OR '1'='1";
        var hits = await _ctx.Query<LkRow>()
            .Where(r => NormFunctions.Like(r.Name, dangerousPattern))
            .ToListAsync();
        // The pattern matches nothing because no row name contains the literal characters
        // `' OR '1'='1`. If the pattern were inlined into the SQL text the WHERE would
        // collapse to true and every row would come back.
        Assert.Empty(hits);
    }

    [Fact]
    public async Task ILike_matches_lowercase_pattern_against_uppercase_value()
    {
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "INSERT INTO LkRow VALUES (6, 'OMEGA');";
        await cmd.ExecuteNonQueryAsync();

        var ids = (await _ctx.Query<LkRow>()
            .Where(r => NormFunctions.ILike(r.Name, "omega"))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 6 }, ids);
    }

    [Table("LkRow")]
    public sealed class LkRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
