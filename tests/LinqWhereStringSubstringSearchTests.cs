using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the substring-search family in Where predicates:
///   * <c>col.Contains(needle)</c> -> <c>col LIKE '%needle%'</c>
///   * <c>col.StartsWith(prefix)</c> -> <c>col LIKE 'prefix%'</c>
///   * <c>col.EndsWith(suffix)</c> -> <c>col LIKE '%suffix'</c>
///
/// Most search/filter UIs reach for these. The translator must:
///   * Escape SQL LIKE metacharacters in the literal needle/prefix/suffix
///     (%, _, ESCAPE-char) so a user-typed '%' doesn't widen the match.
///   * Match SQLite's default case-sensitive TEXT comparison (unless the
///     user explicitly asks for NOCASE via StringComparison overload).
///   * Honor 3VL: NULL column returns false for any LIKE comparison.
///
/// Silent-wrongness shapes:
///   * Translator forgets to escape '%' -> Contains("100%") matches
///     everything starting with "100".
///   * Translator emits =, not LIKE -> Contains("alpha") matches only
///     exact-equality rows.
///   * Translator drops the leading or trailing % wildcard -> Contains
///     degrades to StartsWith or EndsWith silently.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringSubstringSearchTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WssItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WssItem VALUES
                (1, 'alpha'),
                (2, 'alphabet'),
                (3, 'beta-alpha'),
                (4, 'gamma'),
                (5, '100%-off');
            -- Id 1 starts and ends with alpha and contains it.
            -- Id 2 starts with alpha, contains it, but doesn't end with it.
            -- Id 3 contains alpha and ends with it, but doesn't start with it.
            -- Id 4 has no overlap.
            -- Id 5 has a literal '%' to probe LIKE metacharacter escaping.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WssItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_string_Contains_matches_substring_anywhere()
    {
        // alpha appears in Ids 1, 2, 3. Silent-wrongness:
        //   * dropped trailing % -> matches only 1, 2 (StartsWith-only)
        //   * dropped leading %  -> matches only 1, 3 (EndsWith-only)
        //   * emit = instead of LIKE -> matches only Id 1 (exact)
        var result = await _ctx.Query<WssItem>()
            .Where(i => i.Name.Contains("alpha"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_string_StartsWith_matches_only_prefix_rows()
    {
        // alpha-prefix: Ids 1, 2. Id 3 has alpha at the END only.
        var result = await _ctx.Query<WssItem>()
            .Where(i => i.Name.StartsWith("alpha"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_string_EndsWith_matches_only_suffix_rows()
    {
        // alpha-suffix: Ids 1, 3. Id 2 ('alphabet') doesn't end with alpha.
        var result = await _ctx.Query<WssItem>()
            .Where(i => i.Name.EndsWith("alpha"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_string_Contains_escapes_underscore_metacharacter()
    {
        // User-typed '_' must be treated as a literal single underscore, not
        // a single-character wildcard. None of the seeded rows contain a
        // literal '_', so:
        //   * Escaped (literal '_'): 0 rows
        //   * Not escaped ('_' as wildcard): "alph_" -> "alph + any-char";
        //     matches 'alpha' (positions 0-4) and 'alphabet' (positions 0-4)
        //     and 'beta-alpha' (positions 5-9) -> 3 rows
        var result = await _ctx.Query<WssItem>()
            .Where(i => i.Name.Contains("alph_"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Fact]
    public async Task Where_with_string_Contains_finds_literal_percent_when_present()
    {
        // Discriminating probe: Contains("%-") matches the literal sequence
        // '%-' which appears only in Id 5 ("100%-off"). If '%' were treated
        // as a wildcard, "%-" would mean "any-chars + '-'" and Id 3
        // ("beta-alpha") would also match -> {3, 5}. Asserting {5} only
        // confirms the literal-escape path is correct.
        var result = await _ctx.Query<WssItem>()
            .Where(i => i.Name.Contains("%-"))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WssItem")]
    public sealed class WssItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
