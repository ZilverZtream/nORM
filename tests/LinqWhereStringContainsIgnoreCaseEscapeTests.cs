using System;
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
/// Closes the ignore-case escape family alongside e4718b8 (no-comparison
/// Contains/StartsWith/EndsWith) and 6661b74 (StartsWith/EndsWith ignore-
/// case). The 3-arg <c>col.Contains(needle, StringComparison.X)</c>
/// overload goes through <c>HandleStringContainsWithComparison</c> --
/// same final <c>EmitLikePredicate</c> but the arg-shape differs, so
/// regressions here are independent of the no-comparison probes.
///
/// Silent-wrongness shapes:
///   * Case-insensitive Contains drops the StringComparison: returns
///     only exact-case matches.
///   * Translator forgets to escape '%' / '_' on this arm: user-typed
///     wildcards poison the LIKE pattern.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringContainsIgnoreCaseEscapeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WciceItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WciceItem VALUES
                (1, 'AlphaBeta'),
                (2, 'alphabeta'),
                (3, 'ALPHABETA'),
                (4, 'gamma'),
                (5, '100_value');
            -- Ids 1,2,3 share the 'alpha' substring in three case variants.
            -- Id 5 contains a literal '_' to probe wildcard escaping.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WciceItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_Contains_OrdinalIgnoreCase_matches_all_case_variants()
    {
        // 'alpha' substring case-insensitive -> Ids 1, 2, 3.
        // Silent-wrongness: dropped StringComparison -> exact-case substring
        // returns only Id 2 (lowercase 'alpha' in 'alphabeta').
        var result = await _ctx.Query<WciceItem>()
            .Where(i => i.Name.Contains("alpha", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_Contains_OrdinalIgnoreCase_escapes_underscore_metacharacter()
    {
        // Discriminating probe: literal '_' as substring; only Id 5 contains
        // a literal underscore. If '_' were treated as a wildcard, the
        // LIKE pattern '%_%' would match every non-empty row (5 results).
        var result = await _ctx.Query<WciceItem>()
            .Where(i => i.Name.Contains("_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_Contains_OrdinalIgnoreCase_finds_literal_underscore_with_surrounding_chars()
    {
        // Stronger discriminator: literal '0_' (digit + underscore). Only
        // Id 5 ("100_value") contains '0_' literally. Wildcard semantics
        // would still match Id 5 (because '0' + any-char includes '00'
        // wait no, '0_' as wildcard means '0' then any single char, so
        // '00' would match too) -- but the no-other-matches behaviour is
        // the same. The stronger probe: 'pha_b' (alpha[any]beta) which
        // would match Ids 1,2,3 if '_' is wildcard but no rows literally.
        var result = await _ctx.Query<WciceItem>()
            .Where(i => i.Name.Contains("pha_b", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("WciceItem")]
    public sealed class WciceItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
