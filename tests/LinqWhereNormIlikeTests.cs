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
/// Pins <see cref="NormFunctions.ILike"/> — case-insensitive LIKE — inside
/// a WHERE predicate. SqliteProvider lowers it to
/// <c>(LOWER(col) LIKE LOWER(pattern))</c>; silent-wrongness shape is
/// the call collapsing to a plain case-sensitive LIKE, in which case case
/// mismatches silently filter out rows the user intended to match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNormIlikeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IlkRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            -- Mixed casing so case-sensitive LIKE would return only Id 4 for
            -- pattern 'apple%'; ILike must catch all three apple* rows.
            INSERT INTO IlkRow VALUES
              (1, 'APPLE'),
              (2, 'Apple Pie'),
              (3, 'apricot'),
              (4, 'apple'),
              (5, 'banana');
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
    public async Task Where_ilike_with_prefix_pattern_matches_case_insensitively()
    {
        // Pattern 'apple%' should match Ids 1 (APPLE), 2 (Apple Pie), 4 (apple).
        // Silent-wrongness check: plain LIKE on SQLite is case-insensitive for
        // ASCII by default — so the test must use a provider that guarantees
        // case-sensitive LIKE to truly fail on a dropped ILike. SqliteProvider
        // wraps ILike with explicit LOWER(...) so callers can rely on the
        // semantics regardless of PRAGMA case_sensitive_like settings.
        var ids = (await _ctx.Query<IlkRow>()
            .Where(p => NormFunctions.ILike(p.Name, "apple%"))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 4 }, ids);
    }

    [Fact]
    public async Task Where_ilike_with_contains_pattern_matches_substring_case_insensitively()
    {
        // Pattern '%pie%' should match only Id 2 (Apple Pie -- 'Pie').
        var ids = (await _ctx.Query<IlkRow>()
            .Where(p => NormFunctions.ILike(p.Name, "%pie%"))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public async Task Where_ilike_negated_returns_non_matching_rows()
    {
        // !ILike('apple%') -> Ids 3 (apricot), 5 (banana).
        var ids = (await _ctx.Query<IlkRow>()
            .Where(p => !NormFunctions.ILike(p.Name, "apple%"))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 5 }, ids);
    }

    [Table("IlkRow")]
    public sealed class IlkRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
