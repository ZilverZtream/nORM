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
/// Pins the critical empty-string vs NULL distinction in WHERE predicates.
/// Two silent-wrongness shapes:
/// <list type="bullet">
///   <item><c>== ""</c> must NOT match NULL rows (SQL: <c>col = ''</c>
///         excludes NULL by 3VL).</item>
///   <item><c>== null</c> must emit <c>IS NULL</c> not <c>= NULL</c>
///         (the latter is always UNKNOWN and would return zero rows
///         silently).</item>
/// </list>
/// Plus <see cref="string.IsNullOrEmpty"/> as the both-arms idiom.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringEmptyVsNullTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsenRow (Id INTEGER PRIMARY KEY, Name TEXT NULL);
            INSERT INTO WsenRow VALUES
              (1, 'alpha'),
              (2, ''),     -- empty string
              (3, NULL),   -- explicit null
              (4, 'bravo'),
              (5, ''),     -- another empty
              (6, NULL);   -- another null
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
    public async Task Where_string_equals_empty_returns_only_empty_string_rows_not_nulls()
    {
        // == "" -> Ids {2, 5} only. NULL rows (3, 6) excluded by 3VL.
        // Silent-wrongness check: if translator emits OR IS NULL alongside
        // the = '' check (well-meaning auto-injection), the result would
        // also include 3 and 6.
        var ids = (await _ctx.Query<WsenRow>()
            .Where(p => p.Name == "")
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 5 }, ids);
    }

    [Fact]
    public async Task Where_string_equals_null_emits_is_null_predicate_and_matches_null_rows()
    {
        // == null must emit `IS NULL` (not `= NULL` which is always UNKNOWN).
        // Silent-wrongness check: a naive `= @p` binding with @p=DBNull
        // returns zero rows; the translator must lower to IS NULL so Ids
        // {3, 6} match.
        var ids = (await _ctx.Query<WsenRow>()
            .Where(p => p.Name == null)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 6 }, ids);
    }

    [Fact]
    public async Task Where_string_is_null_or_empty_matches_both_arms()
    {
        // IsNullOrEmpty -> (col IS NULL OR col = '') -> Ids {2, 3, 5, 6}.
        // The both-arms convenience method is the canonical way to get the
        // C#-intuition behaviour that excludes only the non-empty strings.
        var ids = (await _ctx.Query<WsenRow>()
            .Where(p => string.IsNullOrEmpty(p.Name))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 3, 5, 6 }, ids);
    }

    [Fact]
    public async Task Where_string_not_equals_empty_includes_null_rows_per_csharp_semantics()
    {
        // !=  "" returns NON-empty strings PLUS NULL rows (Ids 1, 3, 4, 6).
        // nORM emits a null-safe `(col <> @p OR (col IS NULL AND @p IS NULL))`
        // form for nullable comparisons (commit 50 family), which matches the
        // C# `null != ""` -> true intuition rather than strict SQL 3VL.
        // The nullable-bool sister test (a476b97) deliberately preserves SQL
        // 3VL for the bool case -- the divergence is per-type and intentional.
        // If a future "fix" reverts to strict SQL 3VL here, Ids 3 and 6 would
        // silently drop out and this assertion would catch it.
        var ids = (await _ctx.Query<WsenRow>()
            .Where(p => p.Name != "")
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3, 4, 6 }, ids);
    }

    [Table("WsenRow")]
    public sealed class WsenRow
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }
    }
}
