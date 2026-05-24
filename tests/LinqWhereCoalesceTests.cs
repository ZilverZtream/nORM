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
/// Pins <c>??</c> (null-coalesce) inside a WHERE predicate, e.g.
/// <c>Where(p =&gt; (p.Score ?? 0) &gt; 5)</c>. Critical SQL semantics: NULL
/// arithmetic / comparison propagates as UNKNOWN, which WHERE treats as false.
/// If the coalesce is dropped and the bare nullable column is compared, every
/// row with a NULL value is silently excluded — a row that the user explicitly
/// intended to participate (via the default substitution) goes missing.
/// Sister of <c>98d2e76</c> which pinned <c>??</c> in projection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereCoalesceTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WcoRow (Id INTEGER PRIMARY KEY, Score INTEGER NULL, Threshold INTEGER NOT NULL);
            -- Mix of explicit nulls and concrete values so a dropped coalesce
            -- changes the row set:
            --   Id 1: Score=NULL  -> (NULL ?? 0) > -1 -> 0 > -1 = true   (match)
            --   Id 2: Score=10    -> (10 ?? 0) > -1   -> 10 > -1 = true  (match)
            --   Id 3: Score=NULL  -> (NULL ?? 100) > 50 -> 100 > 50 = true (match for second test)
            --   Id 4: Score=-5    -> -5 > -1 = false (no match)
            --   Id 5: Score=NULL  -> (NULL ?? 0) > -1 -> 0 > -1 = true   (match)
            INSERT INTO WcoRow VALUES (1, NULL, 0), (2, 10, 0), (3, NULL, 50), (4, -5, 0), (5, NULL, 0);
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
    public async Task Where_coalesce_nullable_with_default_zero_includes_null_rows_above_threshold()
    {
        // (Score ?? 0) > -1 → all rows where coalesced Score exceeds -1.
        // Matches NULL→0, 10, NULL→0, NULL→0 (Ids 1, 2, 3, 5). Id 4 (-5) excluded.
        // Silent-wrongness: if coalesce dropped, NULL > -1 is UNKNOWN → false →
        // Ids 1, 3, 5 silently excluded and only {2} returns.
        var ids = (await _ctx.Query<WcoRow>()
            .Where(p => (p.Score ?? 0) > -1)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 3, 5 }, ids);
    }

    [Fact]
    public async Task Where_coalesce_with_nondefault_substitution_changes_matching_set()
    {
        // (Score ?? 100) > 50 → NULL rows coalesce to 100 (match),
        // 10 (no match), -5 (no match) → Ids {1, 3, 5}.
        // Critical: the substitution value (100) actually affects the result, so
        // a dropped coalesce won't accidentally happen to match.
        var ids = (await _ctx.Query<WcoRow>()
            .Where(p => (p.Score ?? 100) > 50)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 3, 5 }, ids);
    }

    [Fact]
    public async Task Where_coalesce_compared_against_other_column_treats_null_as_substitute()
    {
        // (Score ?? Threshold) > 25:
        //   Id 1: NULL  -> Threshold=0   -> 0 > 25  = false
        //   Id 2: 10                      -> 10 > 25 = false
        //   Id 3: NULL  -> Threshold=50  -> 50 > 25 = true   (match)
        //   Id 4: -5                      -> -5 > 25 = false
        //   Id 5: NULL  -> Threshold=0   -> 0 > 25  = false
        var ids = (await _ctx.Query<WcoRow>()
            .Where(p => (p.Score ?? p.Threshold) > 25)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3 }, ids);
    }

    [Table("WcoRow")]
    public sealed class WcoRow
    {
        [Key] public int Id { get; set; }
        public int? Score { get; set; }
        public int Threshold { get; set; }
    }
}
