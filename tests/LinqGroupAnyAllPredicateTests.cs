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
/// Pins <c>g.Any(predicate)</c> and <c>g.All(predicate)</c> inside a grouped
/// projection — the natural follow-on to the 9ef6d67 Count(predicate) fix.
/// Both expand to existential-style aggregates in SQL: Any → MAX(CASE WHEN
/// pred THEN 1 ELSE 0 END) = 1, All → MIN(...) = 1. Without server-side
/// translation the group projection either crashes at runtime or silently
/// drops the predicate.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupAnyAllPredicateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GaaRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO GaaRow VALUES
                (1, 'NA',   90),
                (2, 'NA',   60),
                (3, 'NA',   85),
                (4, 'EMEA', 70),
                (5, 'EMEA', 55),
                (6, 'APAC', 95);
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
    public async Task GroupBy_with_any_predicate_returns_true_for_groups_with_match()
    {
        // Any(Score >= 80): NA → true (90 and 85), EMEA → false (70, 55), APAC → true (95).
        var groups = (await _ctx.Query<GaaRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, HasHigh = g.Any(r => r.Score >= 80) })
            .ToListAsync())
            .OrderBy(g => g.Region).ToArray();

        Assert.Equal(3, groups.Length);
        Assert.Equal(("APAC", true),  (groups[0].Region, groups[0].HasHigh));
        Assert.Equal(("EMEA", false), (groups[1].Region, groups[1].HasHigh));
        Assert.Equal(("NA",   true),  (groups[2].Region, groups[2].HasHigh));
    }

    [Fact]
    public async Task GroupBy_with_all_predicate_returns_true_only_when_every_row_matches()
    {
        // All(Score >= 60): NA → false (60 is exactly the boundary — wait yes 60 >= 60 → true, 90 ✓, 85 ✓ → true).
        // EMEA → false (55 < 60). APAC → true (95).
        var groups = (await _ctx.Query<GaaRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, AllAtLeast60 = g.All(r => r.Score >= 60) })
            .ToListAsync())
            .OrderBy(g => g.Region).ToArray();

        Assert.Equal(3, groups.Length);
        Assert.Equal(("APAC", true),  (groups[0].Region, groups[0].AllAtLeast60));
        Assert.Equal(("EMEA", false), (groups[1].Region, groups[1].AllAtLeast60));
        Assert.Equal(("NA",   true),  (groups[2].Region, groups[2].AllAtLeast60));
    }

    [Table("GaaRow")]
    public sealed class GaaRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
