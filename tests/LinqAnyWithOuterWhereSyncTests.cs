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
/// Probe for the param-collision pattern in <c>HandleSetOperation</c>.
/// Audit hypothesis: <c>Any</c> goes through
/// <c>SetPredicateTranslator -&gt; HandleSetOperation -&gt; TranslateInSubContext</c>,
/// which pushes a fresh translator context and sets
/// <c>_parameterManager.Index = parameterIndex</c> from the outer.
/// That path should NOT have the f2fcfb8 collision -- but verify with
/// a multi-constant-capture case where the bug would surface as
/// silent wrongness rather than a crash.
///
/// Pre-fix shape (HandleAllOperation, f2fcfb8): outer Where captures
/// <c>@p0</c>, inner aggregate predicate captures <c>@p0</c> too,
/// inner overwrites outer, the actual-effective predicate uses the
/// wrong threshold and silently changes the result.
///
/// Expected outcome here: all assertions pass (audit hypothesis holds).
/// If any assertion fails, the same fix pattern as f2fcfb8 applies.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAnyWithOuterWhereSyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AnySyncItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
            INSERT INTO AnySyncItem VALUES (1, 10), (2, 20), (3, 30), (4, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AnySyncItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public void Any_after_outer_where_returns_true_when_filtered_subset_satisfies()
    {
        // Outer Where keeps {20, 30, 40}; Any(Amount > 25) -> true (30 and 40 match).
        // If params collided: outer's 20 overwritten by inner's 25 -> the SQL
        // becomes EXISTS(Amount >= 25 AND Amount > 25) = EXISTS({30, 40, 25-and-up})
        // which still returns true here, so this test alone doesn't prove safety;
        // pair with the false-case below for full coverage.
        var any = _ctx.Query<AnySyncItem>()
            .Where(i => i.Amount >= 20)
            .Any(i => i.Amount > 25);
        Assert.True(any);
    }

    [Fact]
    public void Any_after_outer_where_returns_true_when_singleton_matches_inner()
    {
        // Outer keeps only Id=1 (Amount=10); inner 10 > 5 -> true.
        var any = _ctx.Query<AnySyncItem>()
            .Where(i => i.Amount <= 15)
            .Any(i => i.Amount > 5);
        Assert.True(any);
    }

    [Fact]
    public void Any_after_outer_where_with_disjoint_predicates_returns_false()
    {
        // Outer keeps {30, 40}; inner Amount < 10 never matches -> false.
        var any = _ctx.Query<AnySyncItem>()
            .Where(i => i.Amount >= 30)
            .Any(i => i.Amount < 10);
        Assert.False(any);
    }

    [Fact]
    public void Any_after_outer_where_definitive_collision_probe()
    {
        // Outer keeps {10, 20, 30} (excludes 40); inner Amount == 40 matches none
        // of {10, 20, 30} -> false. Definitive probe: if outer's @p0=30 collided
        // and was overwritten by inner's @p0=40, outer would become "Amount <= 40"
        // -> keeps {10, 20, 30, 40} -> inner Amount == 40 -> Id=4 matches -> TRUE.
        // So if this assertion fails (true returned), the collision bug exists.
        var any = _ctx.Query<AnySyncItem>()
            .Where(i => i.Amount <= 30)
            .Any(i => i.Amount == 40);
        Assert.False(any);
    }

    [Table("AnySyncItem")]
    public sealed class AnySyncItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
    }
}
