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
/// Probes the parameter-collision pattern in <c>ExpressionToSqlVisitor</c>
/// around lines 1245 and 1287: when a grouping aggregate's inner
/// selector/predicate is translated, the sub-<c>VisitorContext</c> currently
/// does not pass <c>paramIndexStart</c> so the inner visitor's pooled
/// <c>_paramIndex</c> resets to 0 -- which means the outer HAVING/Where
/// predicate's <c>@p0</c> and the inner aggregate selector's <c>@p0</c>
/// emit the same parameter name and the inner value silently overwrites
/// the outer in <c>_params</c>.
///
/// Same root-cause shape as f2fcfb8 (HandleAllOperation) and memory #51
/// (compiled-query). If reproducible, the fix is the same: pass
/// <c>_params.Count</c> as <c>paramIndexStart</c>.
///
/// The test below exercises <c>GroupBy(...).Where(g =&gt; g.Count(pred) &gt; outer)</c>
/// -- both predicates capture an int constant so they both want <c>@p0</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupingAggregatePredicateParamCollisionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GcpItem (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Amount INTEGER NOT NULL);
            -- Cat A: amounts {1, 6, 7}    -- 2 rows with Amount > 5
            -- Cat B: amounts {2, 3, 4, 8} -- 1 row with Amount > 5
            -- Cat C: amounts {9, 10, 11}  -- 3 rows with Amount > 5
            INSERT INTO GcpItem VALUES
                (1, 'A', 1), (2, 'A', 6), (3, 'A', 7),
                (4, 'B', 2), (5, 'B', 3), (6, 'B', 4), (7, 'B', 8),
                (8, 'C', 9), (9, 'C', 10), (10, 'C', 11);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GcpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_then_having_on_count_predicate_returns_only_groups_satisfying_both_thresholds()
    {
        // For each category compute Count(Amount > 5) and keep groups where
        // that count > 1. Expected: A (count=2) and C (count=3); B has count=1.
        // Silent-wrongness if the inner @p0 collides with outer @p0: the
        // single parameter would be bound to 1 (the last @p0 written) and
        // both predicates would read it -> HAVING COUNT(Amount > 1) > 1.
        // Every category would then qualify (all have count>=3 of Amount>1).
        var result = await _ctx.Query<GcpItem>()
            .GroupBy(i => i.Category)
            .Where(g => g.Count(x => x.Amount > 5) > 1)
            .Select(g => g.Key)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Contains("A", result);
        Assert.Contains("C", result);
        Assert.DoesNotContain("B", result);
    }

    [Fact]
    public async Task GroupBy_then_having_on_sum_selector_returns_only_groups_satisfying_threshold()
    {
        // SUM(CASE WHEN Amount > 5 THEN 1 ELSE 0 END) > 2 -> only C qualifies (count=3).
        // Mirror probe for the Sum/Avg/Min/Max path at line 1287.
        var result = await _ctx.Query<GcpItem>()
            .GroupBy(i => i.Category)
            .Where(g => g.Sum(x => x.Amount > 5 ? 1 : 0) > 2)
            .Select(g => g.Key)
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("C", result[0]);
    }

    [Table("GcpItem")]
    public sealed class GcpItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
