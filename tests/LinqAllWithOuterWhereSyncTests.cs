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
/// Pins the fix for HandleAllOperation when an outer Where leaves
/// predicates in <c>_where</c>. Before the fix, a synchronous
/// <c>q.Where(p1).All(p2)</c> crashed with "no such column: T0.x"
/// because the outer Where populated <c>_where</c> and Build()
/// appended <c>WHERE T0.x = ...</c> AFTER the
/// <c>SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM tbl T0 WHERE NOT (p2)) ...</c>
/// expression -- but the outer SELECT has no FROM clause, so the
/// alias was undefined. The async path ef5f1fa sidesteps this via
/// <c>!Any(!p)</c> lowering; this test exercises the direct sync
/// <c>All</c> translator path to keep both honest.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAllWithOuterWhereSyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AllSyncItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
            INSERT INTO AllSyncItem VALUES (1, 10), (2, 20), (3, 30), (4, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AllSyncItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public void All_after_outer_where_returns_true_when_filtered_subset_all_satisfies()
    {
        // Outer Where narrows to {30, 40}; All(Amount > 25) is true on that subset.
        // Pre-fix: SQLite Error 1 "no such column: T0.Amount".
        var all = _ctx.Query<AllSyncItem>()
            .Where(i => i.Amount >= 30)
            .All(i => i.Amount > 25);
        Assert.True(all);
    }

    [Fact]
    public void All_after_outer_where_returns_false_when_one_row_in_subset_violates()
    {
        // Outer Where keeps {20, 30, 40}; All(Amount > 25) violates for Amount=20.
        // Silent-wrongness probe: before the paramIndexStart fix, both predicates
        // emitted @p0; the inner value (25) overwrote the outer (20) so the WHERE
        // became `Amount <= 25 AND Amount >= 25` -- matched no row -> NOT EXISTS
        // returned true -> All returned true. After the fix, parameters are
        // @p0/@p1 and the query returns false correctly.
        var all = _ctx.Query<AllSyncItem>()
            .Where(i => i.Amount >= 20)
            .All(i => i.Amount > 25);
        Assert.False(all);
    }

    [Fact]
    public void All_without_outer_where_returns_true_when_every_row_satisfies()
    {
        // Baseline -- the no-outer-Where path was always working; this pin makes
        // sure the fix doesn't regress it.
        var all = _ctx.Query<AllSyncItem>().All(i => i.Amount > 0);
        Assert.True(all);
    }

    [Fact]
    public void All_after_two_outer_wheres_chained_returns_true_when_intersection_satisfies()
    {
        // Two chained Wheres -- _where ends with "T0.Amount >= 20 AND T0.Amount <= 40".
        // The fold-into-subquery fix must capture BOTH conjuncts.
        var all = _ctx.Query<AllSyncItem>()
            .Where(i => i.Amount >= 20)
            .Where(i => i.Amount <= 40)
            .All(i => i.Amount > 5);
        Assert.True(all);
    }

    [Table("AllSyncItem")]
    public sealed class AllSyncItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
    }
}
