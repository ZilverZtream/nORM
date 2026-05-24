using System.Collections.Generic;
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
/// Second-order regression check for b2df7e1: when WHERE filter, OrderBy,
/// AND a nav-aggregate projection all stack on the same query, the alias
/// passed to SelectClauseVisitor must still match the outer FROM clause's
/// alias. WHERE creates its own correlated-param scope; if that interaction
/// with OrderBy's alias assignment yields a different alias than the one
/// finally rendered into FROM, the nav-aggregate subquery binds against
/// the wrong outer reference and SQLite throws "no such column".
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereOrderByThenNavAggregateSelectTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WonParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsActive INTEGER NOT NULL);
            CREATE TABLE WonChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO WonParent VALUES (1, 'A', 1), (2, 'B', 0), (3, 'C', 1), (4, 'D', 1);
            -- Parent 1 -> 60, Parent 2 -> 99 (filtered out), Parent 3 -> 5, Parent 4 -> 0 (no children).
            INSERT INTO WonChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 99), (14, 3, 5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<WonParent>().HasKey(p => p.Id);
                mb.Entity<WonChild>().HasKey(c => c.Id);
                mb.Entity<WonParent>()
                    .HasMany(p => p.Items!)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_orderby_then_select_nav_sum_correlates_across_full_query_shape()
    {
        // Filter to active parents (Ids 1, 3, 4) -> ordered by Id ASC ->
        // project with per-parent Sum. Stress test for the b2df7e1 alias
        // fix under the realistic Where+OrderBy combination.
        var rows = (await _ctx.Query<WonParent>()
            .Where(p => p.IsActive)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
            .ToListAsync())
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(new[] { (1, 60), (3, 5), (4, 0) }, rows);
    }

    [Fact]
    public async Task Where_orderbydescending_then_select_nav_count_correlates_across_full_query_shape()
    {
        // Same shape with Count() instead of Sum + DESC instead of ASC.
        // Exercises EmitNavigationCountSubquery (different emit path than
        // EmitNavigationScalarAggregateSubquery) under the same Where+OrderBy
        // combination.
        var rows = (await _ctx.Query<WonParent>()
            .Where(p => p.IsActive)
            .OrderByDescending(p => p.Id)
            .Select(p => new { p.Id, ChildCount = p.Items!.Count() })
            .ToListAsync())
            .Select(r => (r.Id, r.ChildCount))
            .ToArray();
        Assert.Equal(new[] { (4, 0), (3, 1), (1, 3) }, rows);
    }

    [Table("WonParent")]
    public sealed class WonParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsActive { get; set; }
        public List<WonChild>? Items { get; set; }
    }

    [Table("WonChild")]
    public sealed class WonChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
