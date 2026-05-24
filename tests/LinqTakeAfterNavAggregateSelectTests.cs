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
/// Pins paging applied after a projection that contains a nav-aggregate
/// correlated subquery. Sister of b2df7e1 (OrderBy-before-nav-Select) and
/// cc28dcd (Where+OrderBy-before-nav-Select). Take/Skip wrap the SQL with
/// LIMIT/OFFSET; if that wrapping rewrites the outer FROM alias the
/// correlated subquery binds incorrectly. Three arrangements: bare Take,
/// Skip+Take pagination, and OrderBy+Take combined.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTakeAfterNavAggregateSelectTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TanParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE TanChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO TanParent VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
            -- Parent totals: 1->60, 2->5, 3->0, 4->100, 5->0.
            INSERT INTO TanChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 4, 100);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TanParent>().HasKey(p => p.Id);
                mb.Entity<TanChild>().HasKey(c => c.Id);
                mb.Entity<TanParent>()
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
    public async Task Take_after_select_nav_sum_returns_first_n_with_correct_totals()
    {
        // Bare Take(3) -- no OrderBy. Verify nav-Sum still correlates to the
        // correct parent. Default insertion order on SQLite gives Ids 1,2,3.
        var rows = (await _ctx.Query<TanParent>()
            .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
            .Take(3)
            .ToListAsync())
            .OrderBy(r => r.Id)
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(3, rows.Length);
        // Ids will be {1, 2, 3} per SQLite insertion order; assert sorted view.
        Assert.Equal(new[] { (1, 60), (2, 5), (3, 0) }, rows);
    }

    [Fact]
    public async Task Skip_take_after_select_nav_sum_pages_with_correct_totals()
    {
        // Skip(2).Take(2) over OrderBy(Id) -- middle window with nav-Sum.
        // Hits BOTH the b2df7e1 alias path (OrderBy adds T0 alias) AND
        // the Skip+Take paging wrap.
        var rows = (await _ctx.Query<TanParent>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
            .Skip(2)
            .Take(2)
            .ToListAsync())
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(new[] { (3, 0), (4, 100) }, rows);
    }

    [Fact]
    public async Task Orderby_descending_by_projected_nav_aggregate_throws_actionable_error()
    {
        // Known gap: OrderBy by a projected nav-aggregate ExpandProjection-inlines
        // the original `p.Items.Sum(...)` into the OrderBy keySelector, but the
        // OrderBy translator can't currently re-route a nav-collection access in
        // that position -- ExpressionToSqlVisitor.VisitMember throws "Member
        // 'Items' is not supported in this context". Pin the throw so the error
        // surface is locked; the fix is a separate iteration (OrderByTranslator
        // needs to recognise the inlined nav-aggregate shape, same way SCV does).
        // Workaround: project to a sub-shape first, then OrderBy on the loaded
        // shape client-side.
        await Assert.ThrowsAsync<nORM.Core.NormUnsupportedFeatureException>(async () =>
        {
            await _ctx.Query<TanParent>()
                .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
                .OrderByDescending(r => r.Total)
                .Take(2)
                .ToListAsync();
        });
    }

    [Table("TanParent")]
    public sealed class TanParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<TanChild>? Items { get; set; }
    }

    [Table("TanChild")]
    public sealed class TanChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
