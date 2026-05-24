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
/// Sister of d6b58bb (OrderBy by PROJECTED nav-aggregate). Direct shape with
/// no anonymous-projection inlining: <c>OrderByDescending(p =&gt; p.Items.Sum(i =&gt; i.Amount))</c>
/// on the entity itself. The d6b58bb fix matches both shapes because the
/// detection runs on <c>keySelector.Body</c> after <c>ExpandProjection</c>,
/// and that body looks identical for the projected and direct forms once
/// the alias is inlined. Confirms scope and locks in the direct-call path
/// independent of the projected-path test.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByDirectNavAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OdnParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE OdnChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO OdnParent VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
            -- Sums: 1->60, 2->5, 3->0, 4->100. Counts: 1->3, 2->1, 3->0, 4->1.
            INSERT INTO OdnChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 4, 100);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<OdnParent>().HasKey(p => p.Id);
                mb.Entity<OdnChild>().HasKey(c => c.Id);
                mb.Entity<OdnParent>()
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
    public async Task OrderBy_direct_nav_sum_with_selector_orders_parents_by_total()
    {
        // Ascending by sum: 3(0), 2(5), 1(60), 4(100) -> Ids [3, 2, 1, 4].
        var ids = (await _ctx.Query<OdnParent>()
            .OrderBy(p => p.Items!.Sum(i => i.Amount))
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 2, 1, 4 }, ids);
    }

    [Fact]
    public async Task OrderByDescending_direct_nav_max_with_selector_orders_parents_by_max()
    {
        // Per-parent Max(Amount): 1->30, 2->5, 3->NULL/empty, 4->100.
        // DESC -> 4(100), 1(30), 2(5), 3(NULL). SQLite places NULL last
        // for ORDER BY DESC by default; assert the non-null ranking up to
        // position 3.
        var firstThree = (await _ctx.Query<OdnParent>()
            .OrderByDescending(p => p.Items!.Max(i => i.Amount))
            .ToListAsync())
            .Select(r => r.Id)
            .Take(3)
            .ToArray();
        Assert.Equal(new[] { 4, 1, 2 }, firstThree);
    }

    [Fact]
    public async Task OrderBy_direct_nav_count_orders_parents_by_child_count()
    {
        // Child counts: 1->3, 2->1, 3->0, 4->1. ASC -> 3(0), then any of {2, 4}
        // (both 1), then 1(3). Pin only the deterministic endpoints.
        var ids = (await _ctx.Query<OdnParent>()
            .OrderBy(p => p.Items!.Count())
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(4, ids.Length);
        Assert.Equal(3, ids[0]);  // 0 children
        Assert.Equal(1, ids[^1]); // 3 children
    }

    [Table("OdnParent")]
    public sealed class OdnParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<OdnChild>? Items { get; set; }
    }

    [Table("OdnChild")]
    public sealed class OdnChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
