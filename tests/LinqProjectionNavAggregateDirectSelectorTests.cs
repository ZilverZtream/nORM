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
/// Pins the direct selector overload of a nav-collection aggregate in a
/// projection: <c>Select(p =&gt; new { Total = p.Items.Sum(i =&gt; i.Amount) })</c>.
/// SCV already handles the equivalent <c>p.Items.Select(i =&gt; i.Amount).Sum()</c>
/// shape (efba58f), but the direct 2-arg form is the more natural EF-style
/// idiom and was surfaced in 2bffa3f as an untested gap. Silent-wrongness
/// risk if the selector lambda is dropped and SUM falls back to COUNT(*)
/// or the bare child table.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionNavAggregateDirectSelectorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PndParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PndChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO PndParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            -- Parent 1: 10 + 20 + 30 = 60; max=30, min=10, avg=20
            -- Parent 2: 5;                  max=5,  min=5,  avg=5
            -- Parent 3: (no children) -> default 0 / NULL semantics
            INSERT INTO PndChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PndParent>().HasKey(p => p.Id);
                mb.Entity<PndChild>().HasKey(c => c.Id);
                mb.Entity<PndParent>()
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
    public async Task Projection_nav_sum_with_direct_selector_returns_per_parent_total()
    {
        var rows = (await _ctx.Query<PndParent>()
            .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
            .ToListAsync())
            .OrderBy(r => r.Id)
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(new[] { (1, 60), (2, 5), (3, 0) }, rows);
    }

    [Fact]
    public async Task Projection_nav_sum_with_indirect_select_then_sum_still_works_as_baseline()
    {
        // Sanity check: the existing 1-arg Sum() over Items.Select(...) path
        // (efba58f) should still work on the same data. If THIS fails, the
        // test setup is broken; if only the direct-selector test above
        // fails, the new code path I added is the regression.
        var rows = (await _ctx.Query<PndParent>()
            .Select(p => new { p.Id, Total = p.Items!.Select(i => i.Amount).Sum() })
            .ToListAsync())
            .OrderBy(r => r.Id)
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(new[] { (1, 60), (2, 5), (3, 0) }, rows);
    }

    [Table("PndParent")]
    public sealed class PndParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<PndChild>? Items { get; set; }
    }

    [Table("PndChild")]
    public sealed class PndChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
