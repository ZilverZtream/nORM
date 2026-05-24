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
/// Second-order regression for the d6b58bb OrderBy-by-projected-nav-aggregate
/// fix: stack Where(filter) + Select(new {Id, Total = Sum(...)}) +
/// OrderByDescending(r => r.Total) + Skip(n) + Take(m). This is the realistic
/// dashboard shape ("top 10 active parents by total amount, page N").
/// Each of these operations independently touches alias plumbing
/// (b2df7e1, cc28dcd, d6b58bb, 161d9fb); stacking them all is the most likely
/// place for a second-order alias collision to surface.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereOrderByNavAggregateSkipTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WonsParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsActive INTEGER NOT NULL);
            CREATE TABLE WonsChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            -- Active parents 1, 3, 4, 5; inactive 2, 6.
            INSERT INTO WonsParent VALUES
              (1, 'A', 1), (2, 'B', 0), (3, 'C', 1),
              (4, 'D', 1), (5, 'E', 1), (6, 'F', 0);
            -- Totals: 1->60, 2->999 (inactive, filtered out), 3->5, 4->100, 5->0, 6->50 (filtered).
            INSERT INTO WonsChild VALUES
              (10, 1, 10), (11, 1, 20), (12, 1, 30),
              (13, 2, 999),
              (14, 3, 5),
              (15, 4, 100),
              (16, 6, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<WonsParent>().HasKey(p => p.Id);
                mb.Entity<WonsChild>().HasKey(c => c.Id);
                mb.Entity<WonsParent>()
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
    public async Task Where_active_then_select_then_orderby_total_descending_then_skip_take_pages_correctly()
    {
        // Active parents with totals: 1->60, 3->5, 4->100, 5->0.
        // DESC by Total -> 4(100), 1(60), 3(5), 5(0).
        // Skip(1).Take(2) -> { 1, 3 }.
        var rows = (await _ctx.Query<WonsParent>()
            .Where(p => p.IsActive)
            .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
            .OrderByDescending(r => r.Total)
            .Skip(1)
            .Take(2)
            .ToListAsync())
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(new[] { (1, 60), (3, 5) }, rows);
    }

    [Fact]
    public async Task Where_active_then_orderby_direct_nav_sum_descending_then_take_pages_correctly()
    {
        // Same shape without projection alias -- exercises the direct
        // OrderBy(p.Items.Sum(...)) path from ecf53fd composed with WHERE
        // and Take.
        // Active parents by sum DESC: 4(100), 1(60), 3(5), 5(0). Take(2) -> {4, 1}.
        var ids = (await _ctx.Query<WonsParent>()
            .Where(p => p.IsActive)
            .OrderByDescending(p => p.Items!.Sum(i => i.Amount))
            .Take(2)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4, 1 }, ids);
    }

    [Table("WonsParent")]
    public sealed class WonsParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsActive { get; set; }
        public List<WonsChild>? Items { get; set; }
    }

    [Table("WonsChild")]
    public sealed class WonsChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
