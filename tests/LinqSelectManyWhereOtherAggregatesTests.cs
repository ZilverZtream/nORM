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
/// Sister probe of e438a53 (which fixed SelectMany+Where+CountAsync). My
/// fix was deliberately gated on <c>_methodName is "Count" or "LongCount"</c>
/// to avoid clobbering Sum's working path, so the remaining terminal
/// aggregates over a SelectMany source (Min / Max / Average / Any / All)
/// could plausibly have the same SQL-emit bug. Source SQL after
/// HandleSelectMany is <c>SELECT &lt;cols&gt; FROM Parent JOIN Child ...</c>
/// and the aggregate prefix is gated by <c>if (_sql.Length == 0)</c> in
/// Generate so any aggregate that doesn't have its own rewrite would
/// silently read the first matching row's first column instead of
/// computing the scalar.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyWhereOtherAggregatesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmoParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmoChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO SmoParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            -- Children:
            --   P1: 10, 20, 30
            --   P2: 5, 100
            --   P3: (none)
            -- Items > 10: {20, 30, 100} -> 3 rows
            -- Min(those) = 20; Max = 100; Avg = 50
            INSERT INTO SmoChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 2, 100);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmoParent>().HasKey(p => p.Id);
                mb.Entity<SmoChild>().HasKey(c => c.Id);
                mb.Entity<SmoParent>()
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
    public async Task SelectMany_then_where_then_min_returns_correct_min()
    {
        var min = await _ctx.Query<SmoParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .MinAsync(i => i.Amount);
        Assert.Equal(20, min);
    }

    [Fact]
    public async Task SelectMany_then_where_then_max_returns_correct_max()
    {
        var max = await _ctx.Query<SmoParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .MaxAsync(i => i.Amount);
        Assert.Equal(100, max);
    }

    [Fact]
    public async Task SelectMany_then_where_then_average_returns_correct_average()
    {
        var avg = await _ctx.Query<SmoParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .AverageAsync(i => (double)i.Amount);
        Assert.Equal(50.0, avg);
    }

    [Fact]
    public async Task SelectMany_then_where_then_any_returns_true_when_matches_exist()
    {
        var any = await _ctx.Query<SmoParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .AnyAsync();
        Assert.True(any);
    }

    [Fact]
    public async Task SelectMany_then_where_then_any_returns_false_when_no_matches()
    {
        // Threshold above all child amounts.
        var any = await _ctx.Query<SmoParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 1000)
            .AnyAsync();
        Assert.False(any);
    }

    [Table("SmoParent")]
    public sealed class SmoParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmoChild>? Items { get; set; }
    }

    [Table("SmoChild")]
    public sealed class SmoChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
