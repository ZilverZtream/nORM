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
/// Pins <c>.OrderBy(p =&gt; p.Id).Select(p =&gt; new { Total = p.Items.Sum(...) })</c>
/// — OrderBy chained before a projection that contains a navigation-aggregate
/// subquery. Discovered in 5d1da71: the correlated subquery hardcodes the
/// outer reference as <c>EscTable.PkCol</c>, but when OrderBy participates the
/// outer SELECT uses a JOIN alias (T0/T1/etc.) instead of the unaliased table
/// name, so SQLite reports "no such column: ParentTable.Id".
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByBeforeNavAggregateSelectTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObnsParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE ObnsChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO ObnsParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            INSERT INTO ObnsChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5);
            -- Parent 1 -> 60, Parent 2 -> 5, Parent 3 -> 0/NULL.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ObnsParent>().HasKey(p => p.Id);
                mb.Entity<ObnsChild>().HasKey(c => c.Id);
                mb.Entity<ObnsParent>()
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
    public async Task OrderBy_then_select_nav_sum_correlates_to_outer_table_alias()
    {
        // The .OrderBy(p => p.Id) chained before the nav-aggregate Select must
        // not break the correlated subquery: the outer reference inside the
        // subquery still needs to bind to the parent row's PK regardless of
        // whether the outer SELECT uses the bare table name or a JOIN alias.
        var rows = (await _ctx.Query<ObnsParent>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Total = p.Items!.Sum(i => i.Amount) })
            .ToListAsync())
            .Select(r => (r.Id, r.Total))
            .ToArray();
        Assert.Equal(new[] { (1, 60), (2, 5), (3, 0) }, rows);
    }

    [Fact]
    public async Task OrderByDescending_then_select_nav_count_correlates_to_outer_table_alias()
    {
        // Same shape via the bare Count() form -- exercises a different
        // emit path (EmitNavigationCountSubquery vs the scalar-aggregate one).
        var rows = (await _ctx.Query<ObnsParent>()
            .OrderByDescending(p => p.Id)
            .Select(p => new { p.Id, ChildCount = p.Items!.Count() })
            .ToListAsync())
            .Select(r => (r.Id, r.ChildCount))
            .ToArray();
        Assert.Equal(new[] { (3, 0), (2, 1), (1, 3) }, rows);
    }

    [Table("ObnsParent")]
    public sealed class ObnsParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<ObnsChild>? Items { get; set; }
    }

    [Table("ObnsChild")]
    public sealed class ObnsChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
