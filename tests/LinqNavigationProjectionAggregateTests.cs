using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of navigation aggregates whose source is a
/// projection over the child collection — e.g.
/// <c>Where(p =&gt; p.Lines.Select(l =&gt; l.Total).Sum() &gt; 100)</c>. The
/// shape lowers to a correlated subquery
/// <c>(SELECT SUM(Total) FROM Line WHERE Line.ParentId = Parent.Id) &gt; @p0</c>.
/// Without server translation the predicate runs client-side and pulls every
/// parent row plus its children — a common N+1 pitfall.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationProjectionAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NpaOrder (Id INTEGER PRIMARY KEY, Customer TEXT NOT NULL);
            CREATE TABLE NpaLine  (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO NpaOrder VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Order 1 totals 250 (3 lines), Order 2 totals 50 (1 line), Order 3 totals 0 (no lines)
            INSERT INTO NpaLine VALUES
                (1, 1, 100),
                (2, 1, 75),
                (3, 1, 75),
                (4, 2, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NpaOrder>().HasKey(o => o.Id);
                mb.Entity<NpaLine>().HasKey(l => l.Id);
                mb.Entity<NpaOrder>().HasMany(o => o.Lines).WithOne()
                    .HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Sum_over_navigation_projection_filters_parents_with_large_totals()
    {
        // Orders whose line-amount sum is > 100. Order 1 sums to 250, others don't qualify.
        var rows = (await _ctx.Query<NpaOrder>()
            .Where(o => o.Lines.Select(l => l.Amount).Sum() > 100)
            .ToListAsync())
            .OrderBy(o => o.Id).ToArray();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Table("NpaOrder")]
    public sealed class NpaOrder
    {
        [Key] public int Id { get; set; }
        public string Customer { get; set; } = string.Empty;
        public ICollection<NpaLine> Lines { get; set; } = new List<NpaLine>();
    }

    [Table("NpaLine")]
    public sealed class NpaLine
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int Amount { get; set; }
        public NpaOrder Order { get; set; } = null!;
    }
}
