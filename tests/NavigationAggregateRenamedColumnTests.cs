using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that a projected navigation aggregate honours fluent <c>HasColumnName</c> renames on the
/// columns it references. The single-hop nav-aggregate subquery emit resolved:
/// <list type="bullet">
/// <item>the FK/PK join columns from the CLR property name (not the mapped column), and</item>
/// <item>a filter-predicate column from the <c>[Column]</c> attribute only (missing fluent renames).</item>
/// </list>
/// So a key or filter column renamed via <c>Property(x =&gt; x.P).HasColumnName("...")</c> produced
/// <c>alias.&lt;PropName&gt;</c> — a column that does not exist. Both now resolve through the
/// <see cref="nORM.Mapping.TableMapping"/> like the aggregate selector already did. The selector case
/// (renamed <c>Amount</c>) is included to confirm it stays correct.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateRenamedColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // Physical columns are ALL renamed away from their CLR property names.
        cmd.CommandText = """
            CREATE TABLE RkOrder (order_pk INTEGER PRIMARY KEY, customer TEXT NOT NULL);
            CREATE TABLE RkLine  (line_pk INTEGER PRIMARY KEY, line_order_fk INTEGER NOT NULL,
                                  line_amount INTEGER NOT NULL, line_status TEXT NOT NULL);
            INSERT INTO RkOrder VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Order 1: 3 lines (250, 2 open); Order 2: 1 line (50, 1 open); Order 3: none.
            INSERT INTO RkLine VALUES
                (1,1,100,'open'),(2,1,75,'open'),(3,1,75,'closed'),
                (4,2,50,'open');
            """;
        await cmd.ExecuteNonQueryAsync();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<RkOrder>().HasKey(o => o.Id);
                mb.Entity<RkOrder>().Property(o => o.Id).HasColumnName("order_pk");
                mb.Entity<RkOrder>().Property(o => o.Customer).HasColumnName("customer");

                mb.Entity<RkLine>().HasKey(l => l.Id);
                mb.Entity<RkLine>().Property(l => l.Id).HasColumnName("line_pk");
                mb.Entity<RkLine>().Property(l => l.OrderId).HasColumnName("line_order_fk");
                mb.Entity<RkLine>().Property(l => l.Amount).HasColumnName("line_amount");
                mb.Entity<RkLine>().Property(l => l.Status).HasColumnName("line_status");

                mb.Entity<RkOrder>().HasMany(o => o.Lines).WithOne()
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
    public async Task Projected_nav_Count_uses_renamed_key_columns()
    {
        var rows = (await _ctx.Query<RkOrder>()
                .Select(o => new { o.Id, N = o.Lines.Count() })
                .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        Assert.Equal(new[] { 3, 1, 0 }, rows.Select(r => r.N).ToArray());
    }

    [Fact]
    public async Task Projected_nav_Sum_uses_renamed_key_and_selector_columns()
    {
        var rows = (await _ctx.Query<RkOrder>()
                .Select(o => new { o.Id, S = o.Lines.Sum(l => l.Amount) })
                .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        Assert.Equal(new[] { 250, 50, 0 }, rows.Select(r => r.S).ToArray());
    }

    [Fact]
    public async Task Filtered_nav_Count_uses_renamed_filter_column()
    {
        var rows = (await _ctx.Query<RkOrder>()
                .Select(o => new { o.Id, N = o.Lines.Count(l => l.Status == "open") })
                .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        Assert.Equal(new[] { 2, 1, 0 }, rows.Select(r => r.N).ToArray());
    }

    public sealed class RkOrder
    {
        [Key] public int Id { get; set; }
        public string Customer { get; set; } = string.Empty;
        public ICollection<RkLine> Lines { get; set; } = new List<RkLine>();
    }

    public sealed class RkLine
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int Amount { get; set; }
        public string Status { get; set; } = string.Empty;
    }
}
