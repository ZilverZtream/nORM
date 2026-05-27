using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// ExecuteDeleteAsync and ExecuteUpdateAsync on entities with composite primary keys,
/// both with simple WHERE predicates and with join-source queries.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompositePkExecuteCudTests : IAsyncLifetime
{
    [Table("CpkOrder")]
    public sealed class CpkOrder
    {
        public int StoreId { get; set; }
        public int OrderId { get; set; }
        public string Status { get; set; } = string.Empty;
        public bool Archived { get; set; }
    }

    [Table("CpkStore")]
    public sealed class CpkStore
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
    }

    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CpkStore (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE CpkOrder (StoreId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Status TEXT NOT NULL, Archived INTEGER NOT NULL DEFAULT 0,
                                   PRIMARY KEY (StoreId, OrderId));
            INSERT INTO CpkStore VALUES (1,1),(2,0),(3,1);
            INSERT INTO CpkOrder VALUES (1,1,'open',0),(1,2,'closed',0),(2,1,'open',0),(2,2,'open',0),(3,1,'closed',0);
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CpkOrder>().HasKey(o => new { o.StoreId, o.OrderId });
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ── Simple WHERE (no join) ───────────────────────────────────────────────

    [Fact]
    public async Task ExecuteDeleteAsync_composite_pk_simple_where_deletes_matching_rows()
    {
        var deleted = await _ctx.Query<CpkOrder>()
            .Where(o => o.Status == "closed")
            .ExecuteDeleteAsync();

        var remaining = await _ctx.Query<CpkOrder>().ToListAsync();
        Assert.Equal(3, remaining.Count);
        Assert.All(remaining, o => Assert.Equal("open", o.Status));
    }

    [Fact]
    public async Task ExecuteUpdateAsync_composite_pk_simple_where_updates_matching_rows()
    {
        await _ctx.Query<CpkOrder>()
            .Where(o => o.StoreId == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Archived, true));

        var storeOneOrders = await _ctx.Query<CpkOrder>()
            .Where(o => o.StoreId == 1)
            .ToListAsync();
        Assert.All(storeOneOrders, o => Assert.True(o.Archived));

        var otherOrders = await _ctx.Query<CpkOrder>()
            .Where(o => o.StoreId != 1)
            .ToListAsync();
        Assert.All(otherOrders, o => Assert.False(o.Archived));
    }

    // ── Join source ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteDeleteAsync_composite_pk_join_source_deletes_correctly()
    {
        // Delete orders from inactive stores (StoreId=2 is inactive).
        var deleted = await _ctx.Query<CpkOrder>()
            .Join(_ctx.Query<CpkStore>().Where(s => !s.Active),
                  o => o.StoreId, s => s.Id,
                  (o, s) => o)
            .ExecuteDeleteAsync();

        var remaining = await _ctx.Query<CpkOrder>().ToListAsync();
        // 5 orders total, 2 from store 2 → 3 remaining
        Assert.Equal(3, remaining.Count);
        Assert.DoesNotContain(remaining, o => o.StoreId == 2);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_composite_pk_join_source_updates_correctly()
    {
        // Archive orders from inactive stores (StoreId=2 is inactive).
        await _ctx.Query<CpkOrder>()
            .Join(_ctx.Query<CpkStore>().Where(s => !s.Active),
                  o => o.StoreId, s => s.Id,
                  (o, s) => o)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Archived, true));

        var store2Orders = await _ctx.Query<CpkOrder>()
            .Where(o => o.StoreId == 2)
            .ToListAsync();
        Assert.All(store2Orders, o => Assert.True(o.Archived));

        var otherOrders = await _ctx.Query<CpkOrder>()
            .Where(o => o.StoreId != 2)
            .ToListAsync();
        Assert.All(otherOrders, o => Assert.False(o.Archived));
    }

    [Fact]
    public async Task ExecuteDeleteAsync_composite_pk_correlated_any_deletes_correctly()
    {
        // Correlated ANY approach (the documented workaround, also verifies it still works).
        await _ctx.Query<CpkOrder>()
            .Where(o => _ctx.Query<CpkStore>().Any(s => s.Id == o.StoreId && !s.Active))
            .ExecuteDeleteAsync();

        var remaining = await _ctx.Query<CpkOrder>().ToListAsync();
        Assert.Equal(3, remaining.Count);
        Assert.DoesNotContain(remaining, o => o.StoreId == 2);
    }
}
