using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class LinqCompositeNavigationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LcnOrder (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                Customer TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId)
            );
            CREATE TABLE LcnLine (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                LineNo INTEGER NOT NULL,
                Amount INTEGER NOT NULL,
                PRIMARY KEY (TenantId, OrderId, LineNo)
            );
            CREATE TABLE LcnReceipt (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                Code TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId)
            );
            INSERT INTO LcnOrder VALUES (1,100,'Alice'),(2,100,'Bob'),(1,101,'Cara');
            INSERT INTO LcnLine VALUES (1,100,1,10),(1,100,2,20),(2,100,1,999),(1,101,1,7);
            INSERT INTO LcnReceipt VALUES (1,100,'alice-receipt'),(2,100,'bob-receipt'),(1,101,'cara-receipt');
            """;
        await cmd.ExecuteNonQueryAsync();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<LcnOrder>().HasKey(o => new { o.TenantId, o.OrderId });
                mb.Entity<LcnLine>().HasKey(l => new { l.TenantId, l.OrderId, l.LineNo });
                mb.Entity<LcnReceipt>().HasKey(r => new { r.TenantId, r.OrderId });
                mb.Entity<LcnOrder>()
                    .HasMany(o => o.Lines)
                    .WithOne()
                    .HasForeignKey(l => new { l.TenantId, l.OrderId });
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Composite_navigation_count_projection_uses_all_key_columns()
    {
        var rows = (await _ctx.Query<LcnOrder>()
            .Select(o => new { o.TenantId, o.OrderId, Count = o.Lines.Count() })
            .ToListAsync())
            .OrderBy(r => r.TenantId)
            .ThenBy(r => r.OrderId)
            .ToArray();

        Assert.Equal(2, rows.Single(r => r.TenantId == 1 && r.OrderId == 100).Count);
        Assert.Equal(1, rows.Single(r => r.TenantId == 2 && r.OrderId == 100).Count);
        Assert.Equal(1, rows.Single(r => r.TenantId == 1 && r.OrderId == 101).Count);
    }

    [Fact]
    public async Task Composite_navigation_any_predicate_uses_all_key_columns()
    {
        var customers = (await _ctx.Query<LcnOrder>()
            .Where(o => o.Lines.Any(l => l.Amount == 999))
            .Select(o => o.Customer)
            .ToListAsync())
            .OrderBy(x => x)
            .ToArray();

        var customer = Assert.Single(customers);
        Assert.Equal("Bob", customer);
    }

    [Fact]
    public async Task Composite_navigation_sum_projection_uses_all_key_columns()
    {
        var rows = (await _ctx.Query<LcnOrder>()
            .Select(o => new { o.Customer, Total = o.Lines.Select(l => l.Amount).Sum() })
            .ToListAsync())
            .OrderBy(r => r.Customer)
            .ToArray();

        Assert.Equal(30, rows.Single(r => r.Customer == "Alice").Total);
        Assert.Equal(999, rows.Single(r => r.Customer == "Bob").Total);
        Assert.Equal(7, rows.Single(r => r.Customer == "Cara").Total);
    }

    [Fact]
    public async Task Composite_navigation_selectmany_uses_all_key_columns()
    {
        var rows = (await _ctx.Query<LcnOrder>()
            .SelectMany(o => o.Lines, (o, l) => new { o.Customer, l.Amount })
            .ToListAsync())
            .OrderBy(r => r.Customer)
            .ThenBy(r => r.Amount)
            .ToArray();

        Assert.Equal(new[] { 10, 20 }, rows.Where(r => r.Customer == "Alice").Select(r => r.Amount).ToArray());
        Assert.Equal(new[] { 999 }, rows.Where(r => r.Customer == "Bob").Select(r => r.Amount).ToArray());
        Assert.Equal(new[] { 7 }, rows.Where(r => r.Customer == "Cara").Select(r => r.Amount).ToArray());
    }

    [Fact]
    public async Task Batched_navigation_loader_uses_all_composite_key_columns()
    {
        var orders = await _ctx.Query<LcnOrder>().ToListAsync();
        using var loader = new BatchedNavigationLoader(_ctx);

        var alice = orders.Single(o => o.TenantId == 1 && o.OrderId == 100);
        var bob = orders.Single(o => o.TenantId == 2 && o.OrderId == 100);

        var aliceTask = loader.LoadNavigationAsync(alice, nameof(LcnOrder.Lines));
        var bobTask = loader.LoadNavigationAsync(bob, nameof(LcnOrder.Lines));
        var loaded = await Task.WhenAll(aliceTask, bobTask);

        Assert.Equal(new[] { 10, 20 }, loaded[0].Cast<LcnLine>().Select(l => l.Amount).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 999 }, loaded[1].Cast<LcnLine>().Select(l => l.Amount).ToArray());
    }

    [Fact]
    public async Task Split_dependent_query_stitches_composite_navigation_by_all_key_columns()
    {
        var rows = (await _ctx.Query<LcnOrder>()
            .Select(o => new LcnOrder
            {
                TenantId = o.TenantId,
                OrderId = o.OrderId,
                Customer = o.Customer,
                Lines = o.Lines
            })
            .ToListAsync())
            .OrderBy(r => r.TenantId)
            .ThenBy(r => r.OrderId)
            .ToArray();

        var alice = rows.Single(r => r.TenantId == 1 && r.OrderId == 100);
        var bob = rows.Single(r => r.TenantId == 2 && r.OrderId == 100);

        Assert.Equal(new[] { 10, 20 }, alice.Lines.Select(l => l.Amount).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 999 }, bob.Lines.Select(l => l.Amount).ToArray());
    }

    [Fact]
    public async Task Explicit_reference_load_uses_all_composite_key_columns()
    {
        var orderMap = _ctx.GetMapping(typeof(LcnOrder));
        var receiptMap = _ctx.GetMapping(typeof(LcnReceipt));
        var navProp = typeof(LcnOrder).GetProperty(nameof(LcnOrder.Receipt))!;
        var principalKeys = orderMap.Columns
            .Where(c => c.PropName is nameof(LcnOrder.TenantId) or nameof(LcnOrder.OrderId))
            .OrderBy(c => c.PropName == nameof(LcnOrder.TenantId) ? 0 : 1)
            .ToArray();
        var foreignKeys = receiptMap.Columns
            .Where(c => c.PropName is nameof(LcnReceipt.TenantId) or nameof(LcnReceipt.OrderId))
            .OrderBy(c => c.PropName == nameof(LcnReceipt.TenantId) ? 0 : 1)
            .ToArray();
        orderMap.Relations[navProp.Name] = new TableMapping.Relation(navProp, typeof(LcnReceipt), principalKeys, foreignKeys);

        var bob = new LcnOrder { TenantId = 2, OrderId = 100, Customer = "Bob" };
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(bob, new NavigationContext(_ctx, typeof(LcnOrder)));

        await bob.LoadAsync(o => o.Receipt);

        Assert.NotNull(bob.Receipt);
        Assert.Equal("bob-receipt", bob.Receipt!.Code);
    }

    [Table("LcnOrder")]
    public sealed class LcnOrder
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Customer { get; set; } = string.Empty;
        public List<LcnLine> Lines { get; set; } = new();
        [NotMapped]
        public LcnReceipt? Receipt { get; set; }
    }

    [Table("LcnLine")]
    public sealed class LcnLine
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public int LineNo { get; set; }
        public int Amount { get; set; }
    }

    [Table("LcnReceipt")]
    public sealed class LcnReceipt
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
