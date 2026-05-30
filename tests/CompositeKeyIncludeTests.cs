using System;
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
/// Include on a dependent entity with a composite primary key (FK + surrogate column).
/// The FK itself is still a single column, so the standard IN-batched load works correctly.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class CompositeKeyIncludeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    private class Blog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public ICollection<OrderLine> OrderLines { get; set; } = new List<OrderLine>();
    }

    private class OrderLine
    {
        public int BlogId { get; set; }
        public int LineNumber { get; set; }
        public string Description { get; set; } = string.Empty;
        public Blog? Blog { get; set; }
    }

    [Table("CkiTenantOrder")]
    private class TenantOrder
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public string Customer { get; set; } = string.Empty;
        public ICollection<TenantOrderLine> Lines { get; set; } = new List<TenantOrderLine>();
    }

    [Table("CkiTenantOrderLine")]
    private class TenantOrderLine
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public int LineNo { get; set; }
        public string Description { get; set; } = string.Empty;
        [NotMapped]
        public TenantOrder? Order { get; set; }
        public ICollection<TenantShipment> Shipments { get; set; } = new List<TenantShipment>();
    }

    [Table("CkiTenantShipment")]
    private class TenantShipment
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public int LineNo { get; set; }
        public int ShipmentId { get; set; }
        public string Tracking { get; set; } = string.Empty;
    }

    [Table("CkiExternalOrder")]
    private class ExternalOrder
    {
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string ExternalNo { get; set; } = string.Empty;
        public string Customer { get; set; } = string.Empty;
        public ICollection<ExternalOrderEvent> Events { get; set; } = new List<ExternalOrderEvent>();
    }

    [Table("CkiExternalOrderEvent")]
    private class ExternalOrderEvent
    {
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string ExternalNo { get; set; } = string.Empty;
        public string EventName { get; set; } = string.Empty;
        [NotMapped]
        public ExternalOrder? Order { get; set; }
    }

    private static DbContextOptions BuildOptions() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<OrderLine>().HasKey(x => new { x.BlogId, x.LineNumber });
            mb.Entity<Blog>()
                .HasMany(b => b.OrderLines)
                .WithOne(o => o.Blog)
                .HasForeignKey(o => o.BlogId, b => b.Id);
            mb.Entity<TenantOrder>().HasKey(x => new { x.TenantId, x.OrderId });
            mb.Entity<TenantOrderLine>().HasKey(x => new { x.TenantId, x.OrderId, x.LineNo });
            mb.Entity<TenantShipment>().HasKey(x => new { x.TenantId, x.OrderId, x.LineNo, x.ShipmentId });
            mb.Entity<TenantOrder>()
                .HasMany(o => o.Lines)
                .WithOne(l => l.Order)
                .HasForeignKey(l => new { l.TenantId, l.OrderId }, o => new { o.TenantId, o.OrderId });
            mb.Entity<TenantOrderLine>()
                .HasMany(l => l.Shipments)
                .WithOne()
                .HasForeignKey(s => new { s.TenantId, s.OrderId, s.LineNo }, l => new { l.TenantId, l.OrderId, l.LineNo });
            mb.Entity<ExternalOrder>().HasKey(x => x.Id);
            mb.Entity<ExternalOrderEvent>().HasKey(x => x.Id);
            mb.Entity<ExternalOrder>()
                .HasMany(o => o.Events)
                .WithOne(e => e.Order)
                .HasForeignKey(e => new { e.TenantId, e.ExternalNo }, o => new { o.TenantId, o.ExternalNo });
        }
    };

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Blog     (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
            CREATE TABLE OrderLine(BlogId INTEGER NOT NULL, LineNumber INTEGER NOT NULL, Description TEXT NOT NULL, PRIMARY KEY(BlogId, LineNumber));
            INSERT INTO Blog(Title) VALUES ('Alpha'), ('Beta'), ('Gamma');
            INSERT INTO OrderLine VALUES (1,1,'a-line1'),(1,2,'a-line2'),(2,1,'b-line1');
            CREATE TABLE CkiTenantOrder(TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, Customer TEXT NOT NULL, PRIMARY KEY(TenantId, OrderId));
            CREATE TABLE CkiTenantOrderLine(TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, LineNo INTEGER NOT NULL, Description TEXT NOT NULL, PRIMARY KEY(TenantId, OrderId, LineNo));
            CREATE TABLE CkiTenantShipment(TenantId INTEGER NOT NULL, OrderId INTEGER NOT NULL, LineNo INTEGER NOT NULL, ShipmentId INTEGER NOT NULL, Tracking TEXT NOT NULL, PRIMARY KEY(TenantId, OrderId, LineNo, ShipmentId));
            CREATE TABLE CkiExternalOrder(Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, ExternalNo TEXT NOT NULL, Customer TEXT NOT NULL);
            CREATE UNIQUE INDEX UX_CkiExternalOrder_Tenant_ExternalNo ON CkiExternalOrder(TenantId, ExternalNo);
            CREATE TABLE CkiExternalOrderEvent(Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, ExternalNo TEXT NOT NULL, EventName TEXT NOT NULL);
            INSERT INTO CkiTenantOrder VALUES (1,100,'Alice'),(2,100,'Bob'),(1,101,'Cara');
            INSERT INTO CkiTenantOrderLine VALUES (1,100,1,'a-100-1'),(1,100,2,'a-100-2'),(2,100,1,'b-100-1'),(1,101,1,'a-101-1');
            INSERT INTO CkiTenantShipment VALUES (1,100,1,1,'a-ship-1'),(1,100,1,2,'a-ship-2'),(2,100,1,1,'b-ship-1'),(1,101,1,1,'c-ship-1');
            INSERT INTO CkiExternalOrder VALUES (1,1,'EXT-100','Alice'),(2,2,'EXT-100','Bob'),(3,1,'EXT-101','Cara');
            INSERT INTO CkiExternalOrderEvent VALUES (1,1,'EXT-100','created'),(2,1,'EXT-100','paid'),(3,2,'EXT-100','created'),(4,1,'EXT-101','created');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), BuildOptions());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Include_CompositeKeyDependent_loads_children_correctly()
    {
        var blogs = await ((INormQueryable<Blog>)_ctx.Query<Blog>())
            .AsSplitQuery()
            .Include(b => b.OrderLines)
            .ToListAsync();

        Assert.Equal(3, blogs.Count);

        var alpha = blogs.First(b => b.Title == "Alpha");
        Assert.Equal(2, alpha.OrderLines.Count);
        Assert.Contains(alpha.OrderLines, ol => ol.Description == "a-line1");
        Assert.Contains(alpha.OrderLines, ol => ol.Description == "a-line2");

        var beta = blogs.First(b => b.Title == "Beta");
        Assert.Single(beta.OrderLines);
        Assert.Equal("b-line1", beta.OrderLines.First().Description);

        var gamma = blogs.First(b => b.Title == "Gamma");
        Assert.Empty(gamma.OrderLines);
    }

    [Fact]
    public async Task Include_CompositeKeyDependent_children_have_correct_composite_keys()
    {
        var blogs = await ((INormQueryable<Blog>)_ctx.Query<Blog>())
            .AsSplitQuery()
            .Include(b => b.OrderLines)
            .ToListAsync();

        var alpha = blogs.First(b => b.Title == "Alpha");
        Assert.All(alpha.OrderLines, ol => Assert.Equal(alpha.Id, ol.BlogId));
        Assert.Equal(new[] { 1, 2 }, alpha.OrderLines.Select(ol => ol.LineNumber).OrderBy(n => n).ToArray());
    }

    [Fact]
    public void Include_CompositeKeyDependent_Sync_loads_children_correctly()
    {
        var blogs = ((INormQueryable<Blog>)_ctx.Query<Blog>())
            .AsSplitQuery()
            .Include(b => b.OrderLines)
            .ToList();

        Assert.Equal(3, blogs.Count);
        var alpha = blogs.First(b => b.Title == "Alpha");
        Assert.Equal(2, alpha.OrderLines.Count);
    }

    [Fact]
    public async Task Include_CompositeKeyDependent_with_Where_loads_filtered_parent_children()
    {
        var blogs = await ((INormQueryable<Blog>)_ctx.Query<Blog>().Where(b => b.Title == "Alpha"))
            .AsSplitQuery()
            .Include(b => b.OrderLines)
            .ToListAsync();

        Assert.Single(blogs);
        Assert.Equal(2, blogs[0].OrderLines.Count);
    }

    [Fact]
    public async Task Include_CompositeForeignKey_loads_only_matching_composite_children()
    {
        var orders = await ((INormQueryable<TenantOrder>)_ctx.Query<TenantOrder>())
            .AsSplitQuery()
            .Include(o => o.Lines)
            .OrderBy(o => o.TenantId)
            .ThenBy(o => o.OrderId)
            .ToListAsync();

        Assert.Equal(3, orders.Count);

        var tenantOneOrder = orders.Single(o => o.TenantId == 1 && o.OrderId == 100);
        Assert.Equal(2, tenantOneOrder.Lines.Count);
        Assert.All(tenantOneOrder.Lines, l =>
        {
            Assert.Equal(1, l.TenantId);
            Assert.Equal(100, l.OrderId);
        });

        var tenantTwoSameOrderId = orders.Single(o => o.TenantId == 2 && o.OrderId == 100);
        var line = Assert.Single(tenantTwoSameOrderId.Lines);
        Assert.Equal("b-100-1", line.Description);
        Assert.Equal(2, line.TenantId);
        Assert.Equal(100, line.OrderId);
    }

    [Fact]
    public void Include_CompositeForeignKey_sync_loads_only_matching_composite_children()
    {
        var orders = ((INormQueryable<TenantOrder>)_ctx.Query<TenantOrder>())
            .AsSplitQuery()
            .Include(o => o.Lines)
            .ToList();

        var tenantOneOrder = orders.Single(o => o.TenantId == 1 && o.OrderId == 100);
        Assert.Equal(2, tenantOneOrder.Lines.Count);

        var tenantTwoSameOrderId = orders.Single(o => o.TenantId == 2 && o.OrderId == 100);
        Assert.Single(tenantTwoSameOrderId.Lines);
    }

    [Fact]
    public async Task ThenInclude_CompositeForeignKey_loads_only_matching_composite_grandchildren()
    {
        var orders = await ((INormQueryable<TenantOrder>)_ctx.Query<TenantOrder>())
            .AsSplitQuery()
            .Include(o => o.Lines)
            .ThenInclude(l => l.Shipments)
            .OrderBy(o => o.TenantId)
            .ThenBy(o => o.OrderId)
            .ToListAsync();

        var alice = orders.Single(o => o.TenantId == 1 && o.OrderId == 100);
        var aliceLine = alice.Lines.Single(l => l.LineNo == 1);
        Assert.Equal(new[] { "a-ship-1", "a-ship-2" }, aliceLine.Shipments.Select(s => s.Tracking).OrderBy(x => x).ToArray());

        var bob = orders.Single(o => o.TenantId == 2 && o.OrderId == 100);
        var bobLine = Assert.Single(bob.Lines);
        var bobShipment = Assert.Single(bobLine.Shipments);
        Assert.Equal("b-ship-1", bobShipment.Tracking);
    }

    [Fact]
    public async Task Include_CompositeForeignKey_to_unique_principal_key_loads_matching_children()
    {
        var orders = await ((INormQueryable<ExternalOrder>)_ctx.Query<ExternalOrder>())
            .AsSplitQuery()
            .Include(o => o.Events)
            .OrderBy(o => o.TenantId)
            .ThenBy(o => o.ExternalNo)
            .ToListAsync();

        var alice = orders.Single(o => o.TenantId == 1 && o.ExternalNo == "EXT-100");
        Assert.Equal(new[] { "created", "paid" }, alice.Events.Select(e => e.EventName).OrderBy(x => x).ToArray());
        Assert.All(alice.Events, e =>
        {
            Assert.Equal(1, e.TenantId);
            Assert.Equal("EXT-100", e.ExternalNo);
        });

        var bob = orders.Single(o => o.TenantId == 2 && o.ExternalNo == "EXT-100");
        var bobEvent = Assert.Single(bob.Events);
        Assert.Equal("created", bobEvent.EventName);

        var cara = orders.Single(o => o.TenantId == 1 && o.ExternalNo == "EXT-101");
        var caraEvent = Assert.Single(cara.Events);
        Assert.Equal("created", caraEvent.EventName);
    }
}
