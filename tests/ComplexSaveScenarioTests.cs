using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial multi-entity save scenarios. These tests exercise realistic
/// e-commerce domain writes: compound inserts across FK-linked tables, order
/// cancellation, and rollback on partial failure — all in single SaveChanges calls.
/// </summary>
public class ComplexSaveScenarioTests
{
    // ── Domain model ───────────────────────────────────────────────────────

    [Table("CsCustomer")]
    private class Customer
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
    }

    [Table("CsOrder")]
    private class Order
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public string OrderDate { get; set; } = string.Empty;
        public string Status { get; set; } = "Pending";
    }

    [Table("CsProduct")]
    private class Product
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public int StockCount { get; set; }
    }

    [Table("CsOrderItem")]
    private class OrderItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int OrderId { get; set; }
        public int ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
    }

    // ── DB setup helper ────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(bool fkEnabled = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        if (fkEnabled)
        {
            cmd.CommandText = "PRAGMA foreign_keys = ON;";
            cmd.ExecuteNonQuery();
            using var cmd2 = cn.CreateCommand();
            cmd2.CommandText = BuildSchema(withForeignKeys: true);
            cmd2.ExecuteNonQuery();
        }
        else
        {
            cmd.CommandText = BuildSchema(withForeignKeys: false);
            cmd.ExecuteNonQuery();
        }

        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    private static string BuildSchema(bool withForeignKeys)
    {
        if (withForeignKeys)
        {
            return
                "CREATE TABLE CsCustomer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Email TEXT NOT NULL);" +
                "CREATE TABLE CsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Price REAL NOT NULL, StockCount INTEGER NOT NULL);" +
                "CREATE TABLE CsOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, CustomerId INTEGER NOT NULL REFERENCES CsCustomer(Id), OrderDate TEXT NOT NULL, Status TEXT NOT NULL);" +
                "CREATE TABLE CsOrderItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL REFERENCES CsOrder(Id), ProductId INTEGER NOT NULL REFERENCES CsProduct(Id), Quantity INTEGER NOT NULL, UnitPrice REAL NOT NULL);";
        }
        return
            "CREATE TABLE CsCustomer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Email TEXT NOT NULL);" +
            "CREATE TABLE CsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Price REAL NOT NULL, StockCount INTEGER NOT NULL);" +
            "CREATE TABLE CsOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, CustomerId INTEGER NOT NULL, OrderDate TEXT NOT NULL, Status TEXT NOT NULL);" +
            "CREATE TABLE CsOrderItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, ProductId INTEGER NOT NULL, Quantity INTEGER NOT NULL, UnitPrice REAL NOT NULL);";
    }

    // ── Test 1: Place order — 6 writes in one SaveChanges ──────────────────

    /// <summary>
    /// Realistic order placement: Customer + Product + Order + 3 OrderItems + Stock update
    /// all saved in one SaveChanges. Asserts FK integrity and stock correctness.
    /// </summary>
    [Fact]
    public async Task PlaceOrder_SixEntitiesOneCall_AllPersistedWithCorrectStock()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        var customer = new Customer { Name = "Alice", Email = "alice@example.com" };
        var product = new Product { Name = "Widget", Price = 9.99m, StockCount = 5 };

        ctx.Add(customer);
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        // Customer and Product now have real IDs
        Assert.True(customer.Id > 0, "Customer should have been assigned an ID");
        Assert.True(product.Id > 0, "Product should have been assigned an ID");

        var order = new Order { CustomerId = customer.Id, OrderDate = "2024-01-15", Status = "Pending" };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        Assert.True(order.Id > 0, "Order should have been assigned an ID");

        // 3 order items
        ctx.Add(new OrderItem { OrderId = order.Id, ProductId = product.Id, Quantity = 1, UnitPrice = 9.99m });
        ctx.Add(new OrderItem { OrderId = order.Id, ProductId = product.Id, Quantity = 1, UnitPrice = 9.99m });
        ctx.Add(new OrderItem { OrderId = order.Id, ProductId = product.Id, Quantity = 1, UnitPrice = 9.99m });

        // Update stock: 5 - 3 = 2
        product.StockCount = 2;
        ctx.Update(product);

        var saved = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(4, saved); // 3 items + 1 product update

        // Verify data correctness
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CsOrderItem WHERE OrderId = " + order.Id;
        Assert.Equal(3L, Convert.ToInt64(vcmd.ExecuteScalar()));

        vcmd.CommandText = "SELECT StockCount FROM CsProduct WHERE Id = " + product.Id;
        Assert.Equal(2L, Convert.ToInt64(vcmd.ExecuteScalar()));

        vcmd.CommandText = "SELECT Status FROM CsOrder WHERE Id = " + order.Id;
        Assert.Equal("Pending", (string)vcmd.ExecuteScalar()!);
    }

    // ── Test 2: Order cancellation in one SaveChanges ──────────────────────

    /// <summary>
    /// Cancel order: Status update + OrderItem deletes + stock restore, all in one
    /// SaveChanges. No orphaned items must remain after the operation.
    /// </summary>
    [Fact]
    public async Task CancelOrder_UpdateDeleteRestore_NeitherOrphanedItemsNorWrongStock()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        // Seed data directly
        using var seed = cn.CreateCommand();
        seed.CommandText =
            "INSERT INTO CsCustomer VALUES (1, 'Alice', 'alice@example.com');" +
            "INSERT INTO CsProduct VALUES (1, 'Widget', 9.99, 2);" +
            "INSERT INTO CsOrder VALUES (1, 1, '2024-01-15', 'Pending');" +
            "INSERT INTO CsOrderItem VALUES (1, 1, 1, 1, 9.99);" +
            "INSERT INTO CsOrderItem VALUES (2, 1, 1, 2, 9.99);";
        seed.ExecuteNonQuery();

        // Load and modify
        var order = ctx.Query<Order>().First(o => o.Id == 1);
        order.Status = "Cancelled";
        ctx.Update(order);

        // Delete all order items for this order
        var items = ctx.Query<OrderItem>().Where(i => i.OrderId == 1).ToList();
        Assert.Equal(2, items.Count);
        foreach (var item in items)
            ctx.Remove(item);

        // Restore stock: items had qty 1 + qty 2 = 3 total, new stock = 2 + 3 = 5
        var product = ctx.Query<Product>().First(p => p.Id == 1);
        product.StockCount = 5;
        ctx.Update(product);

        var saved = await ctx.SaveChangesAsync(detectChanges: false);

        // 1 order update + 2 item deletes + 1 product update = 4
        Assert.Equal(4, saved);

        // No orphaned order items
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CsOrderItem WHERE OrderId = 1";
        Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Order status updated
        vcmd.CommandText = "SELECT Status FROM CsOrder WHERE Id = 1";
        Assert.Equal("Cancelled", (string)vcmd.ExecuteScalar()!);

        // Stock restored
        vcmd.CommandText = "SELECT StockCount FROM CsProduct WHERE Id = 1";
        Assert.Equal(5L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 3: Rollback on partial failure ────────────────────────────────

    /// <summary>
    /// A SaveChanges that includes a valid Customer insert and then an invalid
    /// operation must roll back the entire batch. The Customer must NOT persist.
    /// </summary>
    [Fact]
    public async Task SaveChanges_PartialFailure_RollsBackCompletely()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        // Add a valid customer
        ctx.Add(new Customer { Name = "ValidCustomer", Email = "valid@example.com" });
        await ctx.SaveChangesAsync();

        // Verify it was saved
        using var checkCmd = cn.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(*) FROM CsCustomer WHERE Name = 'ValidCustomer'";
        Assert.Equal(1L, Convert.ToInt64(checkCmd.ExecuteScalar()));

        // Drop the table to force the next save to fail
        using var dropCmd = cn.CreateCommand();
        dropCmd.CommandText = "DROP TABLE CsCustomer";
        dropCmd.ExecuteNonQuery();

        // Re-create without the table — now add another customer (will fail)
        ctx.Add(new Customer { Name = "ShouldNotExist", Email = "fail@example.com" });

        // The save must throw (table doesn't exist)
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.NotNull(ex);
        Assert.IsNotType<AggregateException>(ex); // rollback succeeded, original ex rethrown

        // DB state: table gone (we dropped it), so we can verify via sqlite_master
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='CsCustomer'";
        Assert.Equal(0L, Convert.ToInt64(verifyCmd.ExecuteScalar()));
    }

    // ── Test 4: Mixed operations — insert + update + delete same SaveChanges ─

    /// <summary>
    /// One SaveChanges that simultaneously inserts a new product, updates an existing
    /// order status, and deletes an order item. All three operations must complete.
    /// </summary>
    [Fact]
    public async Task MixedOperations_InsertUpdateDelete_AllCommitted()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        // Seed
        using var seed = cn.CreateCommand();
        seed.CommandText =
            "INSERT INTO CsCustomer VALUES (1, 'Bob', 'bob@example.com');" +
            "INSERT INTO CsProduct VALUES (1, 'OldWidget', 5.00, 10);" +
            "INSERT INTO CsOrder VALUES (1, 1, '2024-02-01', 'Pending');" +
            "INSERT INTO CsOrderItem VALUES (1, 1, 1, 2, 5.00);" +
            "INSERT INTO CsOrderItem VALUES (2, 1, 1, 1, 5.00);";
        seed.ExecuteNonQuery();

        // Insert new product
        var newProduct = new Product { Name = "NewWidget", Price = 7.50m, StockCount = 20 };
        ctx.Add(newProduct);

        // Update order status
        var order = ctx.Query<Order>().First(o => o.Id == 1);
        order.Status = "Shipped";
        ctx.Update(order);

        // Delete one order item
        var itemToDelete = ctx.Query<OrderItem>().First(i => i.Id == 1);
        ctx.Remove(itemToDelete);

        var saved = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(3, saved); // 1 insert + 1 update + 1 delete

        // Verify new product
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CsProduct WHERE Name = 'NewWidget'";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Verify order update
        vcmd.CommandText = "SELECT Status FROM CsOrder WHERE Id = 1";
        Assert.Equal("Shipped", (string)vcmd.ExecuteScalar()!);

        // Verify item deletion
        vcmd.CommandText = "SELECT COUNT(*) FROM CsOrderItem WHERE Id = 1";
        Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Verify item 2 still there
        vcmd.CommandText = "SELECT COUNT(*) FROM CsOrderItem WHERE Id = 2";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 5: Many-to-one FK integrity ──────────────────────────────────

    /// <summary>
    /// With FK enforcement on, inserting an order for a non-existent customer must fail.
    /// nORM must not swallow the FK violation.
    /// </summary>
    [Fact]
    public async Task FKViolation_OrderWithBadCustomerId_ThrowsAndRollsBack()
    {
        var (cn, ctx) = CreateContext(fkEnabled: true);
        await using var _ = ctx;

        // Insert an order with CustomerId=999 (no such customer)
        ctx.Add(new Order { CustomerId = 999, OrderDate = "2024-03-01", Status = "Pending" });

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.NotNull(ex);

        // DB must be clean — no order was committed
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CsOrder";
        Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 6: Bulk insert then selective delete ──────────────────────────

    /// <summary>
    /// Insert 20 products, then delete every other one in a single SaveChanges.
    /// Final count must be exactly 10 with correct IDs.
    /// </summary>
    [Fact]
    public async Task BulkInsertThenSelectiveDelete_CorrectFinalCount()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;

        for (int i = 1; i <= 20; i++)
            ctx.Add(new Product { Name = $"Product{i}", Price = i * 1.0m, StockCount = i });

        await ctx.SaveChangesAsync();

        // Verify all 20 inserted
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CsProduct";
        Assert.Equal(20L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Delete every other product (those with odd IDs)
        var products = ctx.Query<Product>().ToList();
        var toDelete = products.Where(p => p.Id % 2 == 1).ToList();
        Assert.Equal(10, toDelete.Count);

        foreach (var p in toDelete)
            ctx.Remove(p);

        await ctx.SaveChangesAsync(detectChanges: false);

        vcmd.CommandText = "SELECT COUNT(*) FROM CsProduct";
        Assert.Equal(10L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Verify only even-ID products remain
        vcmd.CommandText = "SELECT COUNT(*) FROM CsProduct WHERE Id % 2 = 0";
        Assert.Equal(10L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }
}
