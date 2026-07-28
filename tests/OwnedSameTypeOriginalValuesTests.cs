using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Two owned members of the SAME value-object type (ShipTo/BillTo : Address, Subtotal/Tax : Money) map to
/// distinct prefixed columns (Subtotal_Amount, Tax_Amount) that nonetheless share ONE PropertyInfo
/// (Money.Amount). The original-values snapshot was keyed by the simple property name, so both columns
/// collided onto one slot: OriginalValues["Subtotal_Amount"] silently returned Tax's value, and the OCC
/// conflict-reset (OriginalValues.SetValues) corrupted one member's baseline — a lost-update vector.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedSameTypeOriginalValuesTests
{
    [Owned]
    public class Money
    {
        public decimal Amount { get; set; }
        public string Currency { get; set; } = "";
    }

    [Table("OscOrder_Test")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public Money Subtotal { get; set; } = new();
        public Money Tax { get; set; } = new();
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE OscOrder_Test (Id INTEGER PRIMARY KEY, " +
                "Subtotal_Amount TEXT NOT NULL, Subtotal_Currency TEXT NOT NULL, " +
                "Tax_Amount TEXT NOT NULL, Tax_Currency TEXT NOT NULL);" +
                "INSERT INTO OscOrder_Test VALUES (1, '100', 'USD', '8', 'EUR');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsOne(o => o.Subtotal);
                mb.Entity<Order>().OwnsOne(o => o.Tax);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void OriginalValues_resolve_each_same_typed_owned_column_to_its_own_slot()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var order = ctx.Query<Order>().First();
        var original = ctx.Entry(order).OriginalValues;

        // Each prefixed owned column must read its OWN baseline, not the other member's.
        Assert.Equal(100m, Convert.ToDecimal(original["Subtotal_Amount"]));
        Assert.Equal(8m, Convert.ToDecimal(original["Tax_Amount"]));
        Assert.Equal("USD", original["Subtotal_Currency"]);
        Assert.Equal("EUR", original["Tax_Currency"]);
    }

    [Fact]
    public void IsModified_flags_the_addressed_same_typed_owned_column()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var order = ctx.Query<Order>().First();
        var entry = ctx.Entry(order);

        entry.Property("Subtotal_Amount").IsModified = true;

        Assert.True(entry.Property("Subtotal_Amount").IsModified);
        Assert.False(entry.Property("Tax_Amount").IsModified);
    }
}
