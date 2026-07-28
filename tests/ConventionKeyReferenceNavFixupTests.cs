using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Assigning a reference navigation to a NEW principal whose key is a plain convention
/// store-generated key (a bare <c>[Key] int Id</c> with no <c>[DatabaseGenerated]</c> —
/// the most common key style) must persist the dependent's foreign key as the principal's
/// generated key, not 0. The deferral that waits for a generated principal key was gated
/// only on <c>IsDbGenerated</c>, which is false for a convention key, so the FK was fixed
/// up to the principal's still-default value (0) and written that way — a silent-wrong FK.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ConventionKeyReferenceNavFixupTests
{
    [Table("CkRef_Customer")]
    private class Customer
    {
        [Key] public int Id { get; set; }   // convention store-generated key (no [DatabaseGenerated])
        public string Name { get; set; } = "";
    }

    [Table("CkRef_Order")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        [ForeignKey(nameof(CustomerId))] public Customer? Customer { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CkRef_Customer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                CREATE TABLE CkRef_Order (Id INTEGER PRIMARY KEY AUTOINCREMENT, CustomerId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Customer>().HasKey(c => c.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
            }
        });
    }

    private static long Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task New_principal_with_convention_key_links_dependent_fk_after_insert()
    {
        var ctx = Ctx(out var cn);
        using var _cn = cn; using var _ctx = ctx;

        var order = new Order { Customer = new Customer { Name = "acme" } };
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        // The principal got a generated key; the dependent's FK must match it — in memory AND in the DB.
        Assert.NotEqual(0, order.Customer!.Id);
        Assert.Equal(order.Customer.Id, order.CustomerId);

        // Read the RAW persisted FK column — the silent-wrong bug writes 0 here while the tracked entity looks fine.
        var persistedFk = Scalar(cn, "SELECT CustomerId FROM CkRef_Order WHERE Id = " + order.Id);
        var customerKey = Scalar(cn, "SELECT Id FROM CkRef_Customer WHERE Name = 'acme'");
        Assert.Equal(customerKey, persistedFk);
    }
}
