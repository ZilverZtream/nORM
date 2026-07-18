using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// One projection may mix aggregates over different collection kinds — an owned (OwnsMany) collection and an
/// ordinary relation — plus a scalar owned aggregate, each emitted as an independent correlated subquery.
/// They compose without the per-subquery <c>__nav</c> aliases colliding and each returns its own correct value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateCombinedKindsTests
{
    [Table("CaOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();       // owned
        public List<Payment> Payments { get; set; } = new(); // relation
    }
    private class Line { public int Id { get; set; } public int Amount { get; set; } }
    [Table("CaPayment")]
    private class Payment { [Key] public int Id { get; set; } public int OrderId { get; set; } public int Amount { get; set; } }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CaOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE CaLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                CREATE TABLE CaPayment (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO CaOrder VALUES (1),(2);
                INSERT INTO CaLine VALUES (1,1,10),(2,1,20),(3,2,5);       -- order1: 2 lines, order2: 1
                INSERT INTO CaPayment VALUES (1,1,100),(2,2,50),(3,2,25);  -- order1: 1 payment, order2: 2
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Payment>().HasKey(p => p.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "CaLine", foreignKey: "OrderId");
                mb.Entity<Order>().HasMany(o => o.Payments).WithOne().HasForeignKey(p => p.OrderId, o => o.Id);
            }
        });
    }

    [Fact]
    public void combined_owned_and_relation_aggregate_in_one_projection()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, L = o.Lines.Count(), P = o.Payments.Count(), LSum = o.Lines.Sum(l => l.Amount) })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { 2, 1 }, rows.Select(r => r.L).ToArray());     // owned line counts
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.P).ToArray());     // relation payment counts
        Assert.Equal(new[] { 30, 5 }, rows.Select(r => r.LSum).ToArray()); // owned line sums
    }
}
