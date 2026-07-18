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
/// Decimal aggregates over an owned or many-to-many collection keep full decimal precision — the emit routes
/// the operand through the provider's decimal-aggregate hook exactly as the relation path does, so
/// <c>Sum</c> is exact (0.1 + 0.2 = 0.3, not a float-corrupted 0.30000000000000004) and <c>Max</c> returns a
/// true stored value. Guards against the owned/m2m aggregate path silently falling back to IEEE-754.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateDecimalPrecisionTests
{
    [Table("DcaOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();       // owned
        public List<Tag> Tags { get; set; } = new();         // m2m
    }
    private class Line { public int Id { get; set; } public decimal Price { get; set; } }
    [Table("DcaTag")]
    private class Tag { [Key] public int Id { get; set; } public decimal Weight { get; set; } }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DcaOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE DcaLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Price TEXT NOT NULL);
                CREATE TABLE DcaTag (Id INTEGER PRIMARY KEY, Weight TEXT NOT NULL);
                CREATE TABLE DcaOrderTag (OrderId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO DcaOrder VALUES (1);
                -- 0.1 + 0.2 = 0.3 exactly in decimal (float would give 0.30000000000000004).
                INSERT INTO DcaLine VALUES (1,1,'0.1'),(2,1,'0.2');
                INSERT INTO DcaTag VALUES (1,'0.1'),(2,'0.2');
                INSERT INTO DcaOrderTag VALUES (1,1),(1,2);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "DcaLine", foreignKey: "OrderId");
                mb.Entity<Order>().HasMany<Tag>(o => o.Tags).WithMany().UsingTable("DcaOrderTag", "OrderId", "TagId");
            }
        });
    }

    [Fact]
    public void owned_decimal_sum_is_exact()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var row = ctx.Query<Order>().Select(o => new { S = o.Lines.Sum(l => l.Price) }).ToList().Single();
        Assert.Equal(0.3m, row.S);   // exact decimal, not 0.30000000000000004
    }

    [Fact]
    public void m2m_decimal_sum_is_exact()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var row = ctx.Query<Order>().Select(o => new { S = o.Tags.Sum(t => t.Weight) }).ToList().Single();
        Assert.Equal(0.3m, row.S);
    }

    [Fact]
    public void owned_decimal_max_is_a_true_stored_value()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var row = ctx.Query<Order>().Select(o => new { M = o.Lines.Max(l => l.Price) }).ToList().Single();
        Assert.Equal(0.2m, row.M);
    }
}
