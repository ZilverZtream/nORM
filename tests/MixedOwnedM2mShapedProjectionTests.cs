using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A single projection that shapes TWO different collection kinds at once — an owned (OwnsMany) collection AND
/// a many-to-many collection, each with its own closure-captured element filter — produces two independent
/// split-query dependent loads in one plan. Pins that the two loaders don't interfere: each collection gets
/// its own rows and its own filter's parameters bind independently (a shared @cp collision would corrupt one).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MixedOwnedM2mShapedProjectionTests
{
    [Table("MixOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();   // owned
        public List<Tag> Tags { get; set; } = new();     // many-to-many
    }

    public class Line { [Key] public int Id { get; set; } public int Amount { get; set; } }

    [Table("MixTag")]
    public class Tag { [Key] public int Id { get; set; } public string Label { get; set; } = ""; }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MixOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE MixLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                CREATE TABLE MixTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                CREATE TABLE MixOrderTag (OrderId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO MixOrder VALUES (1);
                INSERT INTO MixLine VALUES (1,1,10),(2,1,5);
                INSERT INTO MixTag VALUES (1,'x'),(2,'y'),(3,'z');
                INSERT INTO MixOrderTag VALUES (1,1),(1,2),(1,3);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Tag>().HasKey(t => t.Id);
            mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "MixLine", foreignKey: "OrderId");
            mb.Entity<Order>().HasMany<Tag>(o => o.Tags).WithMany().UsingTable("MixOrderTag", "OrderId", "TagId");
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Owned_and_m2m_shaped_in_one_projection_load_independently()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, Lines = o.Lines.ToList(), Tags = o.Tags.ToList() })
            .ToList();
        var order = Assert.Single(rows);
        Assert.Equal(new[] { 5, 10 }, order.Lines.Select(l => l.Amount).OrderBy(a => a));
        Assert.Equal(new[] { "x", "y", "z" }, order.Tags.Select(t => t.Label).OrderBy(l => l));
    }

    [Fact]
    public void Owned_and_m2m_filters_bind_independently_in_one_projection()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var minAmount = 6;   // keeps line 10, drops line 5
        var minTagId = 1;    // keeps tags 2,3, drops tag 1
        var rows = ctx.Query<Order>()
            .Select(o => new
            {
                o.Id,
                Lines = o.Lines.Where(l => l.Amount > minAmount).ToList(),
                Tags = o.Tags.Where(t => t.Id > minTagId).ToList()
            })
            .ToList();
        var order = Assert.Single(rows);
        // Each filter's closure must bind to its OWN loader — a collision would cross the thresholds.
        Assert.Equal(new[] { 10 }, order.Lines.Select(l => l.Amount));
        Assert.Equal(new[] { "y", "z" }, order.Tags.Select(t => t.Label).OrderBy(l => l));
    }
}
