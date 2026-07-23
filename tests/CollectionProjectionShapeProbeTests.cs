#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probes the complex-materialization shapes — projecting a shaped/filtered child collection, SelectMany
/// flattening a navigation, and collection-nav aggregates in a projection — against a hand-resolved oracle
/// over the raw rows. These carry the highest remaining translation/materialization risk. Passing shapes
/// stay as regression keepers; a mismatch or throw is a gap to fix.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class CollectionProjectionShapeProbeTests
{
    [Table("CpOrder")]
    public sealed class Order
    {
        [Key] public int Id { get; set; }
        public string Cust { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("CpLine")]
    public sealed class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    private static readonly Order[] Orders =
    {
        new() { Id = 1, Cust = "ann" },
        new() { Id = 2, Cust = "bob" },
        new() { Id = 3, Cust = "cid" }, // no lines
    };
    private static readonly Line[] AllLines =
    {
        new() { Id = 1, OrderId = 1, Sku = "aaa", Qty = 3 },
        new() { Id = 2, OrderId = 1, Sku = "bbb", Qty = 1 },
        new() { Id = 3, OrderId = 2, Sku = "ccc", Qty = 5 },
        new() { Id = 4, OrderId = 2, Sku = "ddd", Qty = 1 },
    };

    private static IEnumerable<Line> LinesOf(Order o) => AllLines.Where(l => l.OrderId == o.Id);

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CpOrder (Id INTEGER PRIMARY KEY, Cust TEXT NOT NULL);
                CREATE TABLE CpLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL, Qty INTEGER NOT NULL);
                INSERT INTO CpOrder VALUES (1,'ann'),(2,'bob'),(3,'cid');
                INSERT INTO CpLine VALUES (1,1,'aaa',3),(2,1,'bbb',1),(3,2,'ccc',5),(4,2,'ddd',1);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId);
            }
        }, ownsConnection: true);
    }

    private static IQueryable<Order> QO(DbContext ctx) => ctx.Query<Order>();

    [Fact]
    public void Project_shaped_child_collection()
    {
        using var ctx = NewCtx();
        var norm = QO(ctx).OrderBy(o => o.Id)
            .Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToList() })
            .ToList().Select(x => (x.Id, string.Join(",", x.Skus.OrderBy(s => s)))).ToList();
        var oracle = Orders.OrderBy(o => o.Id)
            .Select(o => (o.Id, string.Join(",", LinesOf(o).Select(l => l.Sku).OrderBy(s => s)))).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Project_filtered_shaped_child_collection()
    {
        using var ctx = NewCtx();
        var norm = QO(ctx).OrderBy(o => o.Id)
            .Select(o => new { o.Id, Big = o.Lines.Where(l => l.Qty > 1).Select(l => l.Sku).ToList() })
            .ToList().Select(x => (x.Id, string.Join(",", x.Big.OrderBy(s => s)))).ToList();
        var oracle = Orders.OrderBy(o => o.Id)
            .Select(o => (o.Id, string.Join(",", LinesOf(o).Where(l => l.Qty > 1).Select(l => l.Sku).OrderBy(s => s)))).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void SelectMany_flatten_navigation()
    {
        using var ctx = NewCtx();
        var norm = QO(ctx).SelectMany(o => o.Lines, (o, l) => new { o.Cust, l.Sku })
            .OrderBy(x => x.Cust).ThenBy(x => x.Sku).ToList().Select(x => (x.Cust, x.Sku)).ToList();
        var oracle = Orders.SelectMany(o => LinesOf(o), (o, l) => new { o.Cust, l.Sku })
            .OrderBy(x => x.Cust).ThenBy(x => x.Sku).Select(x => (x.Cust, x.Sku)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void SelectMany_with_filtered_navigation()
    {
        using var ctx = NewCtx();
        var norm = QO(ctx).SelectMany(o => o.Lines.Where(l => l.Qty > 1), (o, l) => new { o.Cust, l.Sku })
            .OrderBy(x => x.Cust).ThenBy(x => x.Sku).ToList().Select(x => (x.Cust, x.Sku)).ToList();
        var oracle = Orders.SelectMany(o => LinesOf(o).Where(l => l.Qty > 1), (o, l) => new { o.Cust, l.Sku })
            .OrderBy(x => x.Cust).ThenBy(x => x.Sku).Select(x => (x.Cust, x.Sku)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_sum_and_max_in_projection()
    {
        using var ctx = NewCtx();
        var norm = QO(ctx).OrderBy(o => o.Id)
            .Select(o => new { o.Id, Total = o.Lines.Sum(l => l.Qty), Peak = o.Lines.Max(l => (int?)l.Qty) })
            .ToList().Select(x => (x.Id, x.Total, x.Peak)).ToList();
        var oracle = Orders.OrderBy(o => o.Id)
            .Select(o => (o.Id, Total: LinesOf(o).Sum(l => l.Qty), Peak: LinesOf(o).Select(l => (int?)l.Qty).Max())).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_ordered_first_by_safe_key_in_projection()
    {
        using var ctx = NewCtx();
        // Ordering by an int key (a "safe" order-key type whose SQL order matches C#) selects the same
        // top row both ways, so nORM emits a LIMIT-1 correlated subquery.
        var norm = QO(ctx).OrderBy(o => o.Id)
            .Select(o => new { o.Id, First = o.Lines.OrderBy(l => l.Qty).ThenBy(l => l.Id).Select(l => l.Sku).FirstOrDefault() })
            .ToList().Select(x => (x.Id, x.First)).ToList();
        var oracle = Orders.OrderBy(o => o.Id)
            .Select(o => (o.Id, First: LinesOf(o).OrderBy(l => l.Qty).ThenBy(l => l.Id).Select(l => l.Sku).FirstOrDefault())).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Collection_nav_unordered_first_fails_closed()
    {
        using var ctx = NewCtx();
        // An unordered First over a navigation collection is nondeterministic — there is no defined "first"
        // row — so nORM deliberately fails closed rather than emit a LIMIT 1 that could return a row diverging
        // from LINQ-to-Objects. This pins that correctness choice (no silent arbitrary row).
        Assert.ThrowsAny<Exception>(() =>
            QO(ctx).Select(o => new { o.Id, First = o.Lines.Select(l => l.Sku).FirstOrDefault() }).ToList());
    }

    [Fact]
    public void Collection_nav_string_ordered_first_fails_closed()
    {
        using var ctx = NewCtx();
        // Ordering the "first related value" by a STRING key would order by DB collation, which can differ
        // from C# ordinal ordering and silently pick a different row; nORM fails closed for string order keys.
        Assert.ThrowsAny<Exception>(() =>
            QO(ctx).Select(o => new { o.Id, First = o.Lines.OrderBy(l => l.Sku).Select(l => l.Sku).FirstOrDefault() }).ToList());
    }
}
