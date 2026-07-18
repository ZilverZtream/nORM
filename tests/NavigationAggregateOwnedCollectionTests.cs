using System;
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
/// A projected aggregate over an OWNED collection (OwnsMany) must lower to a correlated subquery over the
/// owned child table — one value per owner row. Owned collections live in a separate mapping structure
/// from ordinary relations, so these aggregates previously fell through to the outer-aggregate handler and
/// silently collapsed the whole result to a single wrong row (e.g. `[3]` instead of `[2, 1, 0]`). The
/// supported shapes emit correct SQL; the not-yet-supported ones fail loud rather than returning wrong data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateOwnedCollectionTests
{
    [Table("OcaOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public string Customer { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    private class Line
    {
        public int Id { get; set; }
        public int Amount { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OcaOrder (Id INTEGER PRIMARY KEY, Customer TEXT NOT NULL);
                CREATE TABLE OcaLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO OcaOrder VALUES (1,'a'),(2,'b'),(3,'c');
                -- Order 1: 2 lines (100, 75); Order 2: 1 line (50); Order 3: none.
                INSERT INTO OcaLine VALUES (1,1,100),(2,1,75),(3,2,50);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "OcaLine", foreignKey: "OrderId");
            }
        });
    }

    [Fact]
    public void Count_lowers_to_a_per_owner_correlated_subquery()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Order>().Select(o => new { o.Id, N = o.Lines.Count() })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { 2, 1, 0 }, rows.Select(r => r.N).ToArray());
    }

    [Fact]
    public void Any_lowers_to_a_per_owner_exists()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Order>().Select(o => new { o.Id, HasLines = o.Lines.Any() })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { true, true, false }, rows.Select(r => r.HasLines).ToArray());
    }

    [Fact]
    public void Sum_min_max_lower_to_per_owner_subqueries()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, Total = o.Lines.Sum(l => l.Amount), Biggest = o.Lines.Max(l => l.Amount) })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(175, rows[0].Total);   // 100 + 75
        Assert.Equal(50, rows[1].Total);
        Assert.Equal(0, rows[2].Total);     // empty → 0 (Enumerable.Sum semantics)
        Assert.Equal(100, rows[0].Biggest);
    }

    [Fact]
    public void Filtered_owned_aggregate_fails_loud_not_silently_wrong()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().Select(o => new { o.Id, N = o.Lines.Count(l => l.Amount > 60) }).ToList());
    }
}
