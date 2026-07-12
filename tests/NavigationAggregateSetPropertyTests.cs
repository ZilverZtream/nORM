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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ExecuteUpdate can set a column from an aggregate over a navigation collection
/// (SetProperty(p => p.Total, p => p.Items.Sum(i => i.Amount))). nORM emits a
/// correlated subquery against the dependent table, correlated to the row being
/// updated — no per-row round-trip and no raw SQL.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NavigationAggregateSetPropertyTests
{
    [Table("NavAggParent")]
    private class Parent
    {
        [Key] public int Id { get; set; }
        public decimal Total { get; set; }
        public int ItemCount { get; set; }
        public int ActiveCount { get; set; }
        public decimal MaxAmount { get; set; }
        public List<Item> Items { get; set; } = new();
    }

    [Table("NavAggItem")]
    private class Item
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public decimal Amount { get; set; }
        public bool Active { get; set; }
        public Parent Parent { get; set; } = null!;
    }

    private static DbContext CreateContext(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NavAggParent (Id INTEGER PRIMARY KEY, Total NUMERIC NOT NULL DEFAULT 0, " +
                "ItemCount INTEGER NOT NULL DEFAULT 0, ActiveCount INTEGER NOT NULL DEFAULT 0, MaxAmount NUMERIC NOT NULL DEFAULT 0);" +
                "CREATE TABLE NavAggItem (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount NUMERIC NOT NULL, Active INTEGER NOT NULL);" +
                // Parent 1 has three items (10, 20, 30); two are active. Parent 2 has none.
                "INSERT INTO NavAggParent (Id) VALUES (1), (2);" +
                "INSERT INTO NavAggItem (Id, ParentId, Amount, Active) VALUES " +
                "(1, 1, 10, 1), (2, 1, 20, 0), (3, 1, 30, 1);";
            cmd.ExecuteNonQuery();
        }

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Parent>()
                .HasKey(p => p.Id)
                .HasMany(p => p.Items).WithOne(i => i.Parent).HasForeignKey(i => i.ParentId, p => p.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Sum_over_navigation_collection_sets_column_and_zeroes_empty_collection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateContext(cn);

        await ctx.Query<Parent>().ExecuteUpdateAsync(s => s.SetProperty(p => p.Total, p => p.Items.Sum(i => i.Amount)));

        var byId = (await ctx.Query<Parent>().ToListAsync()).ToDictionary(p => p.Id);
        Assert.Equal(60m, byId[1].Total);
        Assert.Equal(0m, byId[2].Total); // empty collection -> COALESCE(SUM, 0), matches Enumerable.Sum
    }

    [Fact]
    public async Task Count_and_filtered_count_over_navigation_collection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateContext(cn);

        await ctx.Query<Parent>().ExecuteUpdateAsync(s => s
            .SetProperty(p => p.ItemCount, p => p.Items.Count())
            .SetProperty(p => p.ActiveCount, p => p.Items.Count(i => i.Active)));

        var byId = (await ctx.Query<Parent>().ToListAsync()).ToDictionary(p => p.Id);
        Assert.Equal(3, byId[1].ItemCount);
        Assert.Equal(2, byId[1].ActiveCount);
        Assert.Equal(0, byId[2].ItemCount);
        Assert.Equal(0, byId[2].ActiveCount);
    }

    [Fact]
    public async Task Max_over_navigation_collection_sets_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateContext(cn);

        // Restrict to the parent that has items — MAX over an empty set is NULL and the
        // target column is NOT NULL, which is correct SQL behaviour, not a nORM defect.
        await ctx.Query<Parent>().Where(p => p.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.MaxAmount, p => p.Items.Max(i => i.Amount)));

        var parent = (await ctx.Query<Parent>().ToListAsync()).Single(p => p.Id == 1);
        Assert.Equal(30m, parent.MaxAmount);
    }

    [Fact]
    public async Task Aggregate_with_arithmetic_selector_over_navigation_collection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateContext(cn);

        // Sum of (Amount * 2) = (10+20+30)*2 = 120.
        await ctx.Query<Parent>().Where(p => p.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Total, p => p.Items.Sum(i => i.Amount * 2)));

        var parent = (await ctx.Query<Parent>().ToListAsync()).Single(p => p.Id == 1);
        Assert.Equal(120m, parent.Total);
    }
}
