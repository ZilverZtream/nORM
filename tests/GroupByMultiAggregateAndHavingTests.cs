using System;
using System.Collections.Generic;
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
/// GroupBy with multiple aggregates per group, Having (Where on an aggregate), composite keys,
/// Having+OrderBy-by-aggregate, and a filtered Count(predicate) — all oracle-verified against
/// LINQ-to-objects. (The GroupBy(key, elementSelector) + aggregate variant is a separate, deferred
/// non-data-loss gap; see the groupby_element_selector_gap memory.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupByMultiAggregateAndHavingTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GmaSale")]
    public sealed class Sale
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Region { get; set; } = "";
        public string Product { get; set; } = "";
        public int Amount { get; set; }
    }

    private static readonly (int id, string region, string product, int amount)[] Rows =
    {
        (1, "N", "A", 10), (2, "N", "B", 20), (3, "N", "A", 30),
        (4, "S", "A", 5),  (5, "S", "C", 15),
    };

    private static IEnumerable<Sale> Mem => Rows.Select(r => new Sale { Id = r.id, Region = r.region, Product = r.product, Amount = r.amount });

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GmaSale (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Product TEXT NOT NULL, Amount INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Sale>().HasKey(s => s.Id) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in Rows) await ctx.InsertAsync(new Sale { Id = r.id, Region = r.region, Product = r.product, Amount = r.amount });
        return ctx;
    }

    [Fact]
    public async Task Multiple_aggregates_per_group()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region)
            .Select(g => g.Key + ":" + g.Count() + "," + g.Sum(x => x.Amount) + "," + g.Min(x => x.Amount) + "," + g.Max(x => x.Amount))
            .OrderBy(x => x).ToList();
        var exp = Mem.GroupBy(s => s.Region)
            .Select(g => g.Key + ":" + g.Count() + "," + g.Sum(x => x.Amount) + "," + g.Min(x => x.Amount) + "," + g.Max(x => x.Amount))
            .OrderBy(x => x).ToList();
        Assert.Equal(exp, got);
    }

    [Fact]
    public async Task Average_value_is_correct()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region).OrderBy(g => g.Key).Select(g => g.Average(x => x.Amount)).ToList();
        Assert.Equal(new[] { 20.0, 10.0 }, got);
    }

    [Fact]
    public async Task Having_on_count_and_sum()
    {
        using var ctx = await CtxAsync();
        Assert.Equal(new[] { "N" }, ctx.Query<Sale>().GroupBy(s => s.Region).Where(g => g.Count() > 2).Select(g => g.Key).OrderBy(x => x).ToList());
        Assert.Equal(new[] { "N" }, ctx.Query<Sale>().GroupBy(s => s.Region).Where(g => g.Sum(x => x.Amount) > 30).Select(g => g.Key).OrderBy(x => x).ToList());
    }

    [Fact]
    public async Task Composite_key_group_sum()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => new { s.Region, s.Product })
            .Select(g => g.Key.Region + "/" + g.Key.Product + ":" + g.Sum(x => x.Amount)).OrderBy(x => x).ToList();
        var exp = Mem.GroupBy(s => new { s.Region, s.Product })
            .Select(g => g.Key.Region + "/" + g.Key.Product + ":" + g.Sum(x => x.Amount)).OrderBy(x => x).ToList();
        Assert.Equal(exp, got);
    }

    [Fact]
    public async Task Having_then_orderby_aggregate()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region).Where(g => g.Count() >= 2)
            .OrderByDescending(g => g.Sum(x => x.Amount)).Select(g => g.Key).ToList();
        Assert.Equal(new[] { "N", "S" }, got); // N sum 60 > S sum 20
    }

    [Fact]
    public async Task Filtered_count_predicate_per_group()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region).Select(g => g.Key + ":" + g.Count(x => x.Amount > 10)).OrderBy(x => x).ToList();
        var exp = Mem.GroupBy(s => s.Region).Select(g => g.Key + ":" + g.Count(x => x.Amount > 10)).OrderBy(x => x).ToList();
        Assert.Equal(exp, got);
    }
}
