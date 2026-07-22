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
/// GroupBy WITH an element selector — GroupBy(s => s.Region, s => s.Amount) — then an aggregate over the
/// selected elements. A parameterless aggregate (g.Sum() / g.Max()) uses the element selector's body as its
/// operand, so g.Sum() lowers to SUM(Amount); an identity selector (g.Sum(x => x)) expands the same way.
/// Previously the parameterless form failed loud and the identity form emitted invalid SQL. Covers the
/// top-level and anonymous-type projection shapes (the aggregate-inside-a-computed/concat-body shape goes
/// through a separate visitor and is a known remaining gap — see groupby_element_selector_gap memory).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupByElementSelectorAggregateTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GesSale")]
    public sealed class Sale
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Region { get; set; } = "";
        public int Amount { get; set; }
    }

    private static readonly (int id, string region, int amount)[] Rows =
        { (1, "N", 10), (2, "N", 20), (3, "N", 30), (4, "S", 5), (5, "S", 15) };

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GesSale (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Amount INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Sale>().HasKey(s => s.Id) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in Rows) await ctx.InsertAsync(new Sale { Id = r.id, Region = r.region, Amount = r.amount });
        return ctx;
    }

    [Fact]
    public async Task Parameterless_sum_over_element_selected_group()
    {
        using var ctx = await CtxAsync();
        // N: 10+20+30=60 ; S: 5+15=20.
        var got = ctx.Query<Sale>().GroupBy(s => s.Region, s => s.Amount).Select(g => g.Sum()).ToList().OrderBy(x => x).ToList();
        Assert.Equal(new[] { 20, 60 }, got);
    }

    [Fact]
    public async Task Parameterless_max_over_element_selected_group()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region, s => s.Amount).Select(g => g.Max()).ToList().OrderBy(x => x).ToList();
        Assert.Equal(new[] { 15, 30 }, got);
    }

    [Fact]
    public async Task Parameterless_sum_in_anon_projection()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region, s => s.Amount).Select(g => new { g.Key, S = g.Sum() })
            .ToList().OrderBy(x => x.Key).Select(x => x.Key + ":" + x.S).ToList();
        Assert.Equal(new[] { "N:60", "S:20" }, got);
    }

    [Fact]
    public async Task Identity_selector_sum_in_anon_projection()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Sale>().GroupBy(s => s.Region, s => s.Amount).Select(g => new { g.Key, S = g.Sum(x => x) })
            .ToList().OrderBy(x => x.Key).Select(x => x.Key + ":" + x.S).ToList();
        Assert.Equal(new[] { "N:60", "S:20" }, got);
    }
}
