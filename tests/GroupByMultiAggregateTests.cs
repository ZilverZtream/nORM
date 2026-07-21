using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// GroupBy reporting shapes checked against a LINQ-to-Objects oracle over the same rows: several aggregates
/// (Count/Sum/Min/Max) in one group projection, a Having filter on an aggregate, and a grouped sum ordered
/// descending. These are common EF-style reporting queries; nORM must produce the same results.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByMultiAggregateTests
{
    [Table("GmaSale")]
    public class Sale
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = "";
        public int Amount { get; set; }
    }

    private static readonly (int Id, string Cat, int Amount)[] Seed =
    {
        (1, "a", 10), (2, "a", 30), (3, "a", 20),
        (4, "b", 5),  (5, "b", 15),
        (6, "c", 100),
    };

    private static (SqliteConnection, DbContext) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GmaSale (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, Amount INTEGER NOT NULL);";
            foreach (var (id, cat, amt) in Seed)
                cmd.CommandText += $"INSERT INTO GmaSale VALUES ({id},'{cat}',{amt});";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static List<Sale> Oracle() => Seed.Select(s => new Sale { Id = s.Id, Cat = s.Cat, Amount = s.Amount }).ToList();

    [Fact]
    public async Task Multiple_aggregates_in_one_group_projection_match_the_oracle()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Sale>()
            .GroupBy(s => s.Cat)
            .Select(g => new { g.Key, Count = g.Count(), Sum = g.Sum(x => x.Amount), Min = g.Min(x => x.Amount), Max = g.Max(x => x.Amount) })
            .ToListAsync())
            .OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Count},{x.Sum},{x.Min},{x.Max}").ToList();
        var oracle = Oracle()
            .GroupBy(s => s.Cat)
            .Select(g => new { g.Key, Count = g.Count(), Sum = g.Sum(x => x.Amount), Min = g.Min(x => x.Amount), Max = g.Max(x => x.Amount) })
            .OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Count},{x.Sum},{x.Min},{x.Max}").ToList();

        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Having_filter_on_an_aggregate_matches_the_oracle()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Sale>()
            .GroupBy(s => s.Cat)
            .Where(g => g.Count() > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToListAsync())
            .OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Count}").ToList();
        var oracle = Oracle()
            .GroupBy(s => s.Cat)
            .Where(g => g.Count() > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Count}").ToList();

        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task Grouped_sum_ordered_descending_matches_the_oracle()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var norm = (await ctx.Query<Sale>()
            .GroupBy(s => s.Cat)
            .Select(g => new { g.Key, Total = g.Sum(x => x.Amount) })
            .OrderByDescending(x => x.Total)
            .ToListAsync())
            .Select(x => $"{x.Key}:{x.Total}").ToList();
        var oracle = Oracle()
            .GroupBy(s => s.Cat)
            .Select(g => new { g.Key, Total = g.Sum(x => x.Amount) })
            .OrderByDescending(x => x.Total)
            .Select(x => $"{x.Key}:{x.Total}").ToList();

        Assert.Equal(oracle, norm);
    }
}
