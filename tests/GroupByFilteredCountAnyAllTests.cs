using System;
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
/// A grouped aggregate whose source is a filtered grouping — g.Where(f).Count() / .Any() / .All(p) — must
/// apply the filter, exactly as g.Where(f).Sum(s) already does. Count/Any/All read only the direct
/// predicate argument and never peeled the Where(...) source, so the filter was silently dropped: Count
/// counted the whole group, Any was always true, and All checked the predicate over every row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByFilteredCountAnyAllTests
{
    [Table("GfcSale")]
    public class Sale
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = "";
        public int Amount { get; set; }
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GfcSale (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO GfcSale (Id, Region, Amount) VALUES
                    (1,'N',10),(2,'N',20),(3,'N',30),
                    (4,'S',5),(5,'S',15),
                    (6,'E',5),(7,'E',8);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Sale>().HasKey(s => s.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Filtered_group_count_any_all_apply_the_where_filter()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Sale>()
            .GroupBy(s => s.Region)
            .Select(g => new
            {
                g.Key,
                C = g.Where(x => x.Amount > 10).Count(),
                A = g.Where(x => x.Amount > 10).Any(),
                L = g.Where(x => x.Amount > 10).All(x => x.Amount >= 15),
            })
            .ToListAsync())
            .ToDictionary(r => r.Key);

        // Verify against LINQ-to-Objects on the same shape.
        // N: >10 => {20,30}; S: >10 => {15}; E: >10 => {}
        Assert.Equal(2, rows["N"].C); Assert.True(rows["N"].A); Assert.True(rows["N"].L);   // {20,30} both >=15
        Assert.Equal(1, rows["S"].C); Assert.True(rows["S"].A); Assert.True(rows["S"].L);   // {15} >=15
        Assert.Equal(0, rows["E"].C); Assert.False(rows["E"].A); Assert.True(rows["E"].L);  // {} vacuously all
    }

    [Fact]
    public async Task Filtered_group_count_with_additional_predicate_ands_both()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Sale>()
            .GroupBy(s => s.Region)
            .Select(g => new { g.Key, C = g.Where(x => x.Amount > 10).Count(x => x.Amount < 30) })
            .ToListAsync())
            .ToDictionary(r => r.Key);

        // N: >10 AND <30 => {20}; S: {15}; E: {}
        Assert.Equal(1, rows["N"].C);
        Assert.Equal(1, rows["S"].C);
        Assert.Equal(0, rows["E"].C);
    }
}
