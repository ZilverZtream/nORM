using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// Hardens filtered Include against the temporal (AsOf) interaction — a repeated era-mixing kill site.
/// A filtered Include under AsOf must apply its predicate to the reconstructed historical era (the
/// history-window rows at the requested timestamp), never to the live table: the predicate's alias is the
/// same per-level history window the root reconstructs through. A closure predicate must also bind its own
/// compiled parameter alongside the temporal timestamp parameter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FilteredIncludeTemporalContractTests
{
    [Table("FitParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("FitChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FitParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE FitChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        opts.EnableTemporalVersioning();
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static async Task<DateTime> ServerNowAsync(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    /// <summary>Seeds p→{c1:10, c2:20}, snapshots t1, then bumps c1 to 100. At t1 the era is {10,20}.</summary>
    private static async Task<DateTime> SeedTwoErasAsync(DbContext ctx, SqliteConnection cn)
    {
        ctx.Add(new Parent { Id = 1, Name = "p" });
        ctx.Add(new Child { Id = 1, ParentId = 1, Val = 10 });
        ctx.Add(new Child { Id = 2, ParentId = 1, Val = 20 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync(cn);   // era: c1=10, c2=20
        await Task.Delay(60);

        var c1 = ctx.Find<Child>(1)!;
        c1.Val = 100;                        // c1 bumped above the threshold AFTER t1
        await ctx.SaveChangesAsync();
        return t1;
    }

    [Fact]
    public async Task Filtered_include_under_as_of_filters_the_historical_era_not_live_rows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = Bootstrap(cn);

        var t1 = await SeedTwoErasAsync(ctx, cn);

        // At t1 the children were {1:10, 2:20}; Val >= 15 keeps only c2 (20). If the filter ran against
        // live rows, c1 (now 100) would leak in and/or c2's era value would be wrong.
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children.Where(c => c.Val >= 15))
            .AsOf(t1)
            .ToListAsync();
        var kids = historic.Single().Children.OrderBy(c => c.Id).Select(c => $"{c.Id}:{c.Val}").ToArray();
        Assert.Equal(new[] { "2:20" }, kids);

        // Live filtered include: c1 is now 100 (>= 15) and c2 is 20, so both survive.
        var live = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children.Where(c => c.Val >= 15))
            .ToListAsync();
        Assert.Equal(new[] { "1:100", "2:20" },
            live.Single().Children.OrderBy(c => c.Id).Select(c => $"{c.Id}:{c.Val}").ToArray());
    }

    [Fact]
    public async Task Filtered_include_closure_under_as_of_coexists_with_the_temporal_parameter()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = Bootstrap(cn);

        var t1 = await SeedTwoErasAsync(ctx, cn);

        var threshold = 15;   // closure — its @cp param must bind alongside the @asof timestamp param
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children.Where(c => c.Val >= threshold))
            .AsOf(t1)
            .ToListAsync();

        Assert.Equal(new[] { "2:20" },
            historic.Single().Children.OrderBy(c => c.Id).Select(c => $"{c.Id}:{c.Val}").ToArray());
    }
}
