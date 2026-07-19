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
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that a navigation-collection PROJECTION under AsOf reconstructs the historical era instead of mixing
/// it with live rows. The split-query child load reads the {Table}_History window at the timestamp (aliased AS
/// the live table so the FK/tenant/global/element filters resolve to the reconstructed rows), mirroring the
/// owned-collection reconstruction — so the projected collection holds the children as they were at the
/// timestamp and any element filter applies to those historical values. Provider-native temporal storage
/// isn't wired through this path yet and stays fail-loud (it can't be validated without a live SQL Server).
/// Filtered <c>Include</c> under AsOf is a separate path — see <see cref="FilteredIncludeTemporalContractTests"/>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionProjectionTemporalContractTests
{
    [Table("SptParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("SptChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    public class ChildDto { public int Val { get; set; } }
    public class ParentDto { public int Id { get; set; } public List<ChildDto> Kids { get; set; } = new(); }

    private static DbContext Boot(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SptParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE SptChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
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

    private static async Task<DateTime> ServerNow(SqliteConnection cn)
    {
        using var c = cn.CreateCommand();
        c.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f','now')";
        return DateTime.SpecifyKind(DateTime.Parse((string)(await c.ExecuteScalarAsync())!, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    private static async Task<(SqliteConnection cn, DbContext ctx, DateTime t1)> SeedTwoEras()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        var ctx = Boot(cn);
        ctx.Add(new Parent { Id = 1 });
        ctx.Add(new Child { Id = 1, ParentId = 1, Val = 10 });
        ctx.Add(new Child { Id = 2, ParentId = 1, Val = 20 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNow(cn);
        await Task.Delay(60);
        var c1 = ctx.Find<Child>(1)!; c1.Val = 100; await ctx.SaveChangesAsync();   // c1: 10 -> 100 after t1
        return (cn, ctx, t1);
    }

    [Fact]
    public async Task anon_shaped_collection_projection_under_as_of_reconstructs_the_era()
    {
        var (cn, ctx, t1) = await SeedTwoEras();
        using var _cn = cn; await using var _ctx = ctx;
        // At t1, c1 was 10 (< 15) so only c2 (20) matches; the live c1=100 must NOT leak in — the child load
        // reads the reconstructed history window, and the element filter applies to the historical values.
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Select(p => new { p.Id, Vals = p.Children.Where(c => c.Val >= 15).Select(c => c.Val).ToList() })
            .AsOf(t1).ToListAsync();
        Assert.Equal(new[] { 20 }, historic.Single().Vals.OrderBy(v => v).ToArray());
    }

    [Fact]
    public async Task dto_shaped_collection_projection_under_as_of_reconstructs_the_era()
    {
        var (cn, ctx, t1) = await SeedTwoEras();
        using var _cn = cn; await using var _ctx = ctx;
        // No filter: at t1 the children were {10, 20}; the live c1=100 must reconstruct to its t1 value (10).
        var historic = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Select(p => new ParentDto { Id = p.Id, Kids = p.Children.Select(c => new ChildDto { Val = c.Val }).ToList() })
            .AsOf(t1).ToListAsync();
        Assert.Equal(new[] { 10, 20 }, historic.Single().Kids.Select(k => k.Val).OrderBy(v => v).ToArray());
    }

    [Fact]
    public async Task collection_projection_without_as_of_still_works()
    {
        var (cn, ctx, t1) = await SeedTwoEras();
        using var _cn = cn; await using var _ctx = ctx;
        _ = t1;
        // No AsOf → the live shaped-collection projection is unaffected by the guard.
        var rows = ctx.Query<Parent>()
            .Select(p => new { p.Id, Vals = p.Children.Where(c => c.Val >= 15).Select(c => c.Val).ToList() })
            .ToList();
        Assert.Equal(new[] { 20, 100 }, rows.Single().Vals.OrderBy(v => v).ToArray());   // live era: c1=100, c2=20
    }
}
