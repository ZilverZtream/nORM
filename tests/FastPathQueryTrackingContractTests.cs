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
/// Contract: entities materialized through the fast-path query executor join the change tracker
/// exactly like the full pipeline. The fast path serves the MOST COMMON query shapes - simple
/// property-equality Where lists, plain Take lists, and filtered/ordered pages - and previously
/// returned entities that were invisible to SaveChanges: load-by-id, mutate, SaveChanges was a
/// silent no-op (0 rows affected, edit lost). These tests pin tracking across every fast-path
/// list shape, the AsNoTracking and NoTracking-context opt-outs, identity-map canonicalization,
/// and the end-to-end update round-trip that the bug silently dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FastPathQueryTrackingContractTests
{
    [Table("FpTrack_Item")]
    private class Item
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(QueryTrackingBehavior behavior = QueryTrackingBehavior.TrackAll)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE FpTrack_Item (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO FpTrack_Item VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Item>(),
            DefaultTrackingBehavior = behavior
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx);
    }

    [Fact]
    public async Task Simple_where_list_results_are_tracked()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        var items = await ctx.Query<Item>().Where(i => i.Id == 1).ToListAsync();
        Assert.Single(items);
        Assert.Contains(ctx.ChangeTracker.Entries, e => ReferenceEquals(e.Entity, items[0]));
    }

    [Fact]
    public async Task Simple_take_list_results_are_tracked()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        var items = await ctx.Query<Item>().Take(2).ToListAsync();
        Assert.Equal(2, items.Count);
        Assert.Equal(2, ctx.ChangeTracker.Entries.Count(e => e.Entity is Item));
    }

    [Fact]
    public async Task Filtered_ordered_page_list_results_are_tracked()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        var items = await ctx.Query<Item>()
            .Where(i => i.V >= 10)
            .OrderBy(i => i.Id)
            .Skip(1)
            .Take(2)
            .ToListAsync();
        Assert.Equal(2, items.Count);
        Assert.Equal(2, ctx.ChangeTracker.Entries.Count(e => e.Entity is Item));
    }

    [Fact]
    public void Sync_simple_where_list_results_are_tracked()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        var items = ctx.Query<Item>().Where(i => i.Id == 2).ToList();
        Assert.Single(items);
        Assert.Contains(ctx.ChangeTracker.Entries, e => ReferenceEquals(e.Entity, items[0]));
    }

    [Fact]
    public async Task Loaded_entity_edit_persists_through_savechanges()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        // The silent-loss scenario: load by key, mutate, save.
        var item = (await ctx.Query<Item>().Where(i => i.Id == 1).ToListAsync()).Single();
        item.V = 99;
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT V FROM FpTrack_Item WHERE Id = 1;";
        Assert.Equal(99L, check.ExecuteScalar());
    }

    [Fact]
    public async Task AsNoTracking_opts_fast_path_results_out_of_tracking()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        var byId = await ((INormQueryable<Item>)ctx.Query<Item>()).AsNoTracking().Where(i => i.Id == 1).ToListAsync();
        Assert.Single(byId);
        var page = await ((INormQueryable<Item>)ctx.Query<Item>()).AsNoTracking().Take(2).ToListAsync();
        Assert.Equal(2, page.Count);

        Assert.Empty(ctx.ChangeTracker.Entries);
    }

    [Fact]
    public async Task NoTracking_context_default_keeps_fast_path_results_untracked()
    {
        var (cn, ctx) = CreateContext(QueryTrackingBehavior.NoTracking);
        using var _cn = cn;
        using var _ = ctx;

        var items = await ctx.Query<Item>().Where(i => i.Id == 1).ToListAsync();
        Assert.Single(items);
        Assert.Empty(ctx.ChangeTracker.Entries);
    }

    [Fact]
    public async Task Fast_path_results_are_identity_mapped_to_the_tracked_instance()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ = ctx;

        var first = (await ctx.Query<Item>().Where(i => i.Id == 3).ToListAsync()).Single();
        var second = (await ctx.Query<Item>().Where(i => i.Id == 3).ToListAsync()).Single();

        // Same PK loaded twice through the fast path must resolve to the SAME tracked instance,
        // matching the full pipeline's identity-map behavior.
        Assert.Same(first, second);
    }
}
