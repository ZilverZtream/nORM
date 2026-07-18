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
/// Pins AsTracking() — the EF-parity per-query override of a <see cref="QueryTrackingBehavior.NoTracking"/>
/// context default: results are tracked even though the context would otherwise return them detached. It
/// composes last-wins with AsNoTracking, is a no-op on a default (tracking) context, and — critically — must
/// NOT re-enable tracking for a temporal (AsOf) snapshot in EITHER composition order, since AsOf forces
/// no-tracking as a safety invariant (a tracked snapshot could alias a live entity and return current state).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsTrackingContractTests
{
    [Table("AtWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Make(SqliteConnection cn, QueryTrackingBehavior tracking)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AtWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO AtWidget VALUES (1, 'a'), (2, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            DefaultTrackingBehavior = tracking,
            OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static int TrackedWidgets(DbContext ctx) => ctx.ChangeTracker.Entries.Count(e => e.Entity is Widget);

    [Fact]
    public void No_tracking_default_context_does_not_track()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn, QueryTrackingBehavior.NoTracking);

        var list = ctx.Query<Widget>().ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(0, TrackedWidgets(ctx));   // detached by the NoTracking default
    }

    [Fact]
    public void AsTracking_overrides_a_no_tracking_default_context()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn, QueryTrackingBehavior.NoTracking);

        var list = ctx.Query<Widget>().AsTracking().ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(2, TrackedWidgets(ctx));   // AsTracking forced tracking on
    }

    [Fact]
    public void AsTracking_is_a_noop_on_a_tracking_context()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn, QueryTrackingBehavior.TrackAll);

        var list = ctx.Query<Widget>().AsTracking().ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(2, TrackedWidgets(ctx));
    }

    [Fact]
    public void AsNoTracking_then_AsTracking_last_wins_tracks()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn, QueryTrackingBehavior.TrackAll);

        // AsTracking is written last (outermost) → it wins over the earlier AsNoTracking → tracked.
        var list = ((INormQueryable<Widget>)ctx.Query<Widget>()).AsNoTracking().AsTracking().ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(2, TrackedWidgets(ctx));
    }

    [Fact]
    public void AsTracking_then_AsNoTracking_last_wins_does_not_track()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Make(cn, QueryTrackingBehavior.TrackAll);

        // AsNoTracking is written last (outermost) → it wins over the earlier AsTracking → detached.
        var list = ctx.Query<Widget>().AsTracking().AsNoTracking().ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(0, TrackedWidgets(ctx));
    }

    // ── AsOf safety: AsTracking must never re-enable tracking for a temporal snapshot ──

    [Table("AtTParent")]
    public class TParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static async Task<(DbContext ctx, DateTime t1)> MakeTemporalAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AtTParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<TParent>().HasKey(p => p.Id) };
        opts.EnableTemporalVersioning();
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        ctx.Add(new TParent { Id = 1, Name = "old" });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        using var nowCmd = cn.CreateCommand();
        nowCmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await nowCmd.ExecuteScalarAsync())!;
        var t1 = DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        await Task.Delay(60);
        var p = ctx.Find<TParent>(1)!;
        p.Name = "renamed";
        await ctx.SaveChangesAsync();
        return (ctx, t1);
    }

    [Fact]
    public async Task AsOf_then_AsTracking_does_not_track_the_snapshot()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, t1) = await MakeTemporalAsync(cn);
        await using var _ctx = ctx;

        ctx.ChangeTracker.Clear();   // drop the seed entity tracked during setup so the count isolates the query
        var historic = await ((INormQueryable<TParent>)ctx.Query<TParent>()).AsOf(t1).AsTracking().ToListAsync();
        Assert.Equal("old", historic.Single().Name);                 // historical value
        Assert.Equal(0, ctx.ChangeTracker.Entries.Count(e => e.Entity is TParent));   // AsOf keeps it untracked
    }

    [Fact]
    public async Task AsTracking_then_AsOf_does_not_track_the_snapshot()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, t1) = await MakeTemporalAsync(cn);
        await using var _ctx = ctx;

        ctx.ChangeTracker.Clear();   // drop the seed entity tracked during setup so the count isolates the query
        // AsTracking is visited AFTER AsOf here — it must not clobber AsOf's forced no-tracking.
        var historic = await ((INormQueryable<TParent>)ctx.Query<TParent>()).AsTracking().AsOf(t1).ToListAsync();
        Assert.Equal("old", historic.Single().Name);
        Assert.Equal(0, ctx.ChangeTracker.Entries.Count(e => e.Entity is TParent));
    }
}
