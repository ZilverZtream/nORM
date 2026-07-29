using System;
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
/// Adversarial hunt for silent-wrong results in change tracking / identity map / AsNoTracking /
/// tracked-graph consistency on SQLite. Each test asserts against the EXPECTED tracked-graph or
/// fresh-DB state (reference equality via Assert.Same/NotSame, value freshness/staleness).
/// EF-consistent behavior: for a TRACKING query the identity map wins (returns the tracked instance,
/// pending changes not clobbered by DB values); AsNoTracking always materializes fresh.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IdentityMapTrackingConsistencyHuntTests
{
    [Table("ImcBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("ImcPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public Blog? Blog { get; set; }
    }

    private static DbContext Boot(SqliteConnection cn, QueryTrackingBehavior behavior = QueryTrackingBehavior.TrackAll)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE ImcBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE ImcPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL);" +
                "INSERT INTO ImcBlog VALUES (1,'blog-db'),(2,'blog2-db');" +
                "INSERT INTO ImcPost VALUES (10,1,'post-db'),(11,1,'post2-db'),(12,2,'post3-db');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            DefaultTrackingBehavior = behavior,
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne(p => p.Blog).HasForeignKey(p => p.BlogId, b => b.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static void RawUpdateBlogName(SqliteConnection cn, int id, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE ImcBlog SET Name = @n WHERE Id = @id";
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@id", id);
        cmd.ExecuteNonQuery();
    }

    // ── HUNT 1: identity resolution correctness ───────────────────────────────

    [Fact]
    public void Tracking_requery_returns_same_instance()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var a = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        var b = ctx.Query<Blog>().Where(x => x.Id == 1).First();
        Assert.Same(a, b);
    }

    [Fact]
    public void Tracking_requery_preserves_pending_change_not_clobbered_by_db()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var a = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        a.Name = "pending-unsaved";

        var b = ctx.Query<Blog>().Where(x => x.Id == 1).First();
        // EF identity map: the re-query returns the tracked instance; the pending unsaved change
        // must NOT be clobbered by the DB value.
        Assert.Same(a, b);
        Assert.Equal("pending-unsaved", b.Name);
    }

    // ── HUNT 2: AsNoTracking freshness ────────────────────────────────────────

    [Fact]
    public void AsNoTracking_returns_fresh_db_and_different_instance_while_tracked_modified()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var tracked = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        tracked.Name = "pending-unsaved";

        var fresh = ((INormQueryable<Blog>)ctx.Query<Blog>()).AsNoTracking().Where(b => b.Id == 1).First();
        Assert.NotSame(tracked, fresh);
        Assert.Equal("blog-db", fresh.Name); // fresh DB value, NOT the pending tracked change
    }

    [Fact]
    public void Two_AsNoTracking_queries_return_different_instances()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var a = ((INormQueryable<Blog>)ctx.Query<Blog>()).AsNoTracking().Where(b => b.Id == 1).First();
        var b = ((INormQueryable<Blog>)ctx.Query<Blog>()).AsNoTracking().Where(b => b.Id == 1).First();
        Assert.NotSame(a, b);
    }

    // ── HUNT 5: AsNoTracking must NOT pollute the identity map ─────────────────

    [Fact]
    public void AsNoTracking_does_not_pollute_identity_map()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        // No-tracking query first — must not enter the identity map.
        var noTrack = ((INormQueryable<Blog>)ctx.Query<Blog>()).AsNoTracking().Where(b => b.Id == 1).First();
        noTrack.Name = "no-track-mutated";

        // A later tracking query of the same row must load FRESH into the map, not adopt the no-tracking instance.
        var tracked = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        Assert.NotSame(noTrack, tracked);
        Assert.Equal("blog-db", tracked.Name); // fresh DB value, not the no-tracking mutation
        Assert.Contains(ctx.ChangeTracker.Entries, e => ReferenceEquals(e.Entity, tracked));
    }

    // ── HUNT 9: raw update staleness boundary (49A clean-bill) ─────────────────

    [Fact]
    public void Raw_update_then_tracking_query_returns_stale_tracked_but_noTracking_returns_fresh()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var tracked = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        Assert.Equal("blog-db", tracked.Name);

        RawUpdateBlogName(cn, 1, "raw-updated");

        // Tracking re-query: identity map wins → returns the tracked (stale) instance (EF-consistent).
        var reTracked = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        Assert.Same(tracked, reTracked);
        Assert.Equal("blog-db", reTracked.Name); // stale — matches EF identity-map semantics

        // AsNoTracking: fresh materialization sees the raw update.
        var fresh = ((INormQueryable<Blog>)ctx.Query<Blog>()).AsNoTracking().Where(b => b.Id == 1).First();
        Assert.Equal("raw-updated", fresh.Name);
    }

    // ── HUNT 6: Find identity-map short circuit ────────────────────────────────

    [Fact]
    public void Find_returns_tracked_instance_with_pending_change()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var a = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        a.Name = "pending-unsaved";
        var found = ctx.Find<Blog>(1);
        Assert.Same(a, found);
        Assert.Equal("pending-unsaved", found!.Name);
    }

    // ── HUNT 4: Include collection child ↔ direct-query child identity ─────────

    [Fact]
    public void Include_collection_child_resolves_to_tracked_child_with_pending_change()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        // Track a Post directly and mutate it (unsaved).
        var post = ctx.Query<Post>().Where(p => p.Id == 10).First();
        post.Title = "pending-title";

        // Now load the parent Blog with Include(Posts). The child in the collection must be the SAME
        // tracked instance carrying the pending change — not a fresh clobbering instance.
        var blog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        var included = blog.Posts.Single(p => p.Id == 10);
        Assert.Same(post, included);
        Assert.Equal("pending-title", included.Title);
    }

    // ── HUNT 3: reference-nav Include points to already-tracked parent ─────────

    [Fact]
    public void Reference_nav_include_points_to_tracked_parent_with_pending_change()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var blog = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        blog.Name = "pending-blog";

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Blog).Where(p => p.Id == 10).First();
        Assert.Same(blog, post.Blog);
        Assert.Equal("pending-blog", post.Blog!.Name);
    }

    // ── HUNT (fast path): First preserves pending change / identity ────────────

    [Fact]
    public void FastPath_first_preserves_pending_change()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var a = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        a.Name = "pending-unsaved";

        // Fast-path First(predicate) point read.
        var b = ctx.Query<Blog>().First(x => x.Id == 1);
        Assert.Same(a, b);
        Assert.Equal("pending-unsaved", b.Name);
    }

    // ── HUNT (streaming): AsAsyncEnumerable identity resolution ────────────────

    [Fact]
    public async Task Streaming_asyncenumerable_identity_resolves_to_tracked()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var a = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        a.Name = "pending-unsaved";

        Blog? streamed = null;
        await foreach (var blog in ((INormQueryable<Blog>)ctx.Query<Blog>()).Where(b => b.Id == 1).AsAsyncEnumerable())
            streamed = blog;

        Assert.Same(a, streamed);
        Assert.Equal("pending-unsaved", streamed!.Name);
    }

    // ── HUNT (GroupJoin): outer identity resolution ───────────────────────────

    [Fact]
    public void GroupJoin_outer_identity_resolves_to_tracked()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var blog = ctx.Query<Blog>().Where(b => b.Id == 1).First();
        blog.Name = "pending-blog";

        var grouped = ctx.Query<Blog>()
            .GroupJoin(ctx.Query<Post>(), b => b.Id, p => p.BlogId, (b, ps) => new { Blog = b, Count = ps.Count() })
            .Where(x => x.Blog.Id == 1)
            .ToList();

        var one = grouped.Single();
        Assert.Same(blog, one.Blog);
        Assert.Equal("pending-blog", one.Blog.Name);
        Assert.Equal(2, one.Count);
    }

    // ── HUNT 3 (fixup across two plain tracking queries) ──────────────────────

    [Fact]
    public void Include_collection_requery_after_unsaved_child_added_to_collection()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        // Load parent with its collection.
        var blog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        Assert.Equal(2, blog.Posts.Count);

        // Add an unsaved child to the tracked collection (a pending graph edit).
        blog.Posts.Add(new Post { Id = 999, BlogId = 1, Title = "unsaved-added" });
        Assert.Equal(3, blog.Posts.Count);

        // Re-query the same parent with Include. Diagnostic: does the unsaved added child survive?
        var reBlog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        Assert.Same(blog, reBlog);
        // Assert nORM's behavior (documents whether the collection was overwritten).
        Assert.Contains(reBlog.Posts, p => p.Id == 999);
    }

    // ── Baseline: a child Added to a tracked collection (no ctx.Add) IS persisted by SaveChanges ─

    [Fact]
    public async Task Baseline_add_unsaved_child_to_collection_then_save_inserts_it()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var blog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        blog.Posts.Add(new Post { Id = 998, BlogId = 1, Title = "added-baseline" });
        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM ImcPost WHERE Id = 998";
        Assert.Equal(1L, Convert.ToInt64(check.ExecuteScalar()));
    }

    // ── The silent data loss: an intervening Include re-query drops the pending Added child ───────

    [Fact]
    public async Task Requery_include_before_save_silently_drops_pending_added_child()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var blog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        // Pending graph edit: add a new child to the tracked collection (EF-idiomatic principal.Children.Add).
        blog.Posts.Add(new Post { Id = 999, BlogId = 1, Title = "unsaved-added" });

        // An intervening tracking Include re-query of the SAME tracked parent. In EF this preserves the
        // pending Added child (a query never removes a pending graph edit); nORM overwrites the collection.
        _ = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();

        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM ImcPost WHERE Id = 999";
        // EXPECTED (EF parity): 1 — the pending child is still inserted.
        // ACTUAL (nORM bug): 0 — the re-query overwrote the collection and the INSERT was silently dropped.
        Assert.Equal(1L, Convert.ToInt64(check.ExecuteScalar()));
    }

    // ── Reverse direction: an intervening Include re-query resurrects a pending REMOVED child ─────

    [Fact]
    public async Task Requery_include_before_save_silently_reverts_pending_child_removal()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var blog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        // Pending graph edit: remove a loaded child (disassociation). Delete it outright so a required
        // FK does not block the save — the intent is "post 11 no longer belongs / is gone".
        var post11 = blog.Posts.Single(p => p.Id == 11);
        blog.Posts.Remove(post11);
        ctx.Remove(post11);

        // Intervening Include re-query of the same tracked parent, BEFORE save.
        _ = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();

        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM ImcPost WHERE Id = 11";
        // The pending delete must still take effect (the re-query must not resurrect it).
        Assert.Equal(0L, Convert.ToInt64(check.ExecuteScalar()));
    }

    // ── Pure severance (no ctx.Remove): removing a loaded child from the collection is a pending ─
    // disassociation nORM detects via its load-time snapshot. An intervening Include re-query
    // overwrites BOTH the collection and the snapshot, silently reverting the severance. ─────────

    [Fact]
    public async Task Requery_include_before_save_silently_reverts_collection_severance()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var blog = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();
        // Sever post 11 by removing it from the loaded collection (no ctx.Remove) — nORM's snapshot
        // reconciliation would delete the orphan (required FK) on save.
        var post11 = blog.Posts.Single(p => p.Id == 11);
        blog.Posts.Remove(post11);

        // Intervening Include re-query of the same tracked parent, BEFORE save.
        _ = ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.Posts).Where(b => b.Id == 1).First();

        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM ImcPost WHERE Id = 11";
        // EXPECTED: 0 — the severance (orphan delete) still takes effect.
        // If the re-query resurrected post 11 into the collection + snapshot, this returns 1 (severance lost).
        Assert.Equal(0L, Convert.ToInt64(check.ExecuteScalar()));
    }
}
