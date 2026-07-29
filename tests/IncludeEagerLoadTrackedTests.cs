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
/// Batch 2: TRACKED Include eager loading (exercises the snapshot capture/merge branch in
/// ProcessLevel that AsNoTracking skips), ordered Skip+Take windows, M2M include, tracked
/// re-run, and duplicate parents. Oracle = raw DB truth.
/// </summary>
[Trait("Category", "Fast")]
public class IncludeEagerLoadTrackedTests
{
    [Table("HT_Blog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("HT_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public bool Published { get; set; }
        public int Rank { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("HT_Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HT_Blog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE HT_Post (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL, Published INTEGER NOT NULL, Rank INTEGER NOT NULL);
                CREATE TABLE HT_Tag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                CREATE TABLE HT_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY(PostId, TagId));

                INSERT INTO HT_Blog VALUES (1,'B1'),(2,'B2'),(3,'B3'),(4,'B4');
                -- Blog1: p1(pub,rank3), p2(unpub,rank1)
                -- Blog2: p3(pub,rank2)
                -- Blog3: none
                -- Blog4: p4(pub,rank5), p5(pub,rank4), p6(unpub,rank6)
                INSERT INTO HT_Post VALUES
                    (1,1,'P1',1,3),(2,1,'P2',0,1),(3,2,'P3',1,2),
                    (4,4,'P4',1,5),(5,4,'P5',1,4),(6,4,'P6',0,6);
                INSERT INTO HT_Tag VALUES (10,'red'),(11,'green'),(12,'blue');
                -- Post1 -> red,green ; Post2 -> red ; Post3 -> [] ; Post4 -> blue
                INSERT INTO HT_PostTag VALUES (1,10),(1,11),(2,10),(4,12);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
                mb.Entity<Post>().HasMany(p => p.Tags).WithMany().UsingTable("HT_PostTag", "PostId", "TagId");
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static INormQueryable<T> Q<T>(DbContext ctx) where T : class => (INormQueryable<T>)ctx.Query<T>();

    // ---- TRACKED single collection Include ----
    [Fact]
    public async Task Tracked_SingleCollection_MatchesRaw()
    {
        await using var ctx = Make();
        var blogs = await Q<Blog>(ctx).Include(b => b.Posts).OrderBy(b => b.Id).ToListAsync();
        var byId = blogs.ToDictionary(b => b.Id);
        Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
        Assert.Empty(byId[3].Posts);
        Assert.Equal(new[] { 4, 5, 6 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
        Assert.All(blogs, b => Assert.All(b.Posts, p => Assert.Equal(b.Id, p.BlogId)));
    }

    // ---- TRACKED paginated parent + Include ----
    [Fact]
    public async Task Tracked_PaginatedParent_ScopesChildren()
    {
        await using var ctx = Make();
        var blogs = await Q<Blog>(ctx).Include(b => b.Posts).OrderBy(b => b.Id).Skip(1).Take(2).ToListAsync();
        Assert.Equal(new[] { 2, 3 }, blogs.Select(b => b.Id).ToArray());
        var byId = blogs.ToDictionary(b => b.Id);
        Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
        Assert.Empty(byId[3].Posts);
    }

    // ---- TRACKED filtered Include ----
    [Fact]
    public async Task Tracked_FilteredInclude_OnlyMatching()
    {
        await using var ctx = Make();
        var blogs = await Q<Blog>(ctx).Include(b => b.Posts.Where(p => p.Published)).OrderBy(b => b.Id).ToListAsync();
        var byId = blogs.ToDictionary(b => b.Id);
        Assert.Equal(new[] { 1 }, byId[1].Posts.Select(p => p.Id).ToArray());
        Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
        Assert.Empty(byId[3].Posts);
        Assert.Equal(new[] { 4, 5 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }

    // ---- TRACKED re-run of the SAME Include on the SAME context (snapshot-merge path) ----
    [Fact]
    public async Task Tracked_RerunSameInclude_NoDuplication()
    {
        await using var ctx = Make();
        var first = await Q<Blog>(ctx).Include(b => b.Posts).OrderBy(b => b.Id).ToListAsync();
        // Re-run: parents already tracked, collections already have snapshots.
        var second = await Q<Blog>(ctx).Include(b => b.Posts).OrderBy(b => b.Id).ToListAsync();
        var byId = second.ToDictionary(b => b.Id);
        Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 4, 5, 6 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
        // No duplicated children after re-run.
        Assert.Equal(2, byId[1].Posts.Count);
        Assert.Equal(3, byId[4].Posts.Count);
        Assert.Empty(byId[3].Posts);
    }

    // ---- TRACKED re-run: first UNFILTERED, then FILTERED (narrower). Should reflect fresh set. ----
    [Fact]
    public async Task Tracked_Rerun_UnfilteredThenFiltered()
    {
        await using var ctx = Make();
        var first = await Q<Blog>(ctx).Include(b => b.Posts).OrderBy(b => b.Id).ToListAsync();
        Assert.Equal(3, first.First(b => b.Id == 4).Posts.Count);
        // Now re-run with a filter that keeps only published. Blog4 should now show 2 (p4,p5).
        var second = await Q<Blog>(ctx).Include(b => b.Posts.Where(p => p.Published)).OrderBy(b => b.Id).ToListAsync();
        var b4 = second.First(b => b.Id == 4);
        Assert.Equal(new[] { 4, 5 }, b4.Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }

    // ---- Ordered Skip+Take window per parent ----
    [Fact]
    public async Task OrderedInclude_SkipTake_PerParentWindow()
    {
        foreach (var noTrack in new[] { false, true })
        {
            await using var ctx = Make();
            var q = Q<Blog>(ctx);
            var iq = noTrack ? q.AsNoTracking() : q;
            // Order posts by Rank ASC, skip 1 take 1 => the 2nd-lowest-rank post per blog.
            var blogs = await iq.Include(b => b.Posts.OrderBy(p => p.Rank).Skip(1).Take(1)).OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);
            // Blog1 ranks: p1=3,p2=1 -> asc [p2(1),p1(3)] skip1 take1 => p1
            Assert.Equal(new[] { 1 }, byId[1].Posts.Select(p => p.Id).ToArray());
            // Blog2: only p3 -> skip1 => empty
            Assert.Empty(byId[2].Posts);
            // Blog3: none
            Assert.Empty(byId[3].Posts);
            // Blog4 ranks: p4=5,p5=4,p6=6 -> asc [p5(4),p4(5),p6(6)] skip1 take1 => p4
            Assert.Equal(new[] { 4 }, byId[4].Posts.Select(p => p.Id).ToArray());
        }
    }

    // ---- M2M Include: correctness, empty, cross-contamination ----
    [Fact]
    public async Task M2M_Include_MatchesRaw()
    {
        foreach (var noTrack in new[] { false, true })
        {
            await using var ctx = Make();
            var q = Q<Post>(ctx);
            var iq = noTrack ? q.AsNoTracking() : q;
            var posts = await iq.Include(p => p.Tags).OrderBy(p => p.Id).ToListAsync();
            var byId = posts.ToDictionary(p => p.Id);
            Assert.Equal(new[] { 10, 11 }, byId[1].Tags.Select(t => t.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 10 }, byId[2].Tags.Select(t => t.Id).ToArray());
            Assert.Empty(byId[3].Tags); // no tags
            Assert.Equal(new[] { 12 }, byId[4].Tags.Select(t => t.Id).ToArray());
            Assert.Empty(byId[5].Tags);
            Assert.Empty(byId[6].Tags);
        }
    }

    // ---- M2M Include with parent paging ----
    [Fact]
    public async Task M2M_Include_WithPaging()
    {
        await using var ctx = Make();
        var posts = await Q<Post>(ctx).AsNoTracking().Include(p => p.Tags)
            .OrderBy(p => p.Id).Skip(1).Take(2).ToListAsync(); // posts 2,3
        Assert.Equal(new[] { 2, 3 }, posts.Select(p => p.Id).ToArray());
        var byId = posts.ToDictionary(p => p.Id);
        Assert.Equal(new[] { 10 }, byId[2].Tags.Select(t => t.Id).ToArray());
        Assert.Empty(byId[3].Tags);
    }

    // ---- M2M shared right entity across parents (identity) ----
    [Fact]
    public async Task M2M_SharedTag_AcrossPosts()
    {
        await using var ctx = Make();
        var posts = await Q<Post>(ctx).Include(p => p.Tags).OrderBy(p => p.Id).ToListAsync();
        var byId = posts.ToDictionary(p => p.Id);
        // Tag 'red' (10) is shared by post1 and post2. Under tracking both should reference same instance.
        var redOn1 = byId[1].Tags.First(t => t.Id == 10);
        var redOn2 = byId[2].Tags.First(t => t.Id == 10);
        Assert.Same(redOn1, redOn2);
    }

    // ---- Duplicate parents via Concat + Include ----
    [Fact]
    public async Task DuplicateParents_Concat_Include()
    {
        await using var ctx = Make();
        try
        {
            var blogs = await Q<Blog>(ctx).AsNoTracking().Where(b => b.Id <= 2)
                .Concat(Q<Blog>(ctx).AsNoTracking().Where(b => b.Id <= 2))
                .Include(b => b.Posts)
                .ToListAsync();
            // Each occurrence must carry its own correct children; no child dropped/duplicated per instance.
            Assert.All(blogs, b => Assert.All(b.Posts, p => Assert.Equal(b.Id, p.BlogId)));
            foreach (var b in blogs.Where(x => x.Id == 1))
                Assert.Equal(new[] { 1, 2 }, b.Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
            foreach (var b in blogs.Where(x => x.Id == 2))
                Assert.Equal(new[] { 3 }, b.Posts.Select(p => p.Id).ToArray());
        }
        catch (NormException)
        {
            // Fail-loud is acceptable (unsupported shape); only silent-wrong is a bug.
        }
        catch (NotSupportedException)
        {
        }
    }
}
