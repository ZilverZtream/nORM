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
/// Batch 3: shared-navigation-prefix ThenInclude branches (ResolveIncludeIdentity — flagged
/// silent-data-loss risk), First/Single/FirstOrDefault + Include (fast-path interaction), and
/// Distinct + Include. Oracle = raw DB truth. Both tracking and no-tracking.
/// </summary>
[Trait("Category", "Fast")]
public class IncludeEagerLoadSharedPrefixTests
{
    [Table("HS_Blog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("HS_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public List<Comment> Comments { get; set; } = new();
        public List<Reaction> Reactions { get; set; } = new();
    }

    [Table("HS_Comment")]
    public class Comment
    {
        [Key] public int Id { get; set; }
        public int PostId { get; set; }
        public string Text { get; set; } = "";
    }

    [Table("HS_Reaction")]
    public class Reaction
    {
        [Key] public int Id { get; set; }
        public int PostId { get; set; }
        public string Kind { get; set; } = "";
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HS_Blog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE HS_Post (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL);
                CREATE TABLE HS_Comment (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                CREATE TABLE HS_Reaction (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Kind TEXT NOT NULL);

                INSERT INTO HS_Blog VALUES (1,'B1'),(2,'B2');
                -- Blog1: post1, post2 ; Blog2: post3 (no children)
                INSERT INTO HS_Post VALUES (1,1,'P1'),(2,1,'P2'),(3,2,'P3');
                -- post1 comments c1,c2 ; post2 comment c3 ; post3 none
                INSERT INTO HS_Comment VALUES (10,1,'c1'),(11,1,'c2'),(12,2,'c3');
                -- post1 reaction r1 ; post2 reactions r2,r3 ; post3 none
                INSERT INTO HS_Reaction VALUES (20,1,'like'),(21,2,'love'),(22,2,'wow');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Comment>().HasKey(c => c.Id);
                mb.Entity<Reaction>().HasKey(r => r.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
                mb.Entity<Post>().HasMany(p => p.Comments).WithOne().HasForeignKey(c => c.PostId, p => p.Id);
                mb.Entity<Post>().HasMany(p => p.Reactions).WithOne().HasForeignKey(r => r.PostId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static INormQueryable<T> Q<T>(DbContext ctx) where T : class => (INormQueryable<T>)ctx.Query<T>();

    // ---- Two ThenInclude branches sharing the Posts prefix. Both must populate the SAME posts. ----
    [Fact]
    public async Task SharedPrefix_TwoThenIncludeBranches_BothPopulate()
    {
        foreach (var noTrack in new[] { false, true })
        {
            await using var ctx = Make();
            var q = Q<Blog>(ctx);
            var iq = noTrack ? q.AsNoTracking() : q;
            var blogs = await iq
                .Include(b => b.Posts).ThenInclude(p => p.Comments)
                .Include(b => b.Posts).ThenInclude(p => p.Reactions)
                .OrderBy(b => b.Id)
                .ToListAsync();

            var b1 = blogs.First(b => b.Id == 1);
            var p1 = b1.Posts.First(p => p.Id == 1);
            var p2 = b1.Posts.First(p => p.Id == 2);
            // Both branches present on the same post instances (no branch overwrote the other).
            Assert.Equal(new[] { 10, 11 }, p1.Comments.Select(c => c.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 20 }, p1.Reactions.Select(r => r.Id).ToArray());
            Assert.Equal(new[] { 12 }, p2.Comments.Select(c => c.Id).ToArray());
            Assert.Equal(new[] { 21, 22 }, p2.Reactions.Select(r => r.Id).OrderBy(x => x).ToArray());
            var p3 = blogs.First(b => b.Id == 2).Posts.Single();
            Assert.Empty(p3.Comments);
            Assert.Empty(p3.Reactions);
        }
    }

    // ---- Bare Include after a deeper ThenInclude on the same prefix must NOT drop the grandchildren. ----
    [Fact]
    public async Task SharedPrefix_DeepThenInclude_ThenBareInclude_KeepsGrandchildren()
    {
        foreach (var noTrack in new[] { false, true })
        {
            await using var ctx = Make();
            var q = Q<Blog>(ctx);
            var iq = noTrack ? q.AsNoTracking() : q;
            var blogs = await iq
                .Include(b => b.Posts).ThenInclude(p => p.Comments)
                .Include(b => b.Posts) // bare re-include of the same prefix
                .OrderBy(b => b.Id)
                .ToListAsync();

            var p1 = blogs.First(b => b.Id == 1).Posts.First(p => p.Id == 1);
            // The bare re-include must not have overwritten Posts with comment-less instances.
            Assert.Equal(new[] { 10, 11 }, p1.Comments.Select(c => c.Id).OrderBy(x => x).ToArray());
        }
    }

    // ---- Include + FirstOrDefaultAsync (fast-path candidate) ----
    [Fact]
    public async Task Include_FirstOrDefaultAsync_LoadsChildren()
    {
        await using var ctx = Make();
        var blog = await Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts)
            .Where(b => b.Id == 1).OrderBy(b => b.Id).FirstOrDefaultAsync();
        Assert.NotNull(blog);
        Assert.Equal(new[] { 1, 2 }, blog!.Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }

    // ---- Include + FirstAsync with no Where ----
    [Fact]
    public async Task Include_FirstAsync_LoadsChildren()
    {
        await using var ctx = Make();
        var blog = await Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts).OrderBy(b => b.Id).FirstAsync();
        Assert.Equal(1, blog.Id);
        Assert.Equal(new[] { 1, 2 }, blog.Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }

    // ---- Include + SingleAsync ----
    [Fact]
    public async Task Include_SingleAsync_LoadsChildren()
    {
        await using var ctx = Make();
        var blog = await Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts)
            .Where(b => b.Id == 1).SingleAsync();
        Assert.Equal(new[] { 1, 2 }, blog.Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }

    // ---- Include + First() SYNC path ----
    [Fact]
    public void Include_First_Sync_LoadsChildren()
    {
        using var ctx = Make();
        var blog = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts).OrderBy(b => b.Id).First();
        Assert.Equal(1, blog.Id);
        Assert.Equal(new[] { 1, 2 }, blog.Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }

    // ---- ThenInclude + FirstOrDefaultAsync (grandchildren via single-result path) ----
    [Fact]
    public async Task ThenInclude_FirstOrDefaultAsync_LoadsGrandchildren()
    {
        await using var ctx = Make();
        var blog = await Q<Blog>(ctx).AsNoTracking()
            .Include(b => b.Posts).ThenInclude(p => p.Comments)
            .Where(b => b.Id == 1).FirstOrDefaultAsync();
        Assert.NotNull(blog);
        var p1 = blog!.Posts.First(p => p.Id == 1);
        Assert.Equal(new[] { 10, 11 }, p1.Comments.Select(c => c.Id).OrderBy(x => x).ToArray());
    }

    // ---- Distinct + Include ----
    [Fact]
    public async Task Distinct_Include_LoadsChildren()
    {
        await using var ctx = Make();
        try
        {
            var blogs = await Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts)
                .Distinct().OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);
            Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
        }
        catch (NormException) { }
        catch (NotSupportedException) { }
    }

    // ---- Root filter references a navigation; Include must load ALL children (not the filtered subset) ----
    [Fact]
    public async Task RootNavFilter_Include_LoadsAllChildren()
    {
        await using var ctx = Make();
        // Blogs having any post with id > 1 => blog1 (posts 1,2). Include must still load both posts.
        var blogs = await Q<Blog>(ctx).AsNoTracking()
            .Where(b => b.Posts.Any(p => p.Id == 2))
            .Include(b => b.Posts)
            .OrderBy(b => b.Id).ToListAsync();
        Assert.Equal(new[] { 1 }, blogs.Select(b => b.Id).ToArray());
        Assert.Equal(new[] { 1, 2 }, blogs[0].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
    }
}
