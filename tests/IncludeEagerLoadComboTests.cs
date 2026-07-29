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
/// Batch 4: multi-level self-referencing ThenInclude, filtered+ordered+top-N combined on one
/// level (filter must live INSIDE the ROW_NUMBER window), and the genuine projection-split path
/// (Select with collection => dependent queries) with paging+filter+ordering combined.
/// </summary>
[Trait("Category", "Fast")]
public class IncludeEagerLoadComboTests
{
    [Table("HC_Blog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("HC_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public bool Published { get; set; }
        public int Rank { get; set; }
    }

    [Table("HC_Cat")]
    public class Category
    {
        [Key] public int Id { get; set; }
        public int? ParentId { get; set; }
        public string Name { get; set; } = "";
        public List<Category> Children { get; set; } = new();
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HC_Blog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE HC_Post (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL, Published INTEGER NOT NULL, Rank INTEGER NOT NULL);
                CREATE TABLE HC_Cat (Id INTEGER PRIMARY KEY, ParentId INTEGER NULL, Name TEXT NOT NULL);

                INSERT INTO HC_Blog VALUES (1,'B1'),(2,'B2'),(3,'B3'),(4,'B4');
                -- Blog1: p1(pub,r3), p2(unpub,r1), p7(pub,r9)
                -- Blog2: p3(pub,r2)
                -- Blog3: none
                -- Blog4: p4(pub,r5), p5(pub,r4), p6(unpub,r6)
                INSERT INTO HC_Post VALUES
                    (1,1,'P1',1,3),(2,1,'P2',0,1),(7,1,'P7',1,9),
                    (3,2,'P3',1,2),
                    (4,4,'P4',1,5),(5,4,'P5',1,4),(6,4,'P6',0,6);

                -- Category tree: 1(root) -> 2,3 ; 2 -> 4,5 ; 4 -> 6 ; 3,5,6 leaves
                INSERT INTO HC_Cat VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'a1'),(5,2,'a2'),(6,4,'a1x');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Category>().HasKey(c => c.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
                mb.Entity<Category>().HasMany(c => c.Children).WithOne().HasForeignKey(c => c.ParentId!, c => c.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static INormQueryable<T> Q<T>(DbContext ctx) where T : class => (INormQueryable<T>)ctx.Query<T>();

    // ---- Multi-level self-referencing ThenInclude (Children.ThenInclude(Children)) ----
    [Fact]
    public async Task SelfRef_TwoLevel_ThenInclude()
    {
        foreach (var noTrack in new[] { false, true })
        {
            await using var ctx = Make();
            var q = Q<Category>(ctx);
            var iq = noTrack ? q.AsNoTracking() : q;
            var cats = await iq
                .Where(c => c.ParentId == null) // roots only
                .Include(c => c.Children).ThenInclude(c => c.Children)
                .ToListAsync();

            var root = Assert.Single(cats);
            Assert.Equal(new[] { 2, 3 }, root.Children.Select(c => c.Id).OrderBy(x => x).ToArray());
            var a = root.Children.First(c => c.Id == 2);
            Assert.Equal(new[] { 4, 5 }, a.Children.Select(c => c.Id).OrderBy(x => x).ToArray());
            var b = root.Children.First(c => c.Id == 3);
            Assert.Empty(b.Children);
        }
    }

    // ---- Self-ref Include where children are ALSO roots (tracking identity) ----
    [Fact]
    public async Task SelfRef_ChildrenAlsoRoots_Tracked_SharedInstances()
    {
        await using var ctx = Make();
        var cats = await Q<Category>(ctx).Include(c => c.Children).OrderBy(c => c.Id).ToListAsync();
        var byId = cats.ToDictionary(c => c.Id);
        Assert.Equal(new[] { 2, 3 }, byId[1].Children.Select(c => c.Id).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 4, 5 }, byId[2].Children.Select(c => c.Id).OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 6 }, byId[4].Children.Select(c => c.Id).ToArray());
        // Category 2 appears both as a root and as a child of category 1: same tracked instance.
        var cat2AsChild = byId[1].Children.First(c => c.Id == 2);
        Assert.Same(byId[2], cat2AsChild);
        // And that shared instance carries its own children.
        Assert.Equal(new[] { 4, 5 }, cat2AsChild.Children.Select(c => c.Id).OrderBy(x => x).ToArray());
    }

    // ---- Filtered + ordered + top-N combined on one level: filter must be inside the window ----
    [Fact]
    public async Task FilteredOrderedTopN_Combined()
    {
        foreach (var noTrack in new[] { false, true })
        {
            await using var ctx = Make();
            var q = Q<Blog>(ctx);
            var iq = noTrack ? q.AsNoTracking() : q;
            // Only published posts, ordered by rank desc, take 1 per blog.
            var blogs = await iq
                .Include(b => b.Posts.Where(p => p.Published).OrderByDescending(p => p.Rank).Take(1))
                .OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);
            // Blog1 published: p1(r3), p7(r9) -> top1 desc = p7
            Assert.Equal(new[] { 7 }, byId[1].Posts.Select(p => p.Id).ToArray());
            // Blog2 published: p3(r2) -> p3
            Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
            // Blog3: none
            Assert.Empty(byId[3].Posts);
            // Blog4 published: p4(r5), p5(r4) -> top1 desc = p4  (p6 unpublished r6 excluded — the classic
            // cap-then-filter bug would keep p6)
            Assert.Equal(new[] { 4 }, byId[4].Posts.Select(p => p.Id).ToArray());
        }
    }

    // ---- Projection-split: paging + per-parent ordered top-N combined ----
    [Fact]
    public async Task ProjectionSplit_Paging_And_OrderedTopN()
    {
        await using var ctx = Make();
        var rows = await Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id).Skip(0).Take(2) // blogs 1,2
            .Select(b => new
            {
                b.Id,
                Top = b.Posts.Where(p => p.Published).OrderByDescending(p => p.Rank).Take(1).ToList()
            })
            .ToListAsync();

        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
        var byId = rows.ToDictionary(r => r.Id);
        Assert.Equal(new[] { 7 }, byId[1].Top.Select(p => p.Id).ToArray());
        Assert.Equal(new[] { 3 }, byId[2].Top.Select(p => p.Id).ToArray());
    }

    // ---- Projection-split: TWO different navigations projected + element projection ----
    [Fact]
    public async Task ProjectionSplit_ElementProjection_WithPaging()
    {
        await using var ctx = Make();
        var rows = await Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id).Skip(3).Take(1) // blog 4
            .Select(b => new
            {
                b.Id,
                Titles = b.Posts.Where(p => p.Published).Select(p => p.Title).ToList()
            })
            .ToListAsync();

        var r = Assert.Single(rows);
        Assert.Equal(4, r.Id);
        Assert.Equal(new[] { "P4", "P5" }, r.Titles.OrderBy(x => x).ToArray());
    }

    // ---- Projection-split: parent with zero matching children => empty list, parent present ----
    [Fact]
    public async Task ProjectionSplit_ZeroChildren_EmptyList()
    {
        await using var ctx = Make();
        var rows = await Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id)
            .Select(b => new { b.Id, Posts = b.Posts.Where(p => p.Rank > 100).ToList() })
            .ToListAsync();
        Assert.All(rows, r => Assert.Empty(r.Posts));
        Assert.Equal(4, rows.Count);
    }
}
