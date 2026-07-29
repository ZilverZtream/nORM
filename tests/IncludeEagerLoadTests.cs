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
/// Include / AsSplitQuery eager-loading correctness. For each shape the graph is loaded via Include (single)
/// AND .AsSplitQuery(), asserting BOTH match the raw-DB truth (same parents, same children per parent, counts,
/// order) — no dropped / duplicated / mis-associated related entity. SQLite :memory: only.
/// </summary>
[Trait("Category", "Fast")]
public class IncludeEagerLoadTests
{
    [Table("HB_Blog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("HB_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public bool Published { get; set; }
        public int Rank { get; set; }
        public int? AuthorId { get; set; }
        public Author? Author { get; set; }
        public List<Comment> Comments { get; set; } = new();
    }

    [Table("HB_Comment")]
    public class Comment
    {
        [Key] public int Id { get; set; }
        public int PostId { get; set; }
        public string Text { get; set; } = "";
    }

    [Table("HB_Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Label { get; set; } = "";
    }

    [Table("HB_Author")]
    public class Author
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // Self-referencing category tree.
    [Table("HB_Cat")]
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
                CREATE TABLE HB_Blog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE HB_Post (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL, Published INTEGER NOT NULL, Rank INTEGER NOT NULL, AuthorId INTEGER NULL);
                CREATE TABLE HB_Comment (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                CREATE TABLE HB_Tag (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Label TEXT NOT NULL);
                CREATE TABLE HB_Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE HB_Cat (Id INTEGER PRIMARY KEY, ParentId INTEGER NULL, Name TEXT NOT NULL);

                INSERT INTO HB_Blog VALUES (1,'B1'),(2,'B2'),(3,'B3'),(4,'B4');

                INSERT INTO HB_Author VALUES (100,'Ada'),(200,'Grace');

                -- Blog1: posts 1(pub,rank3,Ada), 2(unpub,rank1,Grace)
                -- Blog2: post 3(pub,rank2,Ada)
                -- Blog3: NO posts (empty)
                -- Blog4: posts 4(pub,rank5), 5(pub,rank4), 6(unpub,rank6)
                INSERT INTO HB_Post VALUES
                    (1,1,'P1',1,3,100),
                    (2,1,'P2',0,1,200),
                    (3,2,'P3',1,2,100),
                    (4,4,'P4',1,5,NULL),
                    (5,4,'P5',1,4,NULL),
                    (6,4,'P6',0,6,NULL);

                -- Comments: Post1 -> c1,c2 ; Post3 -> c3 ; others none
                INSERT INTO HB_Comment VALUES
                    (10,1,'c1'),(11,1,'c2'),(12,3,'c3');

                -- Tags: Blog1 -> t1,t2 ; Blog4 -> t3 ; Blog2/3 none
                INSERT INTO HB_Tag VALUES (20,1,'t1'),(21,1,'t2'),(22,4,'t3');

                -- Category tree: 1(root) -> 2,3 ; 2 -> 4 ; 3,4 leaves
                INSERT INTO HB_Cat VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'a1');
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
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Author>().HasKey(a => a.Id);
                mb.Entity<Category>().HasKey(c => c.Id);

                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
                mb.Entity<Blog>().HasMany(b => b.Tags).WithOne().HasForeignKey(t => t.BlogId, b => b.Id);
                mb.Entity<Post>().HasMany(p => p.Comments).WithOne().HasForeignKey(c => c.PostId, p => p.Id);
                mb.Entity<Category>().HasMany(c => c.Children).WithOne().HasForeignKey(c => c.ParentId!, c => c.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static INormQueryable<T> Q<T>(DbContext ctx) where T : class => (INormQueryable<T>)ctx.Query<T>();

    // ---- Surface 1: single collection Include, split vs single ----
    [Fact]
    public async Task S1_SingleCollection_SplitVsSingle_MatchesRaw()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts);
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).ToListAsync();

            Assert.Equal(new[] { 1, 2, 3, 4 }, blogs.Select(b => b.Id).ToArray());
            var byId = blogs.ToDictionary(b => b.Id);
            Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
            Assert.Empty(byId[3].Posts); // Surface 8: zero children, parent present w/ empty
            Assert.Equal(new[] { 4, 5, 6 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
            // no cross-contamination
            Assert.All(blogs, b => Assert.All(b.Posts, p => Assert.Equal(b.Id, p.BlogId)));
        }
    }

    // ---- Surface 2: PAGINATED parent + collection Include (the sharpest probe) ----
    [Fact]
    public async Task S2_PaginatedParent_SplitVsSingle_ScopesChildrenToPagedParents()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts);
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).Skip(1).Take(2).ToListAsync();

            // Paged parents = blogs 2 and 3.
            Assert.Equal(new[] { 2, 3 }, blogs.Select(b => b.Id).ToArray());
            var byId = blogs.ToDictionary(b => b.Id);
            Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
            Assert.Empty(byId[3].Posts);
        }
    }

    // ---- Surface 3: TWO collection Includes on one parent ----
    [Fact]
    public async Task S3_TwoCollectionIncludes_NoCrossContamination()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts).Include(b => b.Tags);
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);

            Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 20, 21 }, byId[1].Tags.Select(t => t.Id).OrderBy(x => x).ToArray());
            Assert.Empty(byId[2].Tags);
            Assert.Equal(new[] { 22 }, byId[4].Tags.Select(t => t.Id).ToArray());
            Assert.Equal(new[] { 4, 5, 6 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
        }
    }

    // ---- Surface 4: multi-level ThenInclude ----
    [Fact]
    public async Task S4_ThenInclude_GrandchildrenNestedCorrectly()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts).ThenInclude(p => p.Comments);
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);

            var post1 = byId[1].Posts.First(p => p.Id == 1);
            Assert.Equal(new[] { 10, 11 }, post1.Comments.Select(c => c.Id).OrderBy(x => x).ToArray());
            var post2 = byId[1].Posts.First(p => p.Id == 2);
            Assert.Empty(post2.Comments);
            var post3 = byId[2].Posts.First(p => p.Id == 3);
            Assert.Equal(new[] { 12 }, post3.Comments.Select(c => c.Id).ToArray());
            // Blog4 posts have no comments
            Assert.All(byId[4].Posts, p => Assert.Empty(p.Comments));
        }
    }

    // ---- Surface 4b: multi-level ThenInclude WITH parent paging ----
    [Fact]
    public async Task S4b_ThenInclude_WithParentPaging()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts).ThenInclude(p => p.Comments);
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).Skip(0).Take(2).ToListAsync(); // blogs 1,2

            Assert.Equal(new[] { 1, 2 }, blogs.Select(b => b.Id).ToArray());
            var byId = blogs.ToDictionary(b => b.Id);
            Assert.Equal(new[] { 10, 11 }, byId[1].Posts.First(p => p.Id == 1).Comments.Select(c => c.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 12 }, byId[2].Posts.First(p => p.Id == 3).Comments.Select(c => c.Id).ToArray());
        }
    }

    // ---- Surface 5: filtered Include (only matching children) ----
    [Fact]
    public async Task S5_FilteredInclude_OnlyMatchingChildrenPerParent()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts.Where(p => p.Published));
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);

            Assert.Equal(new[] { 1 }, byId[1].Posts.Select(p => p.Id).ToArray()); // only published post 1
            Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
            Assert.Empty(byId[3].Posts);
            Assert.Equal(new[] { 4, 5 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray()); // 6 unpublished excluded
        }
    }

    // ---- Surface 5b: filtered Include with CLOSURE variable + paging ----
    [Fact]
    public async Task S5b_FilteredInclude_ClosureVar_WithPaging()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            int minRank = 4;
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts.Where(p => p.Rank >= minRank));
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).Skip(3).Take(1).ToListAsync(); // blog 4 only
            Assert.Equal(new[] { 4 }, blogs.Select(b => b.Id).ToArray());
            Assert.Equal(new[] { 4, 5, 6 }, blogs[0].Posts.Select(p => p.Id).OrderBy(x => x).ToArray()); // ranks 5,4,6 all >=4
        }
    }

    // ---- Surface 6: WHERE on parent + Include ----
    [Fact]
    public async Task S6_ParentWhere_ChildrenOnlyForMatchingParents()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts);
            if (split) q = q.AsSplitQuery();
            var blogs = await q.Where(b => b.Id == 1 || b.Id == 4).OrderBy(b => b.Id).ToListAsync();
            Assert.Equal(new[] { 1, 4 }, blogs.Select(b => b.Id).ToArray());
            var byId = blogs.ToDictionary(b => b.Id);
            Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 4, 5, 6 }, byId[4].Posts.Select(p => p.Id).OrderBy(x => x).ToArray());
        }
    }

    // ---- Surface 7: reference nav Include mixed with collection Include ----
    [Fact]
    public async Task S7_ReferenceNav_MixedWithCollection()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Post> q = Q<Post>(ctx).AsNoTracking().Include(p => p.Author).Include(p => p.Comments);
            if (split) q = q.AsSplitQuery();
            var posts = await q.OrderBy(p => p.Id).ToListAsync();
            var byId = posts.ToDictionary(p => p.Id);

            Assert.Equal("Ada", byId[1].Author?.Name);
            Assert.Equal("Grace", byId[2].Author?.Name);
            Assert.Equal("Ada", byId[3].Author?.Name);
            Assert.Null(byId[4].Author); // AuthorId NULL
            Assert.Equal(new[] { 10, 11 }, byId[1].Comments.Select(c => c.Id).OrderBy(x => x).ToArray());
            Assert.Empty(byId[2].Comments);
        }
    }

    // ---- Surface 10: ordered / top-N Include ----
    [Fact]
    public async Task S10_OrderedTopN_Include_PerParent()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Blog> q = Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts.OrderByDescending(p => p.Rank).Take(2));
            if (split) q = q.AsSplitQuery();
            var blogs = await q.OrderBy(b => b.Id).ToListAsync();
            var byId = blogs.ToDictionary(b => b.Id);

            // Blog1 posts ranks: p1=3, p2=1 -> top2 desc = p1(3), p2(1)
            Assert.Equal(new[] { 1, 2 }, byId[1].Posts.Select(p => p.Id).ToArray());
            // Blog2: p3 rank2 -> [3]
            Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
            // Blog3: none
            Assert.Empty(byId[3].Posts);
            // Blog4 ranks: p4=5,p5=4,p6=6 -> top2 desc = p6(6), p4(5)
            Assert.Equal(new[] { 6, 4 }, byId[4].Posts.Select(p => p.Id).ToArray());
        }
    }

    // ---- Surface 11: self-referencing Include ----
    [Fact]
    public async Task S11_SelfReferencing_Include()
    {
        foreach (var split in new[] { false, true })
        {
            await using var ctx = Make();
            INormQueryable<Category> q = Q<Category>(ctx).AsNoTracking().Include(c => c.Children);
            if (split) q = q.AsSplitQuery();
            var cats = await q.OrderBy(c => c.Id).ToListAsync();
            var byId = cats.ToDictionary(c => c.Id);

            Assert.Equal(new[] { 2, 3 }, byId[1].Children.Select(c => c.Id).OrderBy(x => x).ToArray());
            Assert.Equal(new[] { 4 }, byId[2].Children.Select(c => c.Id).ToArray());
            Assert.Empty(byId[3].Children);
            Assert.Empty(byId[4].Children);
        }
    }

    // ---- Genuine SPLIT path (shaped projection) with paging: dependent-query stitch ----
    [Fact]
    public async Task SP_ShapedProjection_CollectionSplit_WithPaging()
    {
        await using var ctx = Make();
        // Shaped projection => triggers the real dependent-query (split) path.
        var rows = await Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id).Skip(1).Take(2)
            .Select(b => new { b.Id, Posts = b.Posts.ToList() })
            .ToListAsync();

        Assert.Equal(new[] { 2, 3 }, rows.Select(r => r.Id).ToArray());
        var byId = rows.ToDictionary(r => r.Id);
        Assert.Equal(new[] { 3 }, byId[2].Posts.Select(p => p.Id).ToArray());
        Assert.Empty(byId[3].Posts);
    }

    // ---- Genuine SPLIT path: ordered/top-N shaped collection with paging ----
    [Fact]
    public async Task SP_ShapedProjection_OrderedTopN_WithPaging()
    {
        await using var ctx = Make();
        var rows = await Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id)
            .Select(b => new { b.Id, Top = b.Posts.OrderByDescending(p => p.Rank).Take(2).ToList() })
            .ToListAsync();
        var byId = rows.ToDictionary(r => r.Id);
        Assert.Equal(new[] { 1, 2 }, byId[1].Top.Select(p => p.Id).ToArray());
        Assert.Equal(new[] { 6, 4 }, byId[4].Top.Select(p => p.Id).ToArray());
        Assert.Empty(byId[3].Top);
    }
}
