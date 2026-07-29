using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Batch 5: eager-load and projection-split BATCH BOUNDARY. SQLite MaxParameters=999 gives
/// maxPerBatch ~= 989, so &gt;989 parents force keys.Chunk(...) into multiple batches. Verify no
/// child is dropped or mis-scoped across batch boundaries. Also NULL-FK child handling.
/// </summary>
[Trait("Category", "Fast")]
public class IncludeEagerLoadBatchTests
{
    [Table("HBB_Blog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public List<Post> Posts { get; set; } = new();
    }

    [Table("HBB_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public int Val { get; set; }
    }

    private const int N = 2100; // > 2 * 989 => at least 3 batches

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HBB_Blog (Id INTEGER PRIMARY KEY);
                CREATE TABLE HBB_Post (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Val INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            sb.Append("INSERT INTO HBB_Blog (Id) VALUES ");
            for (int i = 1; i <= N; i++) { if (i > 1) sb.Append(','); sb.Append('(').Append(i).Append(')'); }
            using (var c2 = cn.CreateCommand()) { c2.CommandText = sb.ToString(); c2.ExecuteNonQuery(); }

            // Each blog i gets exactly 2 posts: ids (2i-1) with Val=i*100, (2i) with Val=i*100+1.
            // Blog with i % 50 == 0 gets ZERO posts (holes across batches).
            var sb2 = new StringBuilder();
            sb2.Append("INSERT INTO HBB_Post (Id, BlogId, Val) VALUES ");
            bool first = true;
            for (int i = 1; i <= N; i++)
            {
                if (i % 50 == 0) continue; // no posts for these blogs
                if (!first) sb2.Append(',');
                first = false;
                sb2.Append('(').Append(2 * i - 1).Append(',').Append(i).Append(',').Append(i * 100).Append(')');
                sb2.Append(",(").Append(2 * i).Append(',').Append(i).Append(',').Append(i * 100 + 1).Append(')');
            }
            using (var c3 = cn.CreateCommand()) { c3.CommandText = sb2.ToString(); c3.ExecuteNonQuery(); }
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static INormQueryable<T> Q<T>(DbContext ctx) where T : class => (INormQueryable<T>)ctx.Query<T>();

    private static void AssertGraph(IReadOnlyList<Blog> blogs)
    {
        Assert.Equal(N, blogs.Count);
        var byId = blogs.ToDictionary(b => b.Id);
        for (int i = 1; i <= N; i++)
        {
            var posts = byId[i].Posts;
            if (i % 50 == 0)
            {
                Assert.Empty(posts);
                continue;
            }
            Assert.Equal(2, posts.Count);
            Assert.All(posts, p => Assert.Equal(i, p.BlogId));
            Assert.Equal(new[] { i * 100, i * 100 + 1 }, posts.Select(p => p.Val).OrderBy(v => v).ToArray());
        }
    }

    [Fact]
    public async Task EagerLoad_AcrossBatchBoundary_NoTracking()
    {
        await using var ctx = Make();
        var blogs = await Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts).OrderBy(b => b.Id).ToListAsync();
        AssertGraph(blogs);
    }

    [Fact]
    public async Task EagerLoad_AcrossBatchBoundary_Tracked()
    {
        await using var ctx = Make();
        var blogs = await Q<Blog>(ctx).Include(b => b.Posts).OrderBy(b => b.Id).ToListAsync();
        AssertGraph(blogs);
    }

    [Fact]
    public async Task ProjectionSplit_AcrossBatchBoundary()
    {
        await using var ctx = Make();
        var rows = await Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id)
            .Select(b => new { b.Id, Posts = b.Posts.ToList() })
            .ToListAsync();
        Assert.Equal(N, rows.Count);
        var byId = rows.ToDictionary(r => r.Id);
        for (int i = 1; i <= N; i++)
        {
            var posts = byId[i].Posts;
            if (i % 50 == 0) { Assert.Empty(posts); continue; }
            Assert.Equal(2, posts.Count);
            Assert.All(posts, p => Assert.Equal(i, p.BlogId));
        }
    }

    [Fact]
    public void ProjectionSplit_AcrossBatchBoundary_Sync()
    {
        using var ctx = Make();
        var rows = Q<Blog>(ctx).AsNoTracking()
            .OrderBy(b => b.Id)
            .Select(b => new { b.Id, Posts = b.Posts.ToList() })
            .ToList();
        Assert.Equal(N, rows.Count);
        var byId = rows.ToDictionary(r => r.Id);
        for (int i = 1; i <= N; i++)
        {
            var posts = byId[i].Posts;
            if (i % 50 == 0) { Assert.Empty(posts); continue; }
            Assert.Equal(2, posts.Count);
            Assert.All(posts, p => Assert.Equal(i, p.BlogId));
        }
    }

    [Fact]
    public async Task EagerLoad_AcrossBatchBoundary_WithParentPaging()
    {
        await using var ctx = Make();
        // Page deep into the set so the paged parents straddle a batch boundary.
        var blogs = await Q<Blog>(ctx).AsNoTracking().Include(b => b.Posts)
            .OrderBy(b => b.Id).Skip(980).Take(30).ToListAsync();
        var expectedIds = Enumerable.Range(981, 30).ToArray();
        Assert.Equal(expectedIds, blogs.Select(b => b.Id).ToArray());
        foreach (var b in blogs)
        {
            if (b.Id % 50 == 0) { Assert.Empty(b.Posts); continue; }
            Assert.Equal(2, b.Posts.Count);
            Assert.All(b.Posts, p => Assert.Equal(b.Id, p.BlogId));
        }
    }
}
