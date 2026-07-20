using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins ordered / top-N Include (EF Core parity): <c>Include(b =&gt; b.Posts.OrderByDescending(p =&gt; p.Score)
/// .Take(n))</c> eager-loads the top-N children PER PARENT in the requested order via a
/// <c>ROW_NUMBER() OVER (PARTITION BY fk ORDER BY key)</c> window, with any filter applied inside the window.
/// Skip/Take without an OrderBy and ordered many-to-many Includes fail loud.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderedIncludeContractTests
{
    [Table("OiBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("OiPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public int Score { get; set; }
        public bool Active { get; set; }
        public List<Comment> Comments { get; set; } = new();
    }

    [Table("OiComment")]
    public class Comment
    {
        [Key] public int Id { get; set; }
        public int PostId { get; set; }
        public int Ord { get; set; }
    }

    [Table("OiTag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OiBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE OiPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL, Score INTEGER NOT NULL, Active INTEGER NOT NULL);
                CREATE TABLE OiComment (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Ord INTEGER NOT NULL);
                CREATE TABLE OiTag (Id INTEGER PRIMARY KEY);
                CREATE TABLE OiBlogTag (BlogId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO OiBlog VALUES (1, 'b1'), (2, 'b2');
                -- Blog 1 posts: scores 10, 30, 20, 40 (some inactive)
                INSERT INTO OiPost VALUES (1, 1, 'p10', 10, 1), (2, 1, 'p30', 30, 1), (3, 1, 'p20', 20, 0), (4, 1, 'p40', 40, 1);
                -- Blog 2 posts: scores 5, 15
                INSERT INTO OiPost VALUES (5, 2, 'p5', 5, 1), (6, 2, 'p15', 15, 1);
                INSERT INTO OiComment VALUES (1, 2, 3), (2, 2, 1), (3, 2, 2);
                INSERT INTO OiTag VALUES (1);
                INSERT INTO OiBlogTag VALUES (1, 1);
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
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
                mb.Entity<Post>().HasMany(p => p.Comments).WithOne().HasForeignKey(c => c.PostId, p => p.Id);
                mb.Entity<Blog>().HasMany<Tag>(b => b.Tags).WithMany().UsingTable("OiBlogTag", "BlogId", "TagId");
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void TopN_descending_is_per_parent_and_ordered()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        var blogs = ctx.Query<Blog>()
            .Include(b => b.Posts.OrderByDescending(p => p.Score).Take(2))
            .OrderBy(b => b.Id)
            .ToList();

        var b1 = blogs.Single(b => b.Id == 1);
        var b2 = blogs.Single(b => b.Id == 2);

        // Each blog gets ITS OWN top-2 by score desc — not a global top-2.
        Assert.Equal(new[] { 40, 30 }, b1.Posts.Select(p => p.Score).ToArray());
        Assert.Equal(new[] { 15, 5 }, b2.Posts.Select(p => p.Score).ToArray());
    }

    [Fact]
    public void Ascending_order_supported()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        var b1 = ctx.Query<Blog>()
            .Include(b => b.Posts.OrderBy(p => p.Score).Take(2))
            .Where(b => b.Id == 1)
            .ToList().Single();

        Assert.Equal(new[] { 10, 20 }, b1.Posts.Select(p => p.Score).ToArray());
    }

    [Fact]
    public void Skip_then_take_windows_within_parent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        // Blog 1 ascending scores: 10, 20, 30, 40. Skip(1).Take(2) => 20, 30.
        var b1 = ctx.Query<Blog>()
            .Include(b => b.Posts.OrderBy(p => p.Score).Skip(1).Take(2))
            .Where(b => b.Id == 1)
            .ToList().Single();

        Assert.Equal(new[] { 20, 30 }, b1.Posts.Select(p => p.Score).ToArray());
    }

    [Fact]
    public void Filter_is_applied_inside_the_window()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        // Only Active posts, top-2 by score desc. Blog 1 active scores: 10, 30, 40 (20 is inactive) => 40, 30.
        var b1 = ctx.Query<Blog>()
            .Include(b => b.Posts.Where(p => p.Active).OrderByDescending(p => p.Score).Take(2))
            .Where(b => b.Id == 1)
            .ToList().Single();

        Assert.Equal(new[] { 40, 30 }, b1.Posts.Select(p => p.Score).ToArray());
        Assert.DoesNotContain(b1.Posts, p => !p.Active);
    }

    [Fact]
    public void ThenInclude_ordered_topN_per_parent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        // Post 2 has comments with Ord 3,1,2 -> ordered ascending, top-2 => Ord 1, 2.
        var b1 = ctx.Query<Blog>()
            .Include(b => b.Posts.OrderByDescending(p => p.Score).Take(4))
                .ThenInclude(p => p.Comments.OrderBy(c => c.Ord).Take(2))
            .Where(b => b.Id == 1)
            .ToList().Single();

        var post2 = b1.Posts.Single(p => p.Id == 2);
        Assert.Equal(new[] { 1, 2 }, post2.Comments.Select(c => c.Ord).ToArray());
    }

    [Fact]
    public void ToList_form_and_ordering_without_take_loads_full_collection_ordered()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        // The ToList()-wrapped, no-Take form orders the FULL collection per parent (no top-N cap).
        var b1 = ctx.Query<Blog>()
            .Include(b => b.Posts.OrderByDescending(p => p.Score).ToList())
            .Where(b => b.Id == 1)
            .ToList().Single();

        Assert.Equal(new[] { 40, 30, 20, 10 }, b1.Posts.Select(p => p.Score).ToArray());
    }

    [Fact]
    public void Take_without_orderby_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Blog>().Include(b => b.Posts.Take(2)).ToList());
        Assert.Contains("OrderBy", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Ordered_many_to_many_include_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);

        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Blog>().Include(b => b.Tags.OrderBy(t => t.Id).Take(1)).ToList());
    }
}
