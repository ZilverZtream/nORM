using System;
using System.Collections.Generic;
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
/// Distinct-count over a navigation collection — <c>x.Coll.Select(e =&gt; e.Col).Distinct().Count()</c> and the
/// whole-entity <c>x.Coll.Distinct().Count()</c> — across all three collection kinds: one-to-many
/// (Relations), many-to-many (ManyToManyJoins) and owned (OwnedCollections). Two silent-wrong bugs are
/// pinned here: (1) m2m and owned distinct-count fell through the Relations-only emit and collapsed to a
/// single GLOBAL COUNT(DISTINCT) row instead of one per outer row; (2) a nullable/reference (e.g. string)
/// selector was rejected by an over-conservative guard and then silently collapsed the same way — now every
/// selector kind emits a NULL-aware count <c>COUNT(DISTINCT c) + CASE WHEN COUNT(*) &gt; COUNT(c) THEN 1 ELSE
/// 0 END</c> that matches C# <c>Distinct().Count()</c> (which counts null as one distinct value).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NavigationDistinctCountCollectionKindsTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NdckPost")]
    public sealed class Post
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NdckTag")]
    public sealed class Tag
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Cat { get; set; }
        public string Category { get; set; } = "";
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NdckOrder")]
    public sealed class Order
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public sealed class Line
    {
        public int Id { get; set; }
        public int OrderId { get; set; }
        public int Cat { get; set; }
        public string Category { get; set; } = "";
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NdckBlog")]
    public sealed class Blog
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<BPost> Posts { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NdckBPost")]
    public sealed class BPost
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public int Cat { get; set; }
        public string Category { get; set; } = "";
        public int? NullCat { get; set; }
        public Blog Blog { get; set; } = default!;
    }

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NdckPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NdckTag (Id INTEGER PRIMARY KEY, Cat INTEGER NOT NULL, Category TEXT NOT NULL);" +
                "CREATE TABLE NdckPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY(PostId, TagId));" +
                "CREATE TABLE NdckOrder (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NdckLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Cat INTEGER NOT NULL, Category TEXT NOT NULL);" +
                "CREATE TABLE NdckBlog (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE NdckBPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Cat INTEGER NOT NULL, Category TEXT NOT NULL, NullCat INTEGER);" +
                // Each outer row 1 has 2 distinct categories (Cat 1,1,2), row 2 has 1 (Cat 3), row 3 empty.
                "INSERT INTO NdckPost VALUES (1),(2),(3);" +
                "INSERT INTO NdckTag VALUES (1,1,'X'),(2,1,'X'),(3,2,'Y'),(4,3,'Z');" +
                "INSERT INTO NdckPostTag VALUES (1,1),(1,2),(1,3),(2,4);" +
                "INSERT INTO NdckOrder VALUES (1),(2),(3);" +
                "INSERT INTO NdckLine VALUES (1,1,1,'X'),(2,1,1,'X'),(3,1,2,'Y'),(4,2,3,'Z');" +
                "INSERT INTO NdckBlog VALUES (1),(2),(3);" +
                // NullCat: blog1 {5,null,null}->distinct{5,null}=2 ; blog2 {null}=1 ; blog3 empty=0
                "INSERT INTO NdckBPost VALUES (1,1,1,'X',5),(2,1,1,'X',NULL),(3,1,2,'Y',NULL),(4,2,3,'Z',NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("NdckPostTag", "PostId", "TagId");
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "NdckLine", foreignKey: "OrderId",
                    buildAction: b => b.HasKey(l => l.Id));
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<BPost>().HasKey(p => p.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne(p => p.Blog).HasForeignKey(p => p.BlogId, b => b.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void M2m_int_selector_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Post>().OrderBy(p => p.Id).Select(p => p.Tags.Select(t => t.Cat).Distinct().Count()).ToList());
    }

    [Fact]
    public void M2m_string_selector_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Post>().OrderBy(p => p.Id).Select(p => p.Tags.Select(t => t.Category).Distinct().Count()).ToList());
    }

    [Fact]
    public void M2m_whole_entity_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 3, 1, 0 }, ctx.Query<Post>().OrderBy(p => p.Id).Select(p => p.Tags.Distinct().Count()).ToList());
    }

    [Fact]
    public void Owned_int_selector_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Order>().OrderBy(o => o.Id).Select(o => o.Lines.Select(l => l.Cat).Distinct().Count()).ToList());
    }

    [Fact]
    public void Owned_string_selector_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Order>().OrderBy(o => o.Id).Select(o => o.Lines.Select(l => l.Category).Distinct().Count()).ToList());
    }

    [Fact]
    public void Owned_whole_entity_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 3, 1, 0 }, ctx.Query<Order>().OrderBy(o => o.Id).Select(o => o.Lines.Distinct().Count()).ToList());
    }

    [Fact]
    public void OneToMany_string_selector_distinct_count()
    {
        using var ctx = Ctx();
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Blog>().OrderBy(b => b.Id).Select(b => b.Posts.Select(p => p.Category).Distinct().Count()).ToList());
    }

    [Fact]
    public void OneToMany_nullable_selector_counts_null_group()
    {
        using var ctx = Ctx();
        // {5,null,null}->2, {null}->1, {}->0 : the NULL-aware CASE re-adds the null group.
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Blog>().OrderBy(b => b.Id).Select(b => b.Posts.Select(p => p.NullCat).Distinct().Count()).ToList());
    }
}
