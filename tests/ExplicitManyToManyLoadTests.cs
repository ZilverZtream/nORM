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
/// A lazy / explicit load of a many-to-many navigation (Entry(post).Collection(nameof(Post.Tags)).Load())
/// silently did nothing — the collection stayed empty and was marked loaded — because m2m navigations live in
/// TableMapping.ManyToManyJoins, not Relations, and the inferred-relationship fallback only looks for a
/// direct foreign key on the target table. It must load through the join table, matching Include.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ExplicitManyToManyLoadTests
{
    [Table("EmmlPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("EmmlTag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EmmlPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EmmlTag (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EmmlPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO EmmlPost VALUES (1);" +
                "INSERT INTO EmmlTag VALUES (10), (20), (30);" +
                "INSERT INTO EmmlPostTag VALUES (1, 10), (1, 20);"; // post 1 -> tags 10, 20 (not 30)
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("EmmlPostTag", "PostId", "TagId")
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void Explicit_manytomany_load_populates_the_collection()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var post = ctx.Query<Post>().First();
        ctx.Entry(post).Collection("Tags").Load();

        // Loaded through the join table — tags 10 and 20 (not 30), matching Include.
        Assert.Equal(new[] { 10, 20 }, post.Tags.Select(t => t.Id).OrderBy(i => i).ToArray());
    }
}
