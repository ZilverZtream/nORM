using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Many-to-many eager loading respects the RIGHT-side entity's global filters:
/// filtered-out related rows (soft-deleted tags) never enter the loaded
/// collections — the same visibility rule the regular include levels apply.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyGlobalFilterTests
{
    [Table("M2mGf_Post")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mGf_Tag")]
    private class Tag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
        public bool Hidden { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE M2mGf_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE M2mGf_Tag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Hidden INTEGER NOT NULL);
                CREATE TABLE M2mGf_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO M2mGf_Post VALUES (1, 'hello');
                INSERT INTO M2mGf_Tag VALUES (1, 'ok', 0), (2, 'secret', 1);
                INSERT INTO M2mGf_PostTag VALUES (1, 1), (1, 2);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Tag>(t => !t.Hidden);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mGf_PostTag", "PostId", "TagId");
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void m2m_include_respects_right_side_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        Assert.Single(post.Tags);           // 'secret' is Hidden → filtered
        Assert.Equal("ok", post.Tags[0].Label);
    }
}
