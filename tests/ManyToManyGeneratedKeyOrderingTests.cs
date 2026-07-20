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
/// When a left entity and a NEW related right entity (whose key is database-generated) are saved in one
/// SaveChanges, the join-row sync must read the right entity's key AFTER it has been assigned. The sync ran
/// inline right after the left's own INSERT — before the right entity's group was inserted — so it captured
/// the right key at its CLR default (0) and wrote a join row (leftKey, 0). SQLite does not enforce foreign
/// keys at runtime, so the bad row was accepted silently and the real association was lost; reloading the
/// left's collection came back empty. The correct link must be written regardless of attach order.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyGeneratedKeyOrderingTests
{
    [Table("MmgPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("MmgTag")]
    public class Tag
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:mmg_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MmgPost (Id INTEGER PRIMARY KEY AUTOINCREMENT);
                CREATE TABLE MmgTag  (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                CREATE TABLE MmgPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Post>()
                    .HasMany<Tag>(p => p.Tags).WithMany().UsingTable("MmgPostTag", "PostId", "TagId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static (int PostId, int TagId) JoinRow(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT PostId, TagId FROM MmgPostTag";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read(), "expected exactly one join row");
        return (r.GetInt32(0), r.GetInt32(1));
    }

    [Fact]
    public async Task Left_attached_before_new_right_still_links_the_generated_key()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = new Post();
        var tag = new Tag { Name = "t" };
        ctx.Add(post);   // LEFT attached first (the natural "add parent, then child" order)
        ctx.Add(tag);    // RIGHT attached second, key still 0
        post.Tags.Add(tag);
        await ctx.SaveChangesAsync();

        Assert.True(tag.Id > 0, "tag received a generated key");
        var (postId, tagId) = JoinRow(keeper);
        Assert.Equal(post.Id, postId);
        Assert.Equal(tag.Id, tagId);   // BUG before fix: 0 - the join captured the pre-insert default key

        // Reloading the association must return the tag.
        await using var verify = make();
        var reloaded = ((INormQueryable<Post>)verify.Query<Post>()).Include(p => p.Tags).ToList().Single();
        Assert.Equal(new[] { tag.Id }, reloaded.Tags.Select(t => t.Id).ToArray());
    }

    [Fact]
    public async Task Right_attached_before_left_also_links_the_generated_key()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = new Post();
        var tag = new Tag { Name = "t" };
        ctx.Add(tag);    // RIGHT first (the order that happened to work before)
        ctx.Add(post);
        post.Tags.Add(tag);
        await ctx.SaveChangesAsync();

        var (postId, tagId) = JoinRow(keeper);
        Assert.Equal(post.Id, postId);
        Assert.Equal(tag.Id, tagId);
    }
}
