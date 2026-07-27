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
/// A bidirectional many-to-many configured with HasMany(p =&gt; p.Tags).WithMany(t =&gt; t.Posts) projects both
/// navigations onto the same join table. Editing EITHER navigation must persist the join — editing the
/// inverse (right-side) navigation tag.Posts must write/remove the join just as post.Tags does. Change
/// detection and join sync were purely left-collection-driven, so an inverse-side edit was a silent no-op;
/// the join is now mirrored onto the related type so either side works, with the same row deduped when both
/// sides are edited consistently.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class InverseManyToManyNavigationWriteTests
{
    [Table("C2Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("C2Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public List<Post> Posts { get; set; } = new();
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(bool seedJoin = false)
    {
        var keeper = new SqliteConnection($"Data Source=file:c2_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE C2Post (Id INTEGER PRIMARY KEY);
                CREATE TABLE C2Tag (Id INTEGER PRIMARY KEY);
                CREATE TABLE C2PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO C2Post VALUES (1);
                INSERT INTO C2Tag VALUES (1);
                """;
            cmd.ExecuteNonQuery();
        }
        if (seedJoin)
            using (var cmd = keeper.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO C2PostTag VALUES (1, 1);";
                cmd.ExecuteNonQuery();
            }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                    mb.Entity<Post>().HasMany(p => p.Tags).WithMany(t => t.Posts).UsingTable("C2PostTag", "PostId", "TagId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<(int PostId, int TagId)> Joins(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT PostId, TagId FROM C2PostTag ORDER BY PostId, TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<(int, int)>();
        while (r.Read()) v.Add((r.GetInt32(0), r.GetInt32(1)));
        return v;
    }

    [Fact]
    public async Task Inverse_add_persists_the_join()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var tag = ((INormQueryable<Tag>)ctx.Query<Tag>()).Include(t => t.Posts).ToList().Single();
        var post = ctx.Query<Post>().ToList().Single();
        tag.Posts.Add(post);          // inverse (right-side) navigation only
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { (1, 1) }, Joins(keeper).ToArray());
    }

    [Fact]
    public async Task Inverse_remove_deletes_the_join()
    {
        var (keeper, make) = Setup(seedJoin: true);
        using var _ = keeper;
        await using var ctx = make();

        var tag = ((INormQueryable<Tag>)ctx.Query<Tag>()).Include(t => t.Posts).ToList().Single();
        Assert.Single(tag.Posts);
        tag.Posts.RemoveAll(p => p.Id == 1);   // inverse remove
        await ctx.SaveChangesAsync();

        Assert.Empty(Joins(keeper));
    }

    [Fact]
    public async Task Both_sides_add_consistently_writes_one_deduped_join()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        var tag = ((INormQueryable<Tag>)ctx.Query<Tag>()).Include(t => t.Posts).ToList().Single();
        post.Tags.Add(tag);
        tag.Posts.Add(post);          // both sides, consistent
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { (1, 1) }, Joins(keeper).ToArray());  // exactly one row
    }

    [Fact]
    public async Task Declaring_side_add_still_persists_the_join()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        var tag = ctx.Query<Tag>().ToList().Single();
        post.Tags.Add(tag);           // declaring (left-side) navigation
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { (1, 1) }, Joins(keeper).ToArray());
    }
}
