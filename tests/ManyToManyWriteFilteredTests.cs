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
/// Many-to-many collection writes under a right-side global filter. The Include
/// loads only the visible associations, so the change-tracking snapshot reflects
/// only those. A collection edit must sync ONLY the visible delta and must never
/// clobber a filtered-out (hidden) association the user can't see — deleting a
/// hidden join row on an unrelated edit is silent cross-visibility data loss.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyWriteFilteredTests
{
    [Table("M2mwf_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mwf_Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public bool Hidden { get; set; }
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:m2mwf_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE M2mwf_Post (Id INTEGER PRIMARY KEY);
                CREATE TABLE M2mwf_Tag (Id INTEGER PRIMARY KEY, Hidden INTEGER NOT NULL);
                CREATE TABLE M2mwf_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO M2mwf_Post VALUES (1);
                INSERT INTO M2mwf_Tag VALUES (1, 0), (2, 1), (3, 0), (4, 0);
                -- Post 1 linked to visible tag 1 AND hidden tag 2.
                INSERT INTO M2mwf_PostTag VALUES (1, 1), (1, 2);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions();
            opts.AddGlobalFilter<Tag>(t => !t.Hidden);
            opts.OnModelCreating = mb =>
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mwf_PostTag", "PostId", "TagId");
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<int> Join(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT TagId FROM M2mwf_PostTag WHERE PostId = 1 ORDER BY TagId";
        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read()) ids.Add(reader.GetInt32(0));
        return ids;
    }

    [Fact]
    public async Task Adding_visible_tag_preserves_hidden_association()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        Assert.Equal(new[] { 1 }, post.Tags.Select(t => t.Id).ToArray()); // only visible tag 1 loaded

        post.Tags.Add(ctx.Query<Tag>().ToList().Single(t => t.Id == 3));
        await ctx.SaveChangesAsync();

        // Hidden tag 2's join row must survive; 3 added.
        Assert.Equal(new[] { 1, 2, 3 }, Join(keeper).ToArray());
    }

    [Fact]
    public async Task Removing_visible_tag_preserves_hidden_association()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        post.Tags.RemoveAll(t => t.Id == 1);
        await ctx.SaveChangesAsync();

        // Only visible tag 1 removed; hidden tag 2 preserved.
        Assert.Equal(new[] { 2 }, Join(keeper).ToArray());
    }

    [Fact]
    public async Task Clearing_visible_collection_leaves_hidden_association_intact()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        post.Tags.Clear(); // removes only the visible ones the user could see
        await ctx.SaveChangesAsync();

        // The hidden association is not visible to the user and must not be deleted.
        Assert.Equal(new[] { 2 }, Join(keeper).ToArray());
    }
}
