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
/// Many-to-many relationship writes through the tracked navigation collection:
/// adding or removing a related entity and calling SaveChanges must insert or
/// delete the corresponding join-table row. Dropping a change, or double-writing
/// an add-then-remove, silently corrupts the association set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyWriteTests
{
    [Table("M2mw_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mw_Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext Ctx(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mw_PostTag", "PostId", "TagId")
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static SqliteConnection Open()
    {
        var cn = new SqliteConnection($"Data Source=file:m2mw_{Guid.NewGuid():N}?mode=memory&cache=shared");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE M2mw_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
            CREATE TABLE M2mw_Tag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
            CREATE TABLE M2mw_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
            INSERT INTO M2mw_Post VALUES (1, 'hello');
            INSERT INTO M2mw_Tag VALUES (1, 'a'), (2, 'b'), (3, 'c');
            INSERT INTO M2mw_PostTag VALUES (1, 1);
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static List<(int PostId, int TagId)> ReadJoin(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT PostId, TagId FROM M2mw_PostTag ORDER BY PostId, TagId";
        using var reader = cmd.ExecuteReader();
        var rows = new List<(int, int)>();
        while (reader.Read()) rows.Add((reader.GetInt32(0), reader.GetInt32(1)));
        return rows;
    }

    [Fact]
    public async Task Adding_to_collection_inserts_join_row()
    {
        using var keeper = Open();
        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        await using var ctx = Ctx(cn);

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        var tag2 = ctx.Query<Tag>().ToList().Single(t => t.Id == 2);
        post.Tags.Add(tag2);
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { (1, 1), (1, 2) }, ReadJoin(keeper).ToArray());
    }

    [Fact]
    public async Task Removing_from_collection_deletes_join_row()
    {
        using var keeper = Open();
        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        await using var ctx = Ctx(cn);

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        post.Tags.RemoveAll(t => t.Id == 1);
        await ctx.SaveChangesAsync();

        Assert.Empty(ReadJoin(keeper));
    }

    [Fact]
    public async Task Add_then_remove_same_tag_is_net_noop()
    {
        using var keeper = Open();
        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        await using var ctx = Ctx(cn);

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        var tag3 = ctx.Query<Tag>().ToList().Single(t => t.Id == 3);
        post.Tags.Add(tag3);
        post.Tags.RemoveAll(t => t.Id == 3);
        await ctx.SaveChangesAsync();

        // Only the pre-existing (1,1) remains — the transient add/remove cancels.
        Assert.Equal(new[] { (1, 1) }, ReadJoin(keeper).ToArray());
    }

    [Fact]
    public async Task Swap_tags_in_one_save_inserts_and_deletes()
    {
        using var keeper = Open();
        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        await using var ctx = Ctx(cn);

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        var tags = ctx.Query<Tag>().ToList();
        post.Tags.RemoveAll(t => t.Id == 1);
        post.Tags.Add(tags.Single(t => t.Id == 2));
        post.Tags.Add(tags.Single(t => t.Id == 3));
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { (1, 2), (1, 3) }, ReadJoin(keeper).ToArray());
    }
}
