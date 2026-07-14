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
/// An owner carrying BOTH a many-to-many navigation and an owned collection.
/// Editing one collection must not corrupt the other, an owned edit and an M2M
/// edit in one SaveChanges must both apply, and deleting the owner must cascade
/// both the owned children and the join rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CombinedM2MOwnedWriteTests
{
    [Table("CmoPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();   // owned
        public List<Tag> Tags { get; set; } = new();     // many-to-many
    }

    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    [Table("CmoTag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:cmo_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CmoPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE CmoLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                CREATE TABLE CmoTag (Id INTEGER PRIMARY KEY);
                CREATE TABLE CmoPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO CmoPost VALUES (1, 'p');
                INSERT INTO CmoLine VALUES (1, 1, 'a'), (2, 1, 'b');
                INSERT INTO CmoTag VALUES (1), (2), (3);
                INSERT INTO CmoPostTag VALUES (1, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    var e = mb.Entity<Post>();
                    e.OwnsMany<Line>(p => p.Lines, tableName: "CmoLine", foreignKey: "PostId");
                    e.HasMany<Tag>(p => p.Tags).WithMany().UsingTable("CmoPostTag", "PostId", "TagId");
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<string> Lines(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Text FROM CmoLine WHERE PostId = 1 ORDER BY Text";
        using var r = cmd.ExecuteReader();
        var v = new List<string>(); while (r.Read()) v.Add(r.GetString(0)); return v;
    }

    private static List<int> Tags(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT TagId FROM CmoPostTag WHERE PostId = 1 ORDER BY TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<int>(); while (r.Read()) v.Add(r.GetInt32(0)); return v;
    }

    private static Post Load(DbContext ctx) =>
        ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).Include(p => p.Tags).ToList().Single();

    [Fact]
    public async Task Owned_edit_only_does_not_disturb_m2m()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = Load(ctx);
        post.Lines.Add(new Line { Text = "c" });
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { "a", "b", "c" }, Lines(keeper).ToArray());
        Assert.Equal(new[] { 1 }, Tags(keeper).ToArray()); // M2M unchanged
    }

    [Fact]
    public async Task M2M_edit_only_does_not_disturb_owned()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = Load(ctx);
        post.Tags.Add(ctx.Query<Tag>().ToList().Single(t => t.Id == 2));
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { 1, 2 }, Tags(keeper).ToArray());
        Assert.Equal(new[] { "a", "b" }, Lines(keeper).ToArray()); // owned unchanged
    }

    [Fact]
    public async Task Both_edited_in_one_save_apply()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = Load(ctx);
        post.Lines.RemoveAll(l => l.Text == "a");
        post.Lines.Add(new Line { Text = "c" });
        post.Tags.RemoveAll(t => t.Id == 1);
        post.Tags.Add(ctx.Query<Tag>().ToList().Single(t => t.Id == 3));
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { "b", "c" }, Lines(keeper).ToArray());
        Assert.Equal(new[] { 3 }, Tags(keeper).ToArray());
    }

    [Fact]
    public async Task Deleting_owner_cascades_owned_and_m2m()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = Load(ctx);
        ctx.Remove(post);
        await ctx.SaveChangesAsync();

        Assert.Empty(Lines(keeper));
        Assert.Empty(Tags(keeper));
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM CmoPost WHERE Id = 1";
        Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}
