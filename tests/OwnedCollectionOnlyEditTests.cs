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
/// Editing ONLY an owner's owned collection (no scalar change) and calling
/// SaveChanges must persist the collection edit. If change detection is
/// column-only, an owned-collection-only edit leaves the owner Unchanged and the
/// owned-collection sync never runs — silently dropping the write.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedCollectionOnlyEditTests
{
    [Table("OcoPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:oco_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OcoPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE OcoLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO OcoPost VALUES (1, 'p');
                INSERT INTO OcoLine VALUES (1, 1, 'a'), (2, 1, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Line>(p => p.Lines, tableName: "OcoLine", foreignKey: "PostId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<string> ReadLines(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Text FROM OcoLine WHERE PostId = 1 ORDER BY Text";
        using var reader = cmd.ExecuteReader();
        var texts = new List<string>();
        while (reader.Read()) texts.Add(reader.GetString(0));
        return texts;
    }

    [Fact]
    public async Task Adding_owned_child_only_persists()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).ToList().Single();
        Assert.Equal(2, post.Lines.Count); // owned collection loaded

        post.Lines.Add(new Line { Text = "c" });
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { "a", "b", "c" }, ReadLines(keeper).ToArray());
    }

    [Fact]
    public async Task Removing_owned_child_only_persists()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).ToList().Single();
        post.Lines.RemoveAll(l => l.Text == "a");
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { "b" }, ReadLines(keeper).ToArray());
    }

    [Fact]
    public async Task Editing_owned_child_scalar_only_persists()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).ToList().Single();
        post.Lines.Single(l => l.Text == "a").Text = "z";
        await ctx.SaveChangesAsync();

        Assert.Equal(new[] { "b", "z" }, ReadLines(keeper).ToArray());
    }
}
