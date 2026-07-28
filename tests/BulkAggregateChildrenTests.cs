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
/// The columns-only bulk insert/update fast path writes only the owner's own table columns; it cannot
/// persist owned-collection children or many-to-many join rows (they need the owner's key and per-owner
/// child sync, which SaveChanges performs). Silently dropping populated children is data loss, so bulk
/// insert/update FAIL LOUD when an entity actually carries such children — while an aggregate with EMPTY
/// children keeps the fast path (no regression).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkAggregateChildrenTests
{
    [Table("BacPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();   // owned collection
        public List<Tag> Tags { get; set; } = new();     // many-to-many
    }

    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    [Table("BacTag")]
    public class Tag { [Key] public int Id { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:bac_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BacPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
                "CREATE TABLE BacLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);" +
                "CREATE TABLE BacTag (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE BacPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO BacTag VALUES (1), (2);";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        Func<DbContext> make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    var e = mb.Entity<Post>();
                    e.OwnsMany<Line>(p => p.Lines, tableName: "BacLine", foreignKey: "PostId");
                    e.HasMany<Tag>(p => p.Tags).WithMany().UsingTable("BacPostTag", "PostId", "TagId");
                }
            });
        };
        return (keeper, make);
    }

    private static long PostCount(SqliteConnection k)
    { using var cmd = k.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM BacPost"; return Convert.ToInt64(cmd.ExecuteScalar()); }

    [Fact]
    public async Task BulkInsert_owner_with_owned_children_fails_loud()
    {
        var (keeper, make) = Setup(); using var _ = keeper; await using var ctx = make();
        var posts = new List<Post> { new Post { Title = "p1", Lines = { new Line { Text = "a" } } } };
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkInsertAsync(posts));
    }

    [Fact]
    public async Task BulkInsert_owner_with_m2m_links_fails_loud()
    {
        var (keeper, make) = Setup(); using var _ = keeper; await using var ctx = make();
        var t1 = ctx.Query<Tag>().ToList().Single(t => t.Id == 1);
        var posts = new List<Post> { new Post { Title = "p1", Tags = { t1 } } };
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkInsertAsync(posts));
    }

    [Fact]
    public async Task BulkUpdate_owner_with_owned_children_fails_loud()
    {
        var (keeper, make) = Setup(); using var _ = keeper; await using var ctx = make();
        var post = new Post { Id = 1, Title = "p1", Lines = { new Line { Text = "a" } } };
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkUpdateAsync(new[] { post }));
    }

    [Fact]
    public async Task BulkInsert_owner_with_empty_children_uses_fast_path()
    {
        var (keeper, make) = Setup(); using var _ = keeper; await using var ctx = make();
        var posts = new List<Post> { new Post { Title = "p1" }, new Post { Title = "p2" } };   // empty children
        await ctx.BulkInsertAsync(posts);
        Assert.Equal(2L, PostCount(keeper));   // no throw, owners inserted
    }
}
