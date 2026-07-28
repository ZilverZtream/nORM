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
/// nORM cleans up its OWN nORM-managed child rows (owned-collection children, many-to-many join rows)
/// client-side when an owner/related entity is deleted. The tracked SaveChanges Deleted branch does this;
/// the active-record DeleteAsync and set-based BulkDeleteAsync paths must do the same. Otherwise a delete
/// silently leaves orphaned owned children (unreachable rows) or dangling join rows at a deleted entity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CascadeChildCleanupTests
{
    private static List<(int, int)> JoinRows(SqliteConnection k, string table)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = $"SELECT PostId, TagId FROM {table} ORDER BY PostId, TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<(int, int)>();
        while (r.Read()) v.Add((r.GetInt32(0), r.GetInt32(1)));
        return v;
    }

    // ---- B: owned-collection owner via active-record / bulk delete ----
    [Table("OwPost")] public class OwPost { [Key] public int Id { get; set; } public string Title { get; set; } = ""; public List<OwTag> Tags { get; set; } = new(); }
    public class OwTag { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    private static SqliteConnection SetupOwned(out Func<DbContext> make)
    {
        var keeper = new SqliteConnection($"Data Source=file:ow_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE OwPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);" +
                "CREATE TABLE OwTag (Id INTEGER PRIMARY KEY, OwPostId INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO OwPost VALUES (1, 'p');" +
                "INSERT INTO OwTag VALUES (1, 1, 'a'), (2, 1, 'b');";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<OwPost>().OwnsMany<OwTag>(p => p.Tags, tableName: "OwTag", foreignKey: "OwPostId")
            });
        };
        return keeper;
    }

    private static int OwnedTagCount(SqliteConnection k)
    { using var cmd = k.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM OwTag"; return Convert.ToInt32(cmd.ExecuteScalar()); }

    [Fact]
    public async Task ActiveRecord_DeleteAsync_owner_removes_owned_collection_children()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        var post = ctx.Query<OwPost>().ToList().Single(p => p.Id == 1);
        await ctx.DeleteAsync(post);
        Assert.Equal(0, OwnedTagCount(keeper));
    }

    [Fact]
    public async Task Tracked_Remove_owner_removes_owned_collection_children()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        var post = ctx.Query<OwPost>().ToList().Single(p => p.Id == 1);
        ctx.Remove(post);
        await ctx.SaveChangesAsync();
        Assert.Equal(0, OwnedTagCount(keeper));
    }

    [Fact]
    public async Task BulkDeleteAsync_owner_removes_owned_collection_children()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        var post = ctx.Query<OwPost>().ToList().Single(p => p.Id == 1);
        await ctx.BulkDeleteAsync(new[] { post });
        Assert.Equal(0, OwnedTagCount(keeper));
    }

    // ---- C: active-record DeleteAsync of a bidirectional m2m entity ----
    [Table("BiPost")] public class BiPost { [Key] public int Id { get; set; } public List<BiTag> Tags { get; set; } = new(); }
    [Table("BiTag")] public class BiTag { [Key] public int Id { get; set; } public List<BiPost> Posts { get; set; } = new(); }

    private static SqliteConnection SetupBi(out Func<DbContext> make)
    {
        var keeper = new SqliteConnection($"Data Source=file:bi_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BiPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE BiTag (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE BiPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO BiPost VALUES (1);" +
                "INSERT INTO BiTag VALUES (1), (2);" +
                "INSERT INTO BiPostTag VALUES (1, 1), (1, 2);";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<BiPost>().HasMany(p => p.Tags).WithMany(t => t.Posts).UsingTable("BiPostTag", "PostId", "TagId")
            });
        };
        return keeper;
    }

    [Fact]
    public async Task ActiveRecord_DeleteAsync_m2m_entity_removes_its_join_rows()
    {
        using var keeper = SetupBi(out var make);
        await using var ctx = make();
        var post = ctx.Query<BiPost>().ToList().Single(p => p.Id == 1);
        await ctx.DeleteAsync(post);
        Assert.Empty(JoinRows(keeper, "BiPostTag"));
    }
}
