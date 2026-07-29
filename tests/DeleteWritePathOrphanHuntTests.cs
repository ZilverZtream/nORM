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
/// DELETE write path — set-based ExecuteDeleteAsync cascades nORM-MANAGED children (owned-collection rows,
/// many-to-many join rows) for the common single-table single-key shape via a set-based child DELETE
/// (`... WHERE fk IN (SELECT owner-key ...)`), atomic with the owner delete. nORM manages these client-side
/// (SQLite does not enforce FK cascade at runtime), so without this they would be silently orphaned. Paged /
/// joined / composite-key shapes with managed children still fail loud (rare). DeleteAsync/BulkDeleteAsync
/// also cascade (positive controls below).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeleteWritePathOrphanHuntTests
{
    // ---------- owned-collection owner ----------
    [Table("EdOwPost")] public class EdOwPost { [Key] public int Id { get; set; } public string Title { get; set; } = ""; public List<EdOwTag> Tags { get; set; } = new(); }
    public class EdOwTag { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    private static SqliteConnection SetupOwned(out Func<DbContext> make)
    {
        var keeper = new SqliteConnection($"Data Source=file:edow_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EdOwPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);" +
                "CREATE TABLE EdOwTag (Id INTEGER PRIMARY KEY, EdOwPostId INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO EdOwPost VALUES (1, 'p'), (2, 'q');" +
                "INSERT INTO EdOwTag VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c');";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<EdOwPost>().OwnsMany<EdOwTag>(p => p.Tags, tableName: "EdOwTag", foreignKey: "EdOwPostId")
            });
        };
        return keeper;
    }

    private static int OwnedTagCount(SqliteConnection k, int postId)
    { using var cmd = k.CreateCommand(); cmd.CommandText = $"SELECT COUNT(*) FROM EdOwTag WHERE EdOwPostId = {postId}"; return Convert.ToInt32(cmd.ExecuteScalar()); }

    private static int PostCount(SqliteConnection k, string table, int id)
    { using var cmd = k.CreateCommand(); cmd.CommandText = $"SELECT COUNT(*) FROM {table} WHERE Id = {id}"; return Convert.ToInt32(cmd.ExecuteScalar()); }

    [Fact]
    public async Task ExecuteDelete_owner_cascades_owned_collection_children()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        var deleted = await ctx.Query<EdOwPost>().Where(p => p.Id == 1).ExecuteDeleteAsync();
        Assert.Equal(1, deleted);
        Assert.Equal(0, PostCount(keeper, "EdOwPost", 1));   // owner deleted
        Assert.Equal(0, OwnedTagCount(keeper, 1));           // its 2 owned rows cascaded (not orphaned)
        Assert.Equal(1, OwnedTagCount(keeper, 2));           // sibling owner's child untouched
    }

    [Fact]
    public async Task ExecuteDelete_multiple_matched_owners_cascades_all_their_children()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        // Set-based predicate matching BOTH owners — the owner-key subquery must cascade every matched
        // owner's children, not just one (the differentiator from per-entity cleanup).
        var deleted = await ctx.Query<EdOwPost>().Where(p => p.Id <= 2).ExecuteDeleteAsync();
        Assert.Equal(2, deleted);
        Assert.Equal(0, OwnedTagCount(keeper, 1));
        Assert.Equal(0, OwnedTagCount(keeper, 2));
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM EdOwTag";
        Assert.Equal(0, Convert.ToInt32(cmd.ExecuteScalar()));   // all owned rows cascaded
    }

    [Fact]
    public async Task ExecuteDelete_paged_owner_with_children_fails_loud()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        // The paged owner-key subquery isn't wired for child cascade yet — fail loud rather than orphan.
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            async () => await ctx.Query<EdOwPost>().OrderBy(p => p.Id).Take(1).ExecuteDeleteAsync());
        Assert.Equal(3, OwnedTagCount(keeper, 1) + OwnedTagCount(keeper, 2));   // nothing deleted (2 + 1 tags)
    }

    // ---------- bidirectional m2m ----------
    [Table("EdBiPost")] public class EdBiPost { [Key] public int Id { get; set; } public List<EdBiTag> Tags { get; set; } = new(); }
    [Table("EdBiTag")] public class EdBiTag { [Key] public int Id { get; set; } public List<EdBiPost> Posts { get; set; } = new(); }

    private static SqliteConnection SetupBi(out Func<DbContext> make)
    {
        var keeper = new SqliteConnection($"Data Source=file:edbi_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EdBiPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EdBiTag (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EdBiPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO EdBiPost VALUES (1), (2);" +
                "INSERT INTO EdBiTag VALUES (1), (2);" +
                "INSERT INTO EdBiPostTag VALUES (1, 1), (1, 2), (2, 1);";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<EdBiPost>().HasMany(p => p.Tags).WithMany(t => t.Posts).UsingTable("EdBiPostTag", "PostId", "TagId")
            });
        };
        return keeper;
    }

    private static List<(int, int)> JoinRows(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT PostId, TagId FROM EdBiPostTag ORDER BY PostId, TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<(int, int)>();
        while (r.Read()) v.Add((r.GetInt32(0), r.GetInt32(1)));
        return v;
    }

    [Fact]
    public async Task ExecuteDelete_m2m_entity_cascades_join_rows()
    {
        using var keeper = SetupBi(out var make);
        await using var ctx = make();
        var deleted = await ctx.Query<EdBiPost>().Where(p => p.Id == 1).ExecuteDeleteAsync();
        Assert.Equal(1, deleted);
        // Post #1's two join rows cascaded; post #2's single join row survives (no dangling rows).
        Assert.Equal(new List<(int, int)> { (2, 1) }, JoinRows(keeper));
        Assert.Equal(0, PostCount(keeper, "EdBiPost", 1));
    }

    // ---------- POSITIVE CONTROL: active-record DeleteAsync of the SAME owner cleans up ----------
    // Proves the orphaning above is an ExecuteDelete-path asymmetry, not a general schema/config issue.
    [Fact]
    public async Task DeleteAsync_owner_removes_owned_children_positive_control()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        var post = ctx.Query<EdOwPost>().ToList().Single(p => p.Id == 1);
        await ctx.DeleteAsync(post);
        Assert.Equal(0, OwnedTagCount(keeper, 1));   // cleaned up (CleanupNormManagedChildrenOnDeleteAsync)
        Assert.Equal(1, OwnedTagCount(keeper, 2));
    }

    [Fact]
    public async Task DeleteAsync_m2m_entity_removes_join_rows_positive_control()
    {
        using var keeper = SetupBi(out var make);
        await using var ctx = make();
        var post = ctx.Query<EdBiPost>().ToList().Single(p => p.Id == 1);
        await ctx.DeleteAsync(post);
        Assert.Equal(new List<(int, int)> { (2, 1) }, JoinRows(keeper));   // cleaned up
    }
}
