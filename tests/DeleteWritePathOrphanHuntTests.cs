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
/// DELETE write path — set-based ExecuteDeleteAsync vs active-record DeleteAsync for nORM-MANAGED children
/// (owned-collection rows, many-to-many join rows). nORM manages these client-side (SQLite does not enforce
/// FK cascade at runtime), so a set-based delete cannot cascade them. Rather than SILENTLY ORPHAN them,
/// ExecuteDeleteAsync now REFUSES LOUDLY when the target has owned/m2m children (mirrors the Bulk*
/// aggregate-children guard); DeleteAsync/BulkDeleteAsync DO cascade via CleanupNormManagedChildrenOnDeleteAsync
/// (positive controls below). Full set-based child cleanup in ExecuteDelete is a tracked follow-up.
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
    public async Task ExecuteDelete_owner_with_owned_children_fails_loud_not_orphans()
    {
        using var keeper = SetupOwned(out var make);
        await using var ctx = make();
        // Set-based ExecuteDelete cannot cascade nORM-managed owned children; it refuses loudly rather than
        // orphan them. (DeleteAsync/BulkDeleteAsync cascade — see positive control below.)
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            async () => await ctx.Query<EdOwPost>().Where(p => p.Id == 1).ExecuteDeleteAsync());
        // Nothing deleted — no partial corruption: owner and its children all intact.
        Assert.Equal(1, PostCount(keeper, "EdOwPost", 1));   // owner NOT deleted
        Assert.Equal(2, OwnedTagCount(keeper, 1));           // children NOT orphaned/deleted
        Assert.Equal(1, OwnedTagCount(keeper, 2));
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
    public async Task ExecuteDelete_m2m_entity_fails_loud_not_dangles()
    {
        using var keeper = SetupBi(out var make);
        await using var ctx = make();
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            async () => await ctx.Query<EdBiPost>().Where(p => p.Id == 1).ExecuteDeleteAsync());
        // Nothing deleted — all join rows and posts intact (no dangling m2m rows, no orphaned owner).
        Assert.Equal(new List<(int, int)> { (1, 1), (1, 2), (2, 1) }, JoinRows(keeper));
        Assert.Equal(1, PostCount(keeper, "EdBiPost", 1));
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
