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
/// A UNIDIRECTIONAL many-to-many (HasMany(...).WithMany() with no inverse navigation) mirrors no navigation
/// onto the related type, so deleting the RIGHT (non-declaring) side used to leave its join rows dangling at
/// a now-deleted entity — the related type's mapping carried no join to clean. A cleanup-only inverse join is
/// now registered so the related entity's delete removes its own join rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class UnidirectionalM2mDeleteCleanupTests
{
    [Table("UniPost")] public class UniPost { [Key] public int Id { get; set; } public List<UniTag> Tags { get; set; } = new(); }
    [Table("UniTag")] public class UniTag { [Key] public int Id { get; set; } }   // NO inverse nav

    private static SqliteConnection Setup(out Func<DbContext> make)
    {
        var keeper = new SqliteConnection($"Data Source=file:unidel_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE UniPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE UniTag (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE UniPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO UniPost VALUES (1);" +
                "INSERT INTO UniTag VALUES (1), (2);" +
                "INSERT INTO UniPostTag VALUES (1, 1), (1, 2);";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<UniPost>().HasMany(p => p.Tags).WithMany().UsingTable("UniPostTag", "PostId", "TagId")
            });
        };
        return keeper;
    }

    private static List<(int, int)> JoinRows(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT PostId, TagId FROM UniPostTag ORDER BY PostId, TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<(int, int)>();
        while (r.Read()) v.Add((r.GetInt32(0), r.GetInt32(1)));
        return v;
    }

    [Fact]
    public async Task Tracked_delete_of_right_side_removes_its_join_rows()
    {
        using var keeper = Setup(out var make);
        await using var ctx = make();
        var tag2 = ctx.Query<UniTag>().ToList().Single(t => t.Id == 2);
        ctx.Remove(tag2);
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { (1, 1) }, JoinRows(keeper).ToArray());   // (1,2) removed
    }

    [Fact]
    public async Task ActiveRecord_delete_of_right_side_removes_its_join_rows()
    {
        using var keeper = Setup(out var make);
        await using var ctx = make();
        var tag2 = ctx.Query<UniTag>().ToList().Single(t => t.Id == 2);
        await ctx.DeleteAsync(tag2);
        Assert.Equal(new[] { (1, 1) }, JoinRows(keeper).ToArray());
    }

    [Fact]
    public async Task Deleting_left_declaring_side_still_removes_its_join_rows()
    {
        using var keeper = Setup(out var make);
        await using var ctx = make();
        var post = ctx.Query<UniPost>().ToList().Single(p => p.Id == 1);
        ctx.Remove(post);
        await ctx.SaveChangesAsync();
        Assert.Empty(JoinRows(keeper));   // declaring side cleanup unaffected by the mirror
    }
}
