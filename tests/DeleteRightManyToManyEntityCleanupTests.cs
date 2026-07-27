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
/// Deleting the related (right-side) entity of a bidirectional many-to-many must remove its join rows, just
/// as deleting the declaring entity does. Because the right type carried no join mapping, its Deleted-owner
/// join cleanup never ran and the join rows were orphaned. With the inverse join mirrored onto the related
/// type, deleting it now cleans up the join rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeleteRightManyToManyEntityCleanupTests
{
    [Table("C3Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("C3Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public List<Post> Posts { get; set; } = new();
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:c3_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE C3Post (Id INTEGER PRIMARY KEY);
                CREATE TABLE C3Tag (Id INTEGER PRIMARY KEY);
                CREATE TABLE C3PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO C3Post VALUES (1);
                INSERT INTO C3Tag VALUES (1), (2);
                INSERT INTO C3PostTag VALUES (1, 1), (1, 2);
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
                    mb.Entity<Post>().HasMany(p => p.Tags).WithMany(t => t.Posts).UsingTable("C3PostTag", "PostId", "TagId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<int> TagIds(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT TagId FROM C3PostTag ORDER BY TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<int>();
        while (r.Read()) v.Add(r.GetInt32(0));
        return v;
    }

    [Fact]
    public async Task Deleting_right_side_entity_removes_its_join_rows()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var tag2 = ctx.Query<Tag>().ToList().Single(t => t.Id == 2);
        ctx.Remove(tag2);
        await ctx.SaveChangesAsync();

        // Only tag 1's join row should remain; tag 2's join row (1,2) must be cleaned up.
        Assert.Equal(new[] { 1 }, TagIds(keeper).ToArray());
    }
}
