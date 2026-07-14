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
/// Owned children with DB-generated identity keys must keep their identities when
/// the owner is saved for an UNRELATED change (a scalar edit, or a different
/// collection's edit). SaveOwnedCollectionsAsync re-writes owned children by
/// DELETE-then-INSERT for any Modified owner; if it runs when the owned
/// collection did not change, every child gets a brand-new identity — churning
/// keys that other rows/references may point at, and generating needless writes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedChildIdentityStabilityTests
{
    [Table("OciPost")]
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
        var keeper = new SqliteConnection($"Data Source=file:oci_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OciPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE OciLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO OciPost VALUES (1, 'p');
                INSERT INTO OciLine VALUES (10, 1, 'a'), (11, 1, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Line>(p => p.Lines, tableName: "OciLine", foreignKey: "PostId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<(int Id, string Text)> ReadLines(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Id, Text FROM OciLine WHERE PostId = 1 ORDER BY Text";
        using var r = cmd.ExecuteReader();
        var v = new List<(int, string)>();
        while (r.Read()) v.Add((r.GetInt32(0), r.GetString(1)));
        return v;
    }

    [Fact]
    public async Task Scalar_only_owner_edit_keeps_owned_child_ids()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var before = ReadLines(keeper);
        Assert.Equal(new[] { (10, "a"), (11, "b") }, before.ToArray());

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).ToList().Single();
        post.Title = "changed"; // no owned-collection change
        await ctx.SaveChangesAsync();

        var after = ReadLines(keeper);
        Assert.Equal(new[] { (10, "a"), (11, "b") }, after.ToArray()); // IDs unchanged
    }
}
