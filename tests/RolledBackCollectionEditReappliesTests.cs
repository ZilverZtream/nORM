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
/// Under a caller-owned transaction nORM defers AcceptChanges and advances the m2m / owned-collection snapshot
/// after each save so a within-transaction reversal is detected. But a ROLLBACK must also restore that snapshot,
/// exactly as it restores scalar baselines and OCC tokens — otherwise the advanced snapshot equals the current
/// (still-edited) collection, the next SaveChanges sees no delta, writes nothing, and the association the user
/// still holds is silently dropped: the in-memory model and the database permanently disagree with no error.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class RolledBackCollectionEditReappliesTests
{
    [Table("RbcPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
        public List<Tag> Tags { get; set; } = new();
    }
    public class Line { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Text { get; set; } = ""; }
    [Table("RbcTag")] public class Tag { [Key] public int Id { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:rbc_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE RbcPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE RbcLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                CREATE TABLE RbcTag (Id INTEGER PRIMARY KEY);
                CREATE TABLE RbcPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO RbcPost VALUES (1, 'p');
                INSERT INTO RbcLine VALUES (1, 1, 'a'), (2, 1, 'b');
                INSERT INTO RbcTag VALUES (1), (2), (3);
                INSERT INTO RbcPostTag VALUES (1, 1);
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
                    e.OwnsMany<Line>(p => p.Lines, tableName: "RbcLine", foreignKey: "PostId");
                    e.HasMany<Tag>(p => p.Tags).WithMany().UsingTable("RbcPostTag", "PostId", "TagId");
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<int> DbTags(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT TagId FROM RbcPostTag WHERE PostId = 1 ORDER BY TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<int>(); while (r.Read()) v.Add(r.GetInt32(0)); return v;
    }
    private static List<string> DbLines(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Text FROM RbcLine WHERE PostId = 1 ORDER BY Text";
        using var r = cmd.ExecuteReader();
        var v = new List<string>(); while (r.Read()) v.Add(r.GetString(0)); return v;
    }
    private static Post Load(DbContext ctx) =>
        ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).Include(p => p.Tags).ToList().Single();

    [Fact]
    public async Task Rolled_back_m2m_add_reapplies_on_next_save()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();
        var post = Load(ctx);
        var tag3 = ctx.Query<Tag>().ToList().Single(t => t.Id == 3);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            post.Tags.Add(tag3);
            await ctx.SaveChangesAsync();   // inserts join (1,3) in tx; advances snapshot -> {1,3}
            await tx.RollbackAsync();        // DB reverts to {1}; snapshot must revert too
        }

        // The rollback genuinely reverted the DB.
        Assert.Equal(new[] { 1 }, DbTags(keeper).ToArray());

        // post.Tags still holds tag3; the re-save must re-apply it, not silently no-op.
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { 1, 3 }, DbTags(keeper).ToArray());
    }

    [Fact]
    public async Task Rolled_back_owned_collection_add_reapplies_on_next_save()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();
        var post = Load(ctx);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            post.Lines.Add(new Line { Text = "c" });
            await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }

        Assert.Equal(new[] { "a", "b" }, DbLines(keeper).ToArray());

        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { "a", "b", "c" }, DbLines(keeper).ToArray());
    }
}
