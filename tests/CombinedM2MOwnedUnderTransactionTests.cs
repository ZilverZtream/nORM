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
/// Many-to-many and owned-collection edits spread across MULTIPLE SaveChanges calls inside one caller-owned
/// transaction. Under a caller-owned transaction nORM defers AcceptChanges, so the association baseline the
/// delta sync compares against must still advance between saves — otherwise the second save would re-apply the
/// first save's delta (duplicate join rows / owned children) or lose it. Each committed collection must equal
/// the net effect of all the edits, exactly once.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CombinedM2MOwnedUnderTransactionTests
{
    [Table("TmoPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
        public List<Tag> Tags { get; set; } = new();
    }
    public class Line { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Text { get; set; } = ""; }
    [Table("TmoTag")] public class Tag { [Key] public int Id { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:tmo_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TmoPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE TmoLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                CREATE TABLE TmoTag (Id INTEGER PRIMARY KEY);
                CREATE TABLE TmoPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO TmoPost VALUES (1, 'p');
                INSERT INTO TmoLine VALUES (1, 1, 'a'), (2, 1, 'b');
                INSERT INTO TmoTag VALUES (1), (2), (3);
                INSERT INTO TmoPostTag VALUES (1, 1);
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
                    e.OwnsMany<Line>(p => p.Lines, tableName: "TmoLine", foreignKey: "PostId");
                    e.HasMany<Tag>(p => p.Tags).WithMany().UsingTable("TmoPostTag", "PostId", "TagId");
                }
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<int> Tags(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT TagId FROM TmoPostTag WHERE PostId = 1 ORDER BY TagId";
        using var r = cmd.ExecuteReader();
        var v = new List<int>(); while (r.Read()) v.Add(r.GetInt32(0)); return v;
    }
    private static List<string> Lines(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Text FROM TmoLine WHERE PostId = 1 ORDER BY Text";
        using var r = cmd.ExecuteReader();
        var v = new List<string>(); while (r.Read()) v.Add(r.GetString(0)); return v;
    }
    private static Post Load(DbContext ctx) =>
        ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).Include(p => p.Tags).ToList().Single();

    [Fact]
    public async Task Many_to_many_additions_across_two_saves_in_one_transaction_apply_once_each()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();
        var post = Load(ctx);
        var tag2 = ctx.Query<Tag>().ToList().Single(t => t.Id == 2);
        var tag3 = ctx.Query<Tag>().ToList().Single(t => t.Id == 3);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        post.Tags.Add(tag2); await ctx.SaveChangesAsync();
        post.Tags.Add(tag3); await ctx.SaveChangesAsync();   // must not re-insert the (1,2) join row
        await tx.CommitAsync();

        Assert.Equal(new[] { 1, 2, 3 }, Tags(keeper).ToArray());
    }

    [Fact]
    public async Task Owned_additions_across_two_saves_in_one_transaction_apply_once_each()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();
        var post = Load(ctx);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        post.Lines.Add(new Line { Text = "c" }); await ctx.SaveChangesAsync();
        post.Lines.Add(new Line { Text = "d" }); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(new[] { "a", "b", "c", "d" }, Lines(keeper).ToArray());
    }

    [Fact]
    public async Task Many_to_many_remove_add_then_add_across_two_saves_in_one_transaction()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();
        var post = Load(ctx);
        var tag2 = ctx.Query<Tag>().ToList().Single(t => t.Id == 2);
        var tag3 = ctx.Query<Tag>().ToList().Single(t => t.Id == 3);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        post.Tags.RemoveAll(t => t.Id == 1); post.Tags.Add(tag2); await ctx.SaveChangesAsync();
        post.Tags.Add(tag3); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(new[] { 2, 3 }, Tags(keeper).ToArray());
    }
}
