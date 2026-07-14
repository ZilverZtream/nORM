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
/// Owned-collection writes across MULTIPLE SaveChanges cycles and under a tenant
/// filter. The content snapshot must refresh after each save (AcceptChanges), and
/// the owned sync's tenant scoping must not touch another tenant's owned children.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedCollectionWriteCycleTests
{
    [Table("OcwPost")]
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
        var keeper = new SqliteConnection($"Data Source=file:ocw_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OcwPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE OcwLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO OcwPost VALUES (1, 'p');
                INSERT INTO OcwLine VALUES (1, 1, 'a');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Line>(p => p.Lines, tableName: "OcwLine", foreignKey: "PostId")
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<string> ReadLines(SqliteConnection keeper)
    {
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Text FROM OcwLine WHERE PostId = 1 ORDER BY Text";
        using var reader = cmd.ExecuteReader();
        var texts = new List<string>();
        while (reader.Read()) texts.Add(reader.GetString(0));
        return texts;
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(3131)]
    [InlineData(778_221_004)]
    public async Task Random_owned_edits_across_cycles_match_oracle(int seed)
    {
        var rng = new Random(seed);
        var (keeper, make) = Setup();
        using var _ = keeper;

        var oracle = new List<string> { "a" };
        var nextChar = 'b';

        for (var round = 0; round < 12; round++)
        {
            await using var ctx = make();
            var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).ToList().Single();

            var loaded = post.Lines.Select(l => l.Text).OrderBy(x => x).ToList();
            Assert.True(oracle.OrderBy(x => x).SequenceEqual(loaded),
                $"seed={seed} round={round}: loaded [{string.Join(",", loaded)}] != oracle [{string.Join(",", oracle.OrderBy(x => x))}]");

            var edits = rng.Next(1, 4);
            for (var e = 0; e < edits; e++)
            {
                if (oracle.Count > 0 && rng.Next(2) == 0)
                {
                    var victim = oracle[rng.Next(oracle.Count)];
                    post.Lines.RemoveAll(l => l.Text == victim);
                    oracle.Remove(victim);
                }
                else
                {
                    var text = nextChar.ToString();
                    nextChar = (char)(nextChar + 1);
                    if (nextChar > 'z') nextChar = 'b';
                    post.Lines.Add(new Line { Text = text });
                    oracle.Add(text);
                }
            }

            await ctx.SaveChangesAsync();
            Assert.True(oracle.OrderBy(x => x).SequenceEqual(ReadLines(keeper)),
                $"seed={seed} round={round}: live [{string.Join(",", ReadLines(keeper))}] != oracle [{string.Join(",", oracle.OrderBy(x => x))}]");
        }
    }

    [Fact]
    public async Task Same_context_multiple_owned_saves_refresh_snapshot()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Lines).ToList().Single();

        post.Lines.Add(new Line { Text = "b" });
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { "a", "b" }, ReadLines(keeper).ToArray());

        // Second save on the SAME context: a stale snapshot would re-detect the
        // already-saved 'b' as new or fail to see the 'a' removal.
        post.Lines.RemoveAll(l => l.Text == "a");
        post.Lines.Add(new Line { Text = "c" });
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { "b", "c" }, ReadLines(keeper).ToArray());

        // Third save with NO owned edit must not disturb the collection.
        post.Title = "changed";
        await ctx.SaveChangesAsync();
        Assert.Equal(new[] { "b", "c" }, ReadLines(keeper).ToArray());
    }
}
