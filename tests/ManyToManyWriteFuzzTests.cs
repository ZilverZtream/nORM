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
/// Seeded oracle machine for many-to-many collection writes across MULTIPLE
/// SaveChanges cycles. Each round applies random add/remove edits to a post's
/// tag collection, saves, and checks the live join table against an in-memory
/// oracle set. The snapshot baseline must refresh after every SaveChanges — a
/// stale snapshot re-adds or fails to remove on the next cycle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyWriteFuzzTests
{
    [Table("M2mfw_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mfw_Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
            mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mfw_PostTag", "PostId", "TagId")
    });

    [Theory]
    [InlineData(20260714)]
    [InlineData(9182)]
    [InlineData(404_113_227)]
    public async Task Random_add_remove_cycles_match_oracle(int seed)
    {
        var rng = new Random(seed);
        var keeper = new SqliteConnection($"Data Source=file:m2mfw_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using var _ = keeper;
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE M2mfw_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE M2mfw_Tag (Id INTEGER PRIMARY KEY);
                CREATE TABLE M2mfw_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO M2mfw_Post VALUES (1, 'p');
                """;
            cmd.ExecuteNonQuery();
            for (var t = 1; t <= 10; t++)
            {
                cmd.CommandText = $"INSERT INTO M2mfw_Tag VALUES ({t})";
                cmd.ExecuteNonQuery();
            }
        }

        var oracle = new HashSet<int>();

        for (var round = 0; round < 12; round++)
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            await using var ctx = Ctx(cn);

            var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
            var allTags = ctx.Query<Tag>().ToList();

            // Verify the loaded collection matches the oracle before editing.
            var loaded = post.Tags.Select(t => t.Id).OrderBy(i => i).ToList();
            Assert.True(oracle.OrderBy(i => i).SequenceEqual(loaded),
                $"seed={seed} round={round}: loaded [{string.Join(",", loaded)}] != oracle [{string.Join(",", oracle.OrderBy(i => i))}]");

            var edits = rng.Next(1, 4);
            for (var e = 0; e < edits; e++)
            {
                if (oracle.Count > 0 && rng.Next(2) == 0)
                {
                    // Remove a random linked tag.
                    var victim = oracle.ElementAt(rng.Next(oracle.Count));
                    post.Tags.RemoveAll(t => t.Id == victim);
                    oracle.Remove(victim);
                }
                else
                {
                    // Add a random unlinked tag.
                    var candidates = allTags.Where(t => !oracle.Contains(t.Id)).ToList();
                    if (candidates.Count == 0) continue;
                    var pick = candidates[rng.Next(candidates.Count)];
                    post.Tags.Add(pick);
                    oracle.Add(pick.Id);
                }
            }

            await ctx.SaveChangesAsync();

            // Verify the live join table matches the oracle.
            using var read = keeper.CreateCommand();
            read.CommandText = "SELECT TagId FROM M2mfw_PostTag WHERE PostId = 1 ORDER BY TagId";
            using var reader = read.ExecuteReader();
            var live = new List<int>();
            while (reader.Read()) live.Add(reader.GetInt32(0));

            Assert.True(oracle.OrderBy(i => i).SequenceEqual(live),
                $"seed={seed} round={round}: live [{string.Join(",", live)}] != oracle [{string.Join(",", oracle.OrderBy(i => i))}]");
        }
    }

    [Fact]
    public async Task New_owner_with_linked_tags_populates_join_table()
    {
        var keeper = new SqliteConnection($"Data Source=file:m2mfwn_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using var _ = keeper;
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE M2mfw_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE M2mfw_Tag (Id INTEGER PRIMARY KEY);
                CREATE TABLE M2mfw_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO M2mfw_Tag VALUES (1), (2), (3);
                """;
            cmd.ExecuteNonQuery();
        }

        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        await using var ctx = Ctx(cn);

        var tags = ctx.Query<Tag>().ToList();
        var post = new Post { Id = 5, Title = "new" };
        post.Tags.Add(tags.Single(t => t.Id == 1));
        post.Tags.Add(tags.Single(t => t.Id == 3));
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        using var read = keeper.CreateCommand();
        read.CommandText = "SELECT PostId, TagId FROM M2mfw_PostTag ORDER BY TagId";
        using var reader = read.ExecuteReader();
        var live = new List<(int, int)>();
        while (reader.Read()) live.Add((reader.GetInt32(0), reader.GetInt32(1)));

        Assert.Equal(new[] { (5, 1), (5, 3) }, live.ToArray());
    }

    [Fact]
    public async Task Same_context_multiple_saves_refresh_snapshot()
    {
        var keeper = new SqliteConnection($"Data Source=file:m2mfws_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using var _ = keeper;
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE M2mfw_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE M2mfw_Tag (Id INTEGER PRIMARY KEY);
                CREATE TABLE M2mfw_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO M2mfw_Post VALUES (1, 'p');
                INSERT INTO M2mfw_Tag VALUES (1), (2), (3);
                INSERT INTO M2mfw_PostTag VALUES (1, 1);
                """;
            cmd.ExecuteNonQuery();
        }

        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        await using var ctx = Ctx(cn);

        var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        var tags = ctx.Query<Tag>().ToList();

        // Save 1: add tag 2.  Snapshot must refresh to {1,2} after AcceptChanges.
        post.Tags.Add(tags.Single(t => t.Id == 2));
        await ctx.SaveChangesAsync();

        // Save 2: on the SAME context/entity, remove tag 1 and add tag 3. A stale
        // snapshot from before save 1 would re-insert tag 2 or mishandle the delta.
        post.Tags.RemoveAll(t => t.Id == 1);
        post.Tags.Add(tags.Single(t => t.Id == 3));
        await ctx.SaveChangesAsync();

        using var read = keeper.CreateCommand();
        read.CommandText = "SELECT TagId FROM M2mfw_PostTag WHERE PostId = 1 ORDER BY TagId";
        using var reader = read.ExecuteReader();
        var live = new List<int>();
        while (reader.Read()) live.Add(reader.GetInt32(0));

        Assert.Equal(new[] { 2, 3 }, live.ToArray());
    }
}
