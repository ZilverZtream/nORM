using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adding a NEW, never-tracked entity to a many-to-many (skip) navigation — post.Tags.Add(new Tag{...})
/// without ctx.Add(tag) — must insert that entity and create the join row referencing its hydrated key,
/// the same way EF Core's fixup discovers and inserts it. Relationship fixup walked only 1:N relations and
/// reference navigations, never the m2m joins, so the new entity was never tracked/inserted: it was lost
/// and the join row was written with a default (0) key.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class M2MUntrackedRelatedEntityInsertTests
{
    [Table("MurPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("MurTag")]
    public class Tag
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext CreateDb(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MurPost (Id INTEGER PRIMARY KEY AUTOINCREMENT);
                CREATE TABLE MurTag (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                CREATE TABLE MurPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY (PostId, TagId));
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>()
                .HasMany<Tag>(p => p.Tags)
                .WithMany()
                .UsingTable("MurPostTag", "PostId", "TagId")
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static long Count(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)cmd.ExecuteScalar()!;
    }

    private static long JoinTagIdZero(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM MurPostTag WHERE TagId = 0";
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task New_untracked_tag_added_to_m2m_collection_is_inserted_and_linked()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateDb(cn);

        var post = new Post();
        ctx.Add(post);
        await ctx.SaveChangesAsync();   // post.Id assigned

        post.Tags.Add(new Tag { Name = "new" });   // NEW tag, NOT ctx.Add()'d
        await ctx.SaveChangesAsync();

        Assert.Equal(1, Count(cn, "MurTag"));       // BUG: 0 — the new tag was lost
        Assert.Equal(1, Count(cn, "MurPostTag"));   // one join row...
        Assert.Equal(0, JoinTagIdZero(cn));         // ...referencing the tag's hydrated key, not 0
    }
}
