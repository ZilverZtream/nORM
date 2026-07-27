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
/// Under a caller-owned transaction, an entity inserted by an earlier SaveChanges stays Added (so a
/// rollback can re-insert it). A many-to-many association ADDED to that still-Added entity by a LATER
/// SaveChanges in the same transaction must still be synced — but the deferred M2M sync was built from the
/// INSERT batch, which excludes already-in-tx-inserted entities, so the join row was silently dropped and
/// the association was lost on commit. (Sibling of the scalar modify-after-insert-in-tx handling.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class M2MAssociationAfterInTxInsertTests
{
    [Table("MaiPost")]
    public class Post
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("MaiTag")]
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
                CREATE TABLE MaiPost (Id INTEGER PRIMARY KEY AUTOINCREMENT);
                CREATE TABLE MaiTag (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                CREATE TABLE MaiPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY (PostId, TagId));
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>()
                .HasMany<Tag>(p => p.Tags)
                .WithMany()
                .UsingTable("MaiPostTag", "PostId", "TagId")
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static long Count(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task M2M_association_added_after_in_tx_insert_is_synced_on_commit()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateDb(cn);

        var tag = new Tag { Name = "existing" };
        ctx.Add(tag);
        await ctx.SaveChangesAsync();   // existing tag persisted

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            var post = new Post();
            ctx.Add(post);
            await ctx.SaveChangesAsync();       // post inserted, stays Added (caller-owned tx)

            post.Tags.Add(tag);                 // associate an EXISTING tag with the still-Added post
            await ctx.SaveChangesAsync();       // must sync the join row
            await tx.CommitAsync();
        }

        Assert.Equal(1, Count(cn, "MaiPost"));
        Assert.Equal(1, Count(cn, "MaiPostTag"));   // BUG: 0 — the join row was dropped
    }
}
