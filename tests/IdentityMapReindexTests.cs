using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
#pragma warning disable CS8600, CS8602

namespace nORM.Tests;

/// <summary>
/// Verifies that after SaveChangesAsync assigns a DB-generated identity key to an entity,
/// the entity is correctly re-indexed in the identity map so that subsequent Attach calls do
/// not create a duplicate ChangeTracker entry.
/// </summary>
public class IdentityMapReindexTests
{
    [Table("ReindexPost")]
    private class Post
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    private static (SqliteConnection, DbContext) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE ReindexPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL)";
        setup.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    [Fact]
    public async Task AfterInsert_EntityIsReindexed_NoDuplicateEntry()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = cn;
        using var __ = ctx;

        var post = new Post { Title = "Hello" };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        // After save, Id should have been set by the DB
        Assert.True(post.Id > 0, "Expected DB-generated key to be assigned after SaveChanges.");

        // Re-attach the same reference via Attach — should NOT create a second entry
        ctx.Attach(post);

        var entries = ctx.ChangeTracker.Entries.ToList();
        Assert.Single(entries); // exactly one entry, not two
        Assert.Equal(post, entries[0].Entity);
    }

    [Fact]
    public async Task AfterInsert_EntityCanBeFoundByPk_InIdentityMap()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = cn;
        using var __ = ctx;

        var post = new Post { Title = "World" };
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        Assert.True(post.Id > 0);

        // The identity map lookup should find it (no duplicate / missing entry)
        var entries = ctx.ChangeTracker.Entries.ToList();
        Assert.Single(entries);
        Assert.Equal(EntityState.Unchanged, entries[0].State);
    }
}
