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
/// Modifying an owner and saving must not delete its owned-collection children when the collection
/// was never loaded. SaveOwnedCollectionsAsync DELETE-then-INSERTs a Modified owner's owned children
/// from the in-memory nav; the sync query path did not load owned collections, so editing an
/// unrelated scalar on a sync-loaded owner permanently deleted every owned child — silent data loss.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class OwnedCollectionUnloadedModifyTests
{
    [Table("OcuPost")]
    private class OcuPost
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<OcuTag> Tags { get; set; } = new();
    }

    private class OcuTag
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE OcuPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
                "CREATE TABLE OcuTag (Id INTEGER PRIMARY KEY AUTOINCREMENT, OcuPostId INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO OcuPost VALUES (1, 'original');" +
                "INSERT INTO OcuTag VALUES (1, 1, 'csharp'), (2, 1, 'dotnet');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<OcuPost>().OwnsMany<OcuTag>(p => p.Tags, tableName: "OcuTag", foreignKey: "OcuPostId")
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static int TagCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM OcuTag WHERE OcuPostId = 1";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Sync_loaded_owner_scalar_edit_preserves_owned_children()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // Sync load (owned collection not eagerly materialized), edit an unrelated scalar.
        var post = ctx.Query<OcuPost>().First(p => p.Id == 1);
        post.Title = "changed";
        await ctx.SaveChangesAsync();

        Assert.Equal(2, TagCount(cn)); // the two owned Tag rows must survive
    }
}
