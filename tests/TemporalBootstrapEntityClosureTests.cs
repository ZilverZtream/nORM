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
/// The temporal bootstrap versions the closure of real entities reachable from configured/queried roots
/// through navigations — NOT a physical-table probe (which is scoped to the default schema and would silently
/// skip a real table an unqualified mapping resolves to in a non-default login schema). A child reachable only
/// through a parent's relationship (never configured or queried as a root) must still be versioned; a query
/// projection type must not be.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalBootstrapEntityClosureTests
{
    [Table("TbcParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    // Attribute-mapped only — deliberately NOT configured via mb.Entity<Child>() and never queried as a root,
    // so it enters the mapping set solely as the relationship target discovered from Parent.
    [Table("TbcChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    private static bool TableExists(SqliteConnection cn, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=@n";
        cmd.Parameters.AddWithValue("@n", name);
        return Convert.ToInt32(cmd.ExecuteScalar()) > 0;
    }

    [Fact]
    public async Task relationship_reachable_child_is_versioned_even_though_it_is_not_a_configured_root()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TbcParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE TbcChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                // Only the parent is configured; Child is reached purely as the relationship target.
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        ctx.Add(new Parent { Id = 1 });
        await ctx.SaveChangesAsync();   // first connection → temporal bootstrap runs

        // The child is in the entity closure (relationship target of the configured parent), so its history
        // table must exist even though it was never configured or queried as a root.
        Assert.True(TableExists(cn, "TbcChild_History"), "relationship-reachable child was not versioned");
        Assert.True(TableExists(cn, "TbcParent_History"));
    }

    public class NakedDto { public int Id { get; set; } public int Val { get; set; } }

    [Fact]
    public async Task projection_type_cached_by_a_query_is_not_versioned()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TbcChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Child>().HasKey(c => c.Id)
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Project into a DTO that carries a convention 'Id' key — this caches NakedDto as a mapping. The
        // bootstrap (run on this first connection) must NOT create a history table for the phantom DTO table.
        _ = await ctx.Query<Child>().Select(c => new NakedDto { Id = c.Id, Val = c.Val }).ToListAsync();

        Assert.False(TableExists(cn, "NakedDto_History"), "a projection type was wrongly versioned");
        Assert.True(TableExists(cn, "TbcChild_History"));   // the real entity still is
    }
}
