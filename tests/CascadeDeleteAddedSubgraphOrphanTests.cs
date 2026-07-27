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
/// Removing a never-persisted (Added) principal whose in-memory graph is multiple levels deep must detach
/// the WHOLE Added subgraph — not just the immediate Added dependents. If a cascaded Added dependent's own
/// Added children (grandchildren) are left tracked, SaveChanges inserts them with a foreign key pointing at
/// a row that was never inserted (default 0) — an orphan on SQLite, an FK violation elsewhere.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CascadeDeleteAddedSubgraphOrphanTests
{
    [Table("CdaParent")]
    public class Parent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("CdaChild")]
    public class Child
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }
        public List<GrandChild> Grands { get; set; } = new();
    }

    [Table("CdaGrand")]
    public class GrandChild
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ChildId { get; set; }
    }

    private static DbContext CreateDb(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CdaParent (Id INTEGER PRIMARY KEY AUTOINCREMENT);
                CREATE TABLE CdaChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL);
                CREATE TABLE CdaGrand (Id INTEGER PRIMARY KEY AUTOINCREMENT, ChildId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<GrandChild>().HasKey(g => g.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
                mb.Entity<Child>().HasMany(c => c.Grands).WithOne().HasForeignKey(g => g.ChildId, c => c.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static long Count(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Removing_an_added_principal_detaches_the_whole_added_subgraph()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateDb(cn);

        var parent = new Parent();
        ctx.Add(parent);
        await ctx.SaveChangesAsync();   // parent PERSISTED (Id assigned)

        // Attach a NEW multi-level subgraph (child + grandchild, both Added) under the persisted parent,
        // explicitly tracking them so both are Added before the cascade runs.
        var child = new Child { ParentId = parent.Id };
        var grand = new GrandChild();
        child.Grands.Add(grand);
        ctx.Add(child);                 // graph-adds child + grand as Added
        parent.Children.Add(child);

        // ...then delete the parent: the cascade must detach the whole Added subgraph, not just the child.
        ctx.Remove(parent);
        await ctx.SaveChangesAsync();

        Assert.Equal(0, Count(cn, "CdaParent"));  // parent deleted
        Assert.Equal(0, Count(cn, "CdaChild"));   // added child cascade-detached (never inserted)
        Assert.Equal(0, Count(cn, "CdaGrand"));   // BUG: 1 — the grandchild was inserted as an orphan
    }
}
