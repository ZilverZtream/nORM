using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The batched navigation loader is created lazily per context and registered for disposal, so a pool reset
/// disposes it. But it is also stored in a process-wide, context-keyed table that the reset never cleared, so
/// the next lease of the same pooled context received the DISPOSED loader — every subsequent collection
/// lazy/explicit load then threw ObjectDisposedException (or silently returned empty). The reset must drop the
/// loader mapping so a fresh loader is built on the next lease.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PoolResetNavigationLoaderTests
{
    [Table("PrnlParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("PrnlChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    private static DbContext Make(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Parent>()
                .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Collection_load_works_after_the_pooled_context_is_reset_and_reused()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PrnlParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE PrnlChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
                INSERT INTO PrnlParent VALUES (1);
                INSERT INTO PrnlChild VALUES (10, 1), (11, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = Make(cn);

        // Lease 1: an explicit collection load lazily creates the batched navigation loader.
        var p1 = ctx.Query<Parent>().First();
        ctx.Entry(p1).Collection(nameof(Parent.Children)).Load();
        Assert.Equal(2, p1.Children.Count);

        // Return to pool: disposes the loader.
        Assert.True(ctx.TryResetForPooling());

        // Lease 2 (same pooled context): the collection load must still work — a stale disposed loader would
        // throw ObjectDisposedException or silently yield an empty collection.
        var p2 = ctx.Query<Parent>().First();
        ctx.Entry(p2).Collection(nameof(Parent.Children)).Load();
        Assert.Equal(2, p2.Children.Count);
    }
}
