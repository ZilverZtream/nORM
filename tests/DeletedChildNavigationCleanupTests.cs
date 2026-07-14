using System.Collections.Generic;
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
/// A deleted child that still sits in a tracked parent's navigation collection
/// must NOT be re-discovered by relationship fixup on a later SaveChanges: the
/// original defect re-inserted the deleted row (silent resurrection), because
/// after the delete its tracker entry was gone and the nav still held the
/// instance.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeletedChildNavigationCleanupTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NavClean_Parent")]
    public class NavCleanParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NavCleanChild> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("NavClean_Child")]
    public class NavCleanChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task Deleted_child_still_in_a_tracked_navigation_stays_deleted_across_saves()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavClean_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE NavClean_Child (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NavCleanParent>().HasKey(p => p.Id);
                mb.Entity<NavCleanChild>().HasKey(c => c.Id);
                mb.Entity<NavCleanParent>().HasMany(p => p.Children).WithOne()
                                           .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var parent = new NavCleanParent { Id = 1, Name = "p" };
        var child = new NavCleanChild { Id = 10, Val = 5 };
        parent.Children.Add(child);
        ctx.Add(parent);
        await ctx.SaveChangesAsync(); // fixup inserts the child with ParentId = 1

        Assert.Single(await ctx.Query<NavCleanChild>().ToListAsync());

        // Delete the child WITHOUT removing it from the parent's collection —
        // the common user gesture. The delete lands...
        ctx.Remove(child);
        await ctx.SaveChangesAsync();
        Assert.Empty(await ctx.Query<NavCleanChild>().ToListAsync());

        // ...and a later save must not resurrect it from the stale navigation.
        parent.Name = "renamed";
        await ctx.SaveChangesAsync();
        Assert.Empty(await ctx.Query<NavCleanChild>().ToListAsync());
        Assert.DoesNotContain(child, parent.Children);
    }
}
