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
/// After a child is deleted, it must be stripped from every tracked principal's collection navigation so
/// the next SaveChanges' relationship fixup cannot rediscover the (now untracked) instance and re-insert
/// it. The cleanup only handled navigations whose runtime type is IList (List&lt;T&gt;); a HashSet&lt;T&gt; / ISet&lt;T&gt; /
/// non-IList ICollection&lt;T&gt; navigation was skipped, so the deleted child stayed in the collection and was
/// silently RE-INSERTED (resurrected) on the next save.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeletedChildInHashSetNavResurrectionTests
{
    [Table("DcrsParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public ICollection<Child> Children { get; set; } = new HashSet<Child>();
    }

    [Table("DcrsChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    private static DbContext CreateDb(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DcrsParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE DcrsChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static long ChildCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM DcrsChild";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Deleting_a_child_in_a_hashset_navigation_does_not_resurrect_it()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = CreateDb(cn);

        var parent = new Parent { Id = 1 };
        ctx.Add(parent);
        await ctx.SaveChangesAsync();
        var child = new Child { Id = 1, ParentId = 1 };
        ctx.Add(child);
        await ctx.SaveChangesAsync();

        // The principal's collection navigation is a HashSet holding the (tracked, Unchanged) child.
        parent.Children = new HashSet<Child> { child };

        ctx.Remove(child);
        await ctx.SaveChangesAsync();          // DELETE runs; child left in the HashSet, tracker entry gone
        Assert.Equal(0, ChildCount(cn));

        await ctx.SaveChangesAsync();          // must NOT re-insert the deleted child
        Assert.Equal(0, ChildCount(cn));       // BUG: 1 — fixup resurrected the child
    }
}
