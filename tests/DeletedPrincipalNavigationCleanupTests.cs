using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class DeletedPrincipalNavigationCleanupTests
{
    private readonly ITestOutputHelper _output;
    public DeletedPrincipalNavigationCleanupTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("DelPrin_Parent")]
    public class DelPrinParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<DelPrinChild> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("DelPrin_Child")]
    public class DelPrinChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public DelPrinParent? Parent { get; set; }
    }

    [Fact]
    public async Task Deleted_principal_referenced_by_a_tracked_child_nav_does_not_resurrect()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DelPrin_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE DelPrin_Child (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<DelPrinParent>().HasKey(p => p.Id);
                mb.Entity<DelPrinChild>().HasKey(c => c.Id);
                mb.Entity<DelPrinParent>().HasMany(p => p.Children).WithOne()
                                          .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var parent = new DelPrinParent { Id = 1, Name = "p" };
        var child = new DelPrinChild { Id = 10, Val = 5, Parent = parent };
        ctx.Add(parent);
        ctx.Add(child);
        await ctx.SaveChangesAsync();

        // Delete the parent while the child's Parent navigation still points at
        // it (the database has no enforced FK here — the orphaned child stays).
        ctx.Remove(parent);
        await ctx.SaveChangesAsync();
        Assert.Empty(await ctx.Query<DelPrinParent>().ToListAsync());

        // A later unrelated save must not rediscover the deleted principal via
        // the child's stale navigation and silently re-insert it.
        child.Val = 6;
        await ctx.SaveChangesAsync();

        ctx.ChangeTracker.Clear();
        var parentsAfter = await ctx.Query<DelPrinParent>().ToListAsync();
        _output.WriteLine($"parents after: [{string.Join(",", parentsAfter.Select(p => p.Id))}]");
        Assert.Empty(parentsAfter);
    }
}

