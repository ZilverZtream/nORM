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
public class CascadeDeleteTests
{
    private readonly ITestOutputHelper _output;
    public CascadeDeleteTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("Casc_Parent")]
    public class CascParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<CascChild> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("Casc_Child")]
    public class CascChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Casc_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE Casc_Child (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CascParent>().HasKey(p => p.Id);
                mb.Entity<CascChild>().HasKey(c => c.Id);
                mb.Entity<CascParent>().HasMany(p => p.Children).WithOne()
                                       .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), options));
    }

    [Fact]
    public async Task Removing_a_parent_cascades_to_children_present_in_the_navigation()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var parent = new CascParent { Id = 1, Name = "p" };
        parent.Children.Add(new CascChild { Id = 10, Val = 1 });
        parent.Children.Add(new CascChild { Id = 11, Val = 2 });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();
        Assert.Equal(2, (await ctx.Query<CascChild>().ToListAsync()).Count);

        // Children are in the tracked navigation — Remove cascades to them.
        ctx.Remove(parent);
        await ctx.SaveChangesAsync();

        _output.WriteLine($"parents: {(await ctx.Query<CascParent>().ToListAsync()).Count}");
        var children = await ctx.Query<CascChild>().ToListAsync();
        _output.WriteLine($"children: [{string.Join(",", children.Select(c => c.Id))}]");
        Assert.Empty(await ctx.Query<CascParent>().ToListAsync());
        Assert.Empty(children);
    }

    [Fact]
    public async Task Removing_a_parent_with_an_unloaded_navigation_leaves_children_orphaned()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var parent = new CascParent { Id = 1, Name = "p" };
        parent.Children.Add(new CascChild { Id = 10, Val = 1 });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        // Fresh context: the requeried parent has an EMPTY navigation, so the
        // tracked-graph cascade has nothing to traverse — the child stays.
        ctx.ChangeTracker.Clear();
        var requeried = await ctx.Query<CascParent>().FirstAsync(p => p.Id == 1);
        Assert.Empty(requeried.Children);
        ctx.Remove(requeried);
        await ctx.SaveChangesAsync();

        var children = await ctx.Query<CascChild>().ToListAsync();
        _output.WriteLine($"children after: [{string.Join(",", children.Select(c => c.Id))}]");
        Assert.Empty(await ctx.Query<CascParent>().ToListAsync());
        Assert.Single(children); // orphaned, per the tracked-graph cascade contract
    }
}

