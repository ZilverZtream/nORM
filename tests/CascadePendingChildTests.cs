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
public class CascadePendingChildTests
{
    private readonly ITestOutputHelper _output;
    public CascadePendingChildTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("CascP_Parent")]
    public class CascPParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<CascPChild> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CascP_Child")]
    public class CascPChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public CascPParent? Parent { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CascP_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CascP_Child (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CascPParent>().HasKey(p => p.Id);
                mb.Entity<CascPChild>().HasKey(c => c.Id);
                mb.Entity<CascPParent>().HasMany(p => p.Children).WithOne()
                                        .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), options));
    }

    [Fact]
    public async Task Added_child_referencing_a_deleted_principal_by_nav_does_not_insert_dangling()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var parent = new CascPParent { Id = 1, Name = "p" };
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        // Pending child linked ONLY by reference nav (FK still 0), then the
        // principal dies before the save.
        var child = new CascPChild { Id = 10, Val = 5, Parent = parent };
        ctx.Add(child);
        ctx.Remove(parent);
        await ctx.SaveChangesAsync();

        var children = await ctx.Query<CascPChild>().ToListAsync();
        _output.WriteLine($"children: [{string.Join(",", children.Select(c => $"{c.Id}:fk={c.ParentId}"))}]");
        Assert.Empty(await ctx.Query<CascPParent>().ToListAsync());
        Assert.Empty(children); // the pending child dies with its principal
    }

    [Fact]
    public async Task Added_child_in_a_deleted_principals_collection_does_not_insert_dangling()
    {
        var (cn, ctx) = Create();
        using var _ = cn;
        using var __ = ctx;

        var parent = new CascPParent { Id = 1, Name = "p" };
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        // Pending child explicitly Added AND sitting in the principal's
        // collection (FK still 0), principal deleted before the save.
        var child = new CascPChild { Id = 10, Val = 5 };
        parent.Children.Add(child);
        ctx.Add(child);
        ctx.Remove(parent);
        await ctx.SaveChangesAsync();

        var children = await ctx.Query<CascPChild>().ToListAsync();
        _output.WriteLine($"children: [{string.Join(",", children.Select(c => $"{c.Id}:fk={c.ParentId}"))}]");
        Assert.Empty(children);
    }
}

