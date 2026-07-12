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
/// docs/change-tracking.md: "When a new dependent is added via principal.Children.Add(child), the
/// tracker promotes child to Added and sets the FK column from the principal's PK (or DB-generated
/// default if the principal is also Added)." Verifies that relationship-fixup contract — without it,
/// adding through a collection navigation is a silent lost insert.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class RelationshipFixupChildrenAddTests
{
    [Table("RfCategory")]
    private class RfCategory
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<RfItem> Children { get; set; } = new();
    }

    [Table("RfItem")]
    private class RfItem
    {
        [Key] public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Label { get; set; } = "";
    }

    [Table("RfAutoParent")]
    private class RfAutoParent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<RfAutoChild> Children { get; set; } = new();
    }

    [Table("RfAutoChild")]
    private class RfAutoChild
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext CreateContext(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE RfCategory (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE RfItem (Id INTEGER PRIMARY KEY, CategoryId INTEGER NOT NULL, Label TEXT NOT NULL);" +
                "CREATE TABLE RfAutoParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                "CREATE TABLE RfAutoChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Label TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<RfCategory>().HasKey(c => c.Id)
                    .HasMany(c => c.Children).WithOne().HasForeignKey(i => i.CategoryId, c => c.Id);
                mb.Entity<RfAutoParent>().HasKey(p => p.Id)
                    .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Adding_child_to_tracked_principal_inserts_with_fk()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        ctx.Add(new RfCategory { Id = 1, Name = "root" });
        await ctx.SaveChangesAsync();

        var category = ctx.Query<RfCategory>().Single(c => c.Id == 1);
        category.Children.Add(new RfItem { Id = 5, Label = "child" });
        await ctx.SaveChangesAsync();

        var items = ((INormQueryable<RfItem>)ctx.Query<RfItem>()).AsNoTracking().ToList();
        var item = Assert.Single(items);
        Assert.Equal(5, item.Id);
        Assert.Equal(1, item.CategoryId);
        Assert.Equal("child", item.Label);
    }

    [Fact]
    public async Task Adding_new_graph_with_db_generated_keys_links_children_to_hydrated_parent_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        var parent = new RfAutoParent { Name = "p" };
        parent.Children.Add(new RfAutoChild { Label = "c1" });
        parent.Children.Add(new RfAutoChild { Label = "c2" });
        ctx.Add(parent); // only the parent is explicitly tracked; children discovered via fixup
        await ctx.SaveChangesAsync();

        Assert.True(parent.Id > 0);

        // Both children must reference the parent's DB-generated key. Read raw to avoid the identity map.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT ParentId, Label FROM RfAutoChild ORDER BY Label";
        using var r = cmd.ExecuteReader();
        var rows = new List<(int ParentId, string Label)>();
        while (r.Read()) rows.Add((r.GetInt32(0), r.GetString(1)));
        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(parent.Id, row.ParentId));
        Assert.Equal(new[] { "c1", "c2" }, rows.Select(x => x.Label));
    }
}
