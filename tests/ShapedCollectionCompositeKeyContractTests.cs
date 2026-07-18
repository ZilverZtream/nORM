using System;
using System.Collections.Generic;
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
/// Hardens the split-query shaped-collection features (filtered Include and Stage 2 element projection)
/// against COMPOSITE parent keys. Two groups share the same Code but differ in TenantId, so a stitch that
/// correlated children by only one key column would cross them — the composite key must match on the full
/// tuple.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionCompositeKeyContractTests
{
    [Table("CkGrp")]
    public class Grp
    {
        public int TenantId { get; set; }
        public int Code { get; set; }
        public List<Item> Items { get; set; } = new();
    }

    [Table("CkItem")]
    public class Item
    {
        public int Id { get; set; }
        public int TenantId { get; set; }
        public int GrpCode { get; set; }
        public string Name { get; set; } = "";
        public bool Active { get; set; }
    }

    public class ItemDto { public string Name { get; set; } = ""; }
    public class GrpDto { public int TenantId { get; set; } public int Code { get; set; } public List<ItemDto> Items { get; set; } = new(); }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CkGrp (TenantId INTEGER NOT NULL, Code INTEGER NOT NULL, PRIMARY KEY (TenantId, Code));
                CREATE TABLE CkItem (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, GrpCode INTEGER NOT NULL, Name TEXT NOT NULL, Active INTEGER NOT NULL);
                INSERT INTO CkGrp VALUES (1,100),(2,100);
                INSERT INTO CkItem VALUES (1,1,100,'a',1),(2,1,100,'b',0),(3,2,100,'c',1),(4,2,100,'d',1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Grp>().HasKey(g => new { g.TenantId, g.Code });
            mb.Entity<Item>().HasKey(i => i.Id);
            mb.Entity<Grp>().HasMany(g => g.Items).WithOne()
                .HasForeignKey(i => new { i.TenantId, i.GrpCode }, g => new { g.TenantId, g.Code });
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Filtered_include_stitches_by_the_full_composite_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        var groups = ((INormQueryable<Grp>)ctx.Query<Grp>())
            .Include(g => g.Items.Where(i => i.Active))
            .ToList().OrderBy(g => g.TenantId).ToList();

        // Tenant 1 / Code 100 → active item 'a' only (b is inactive); tenant 2 / Code 100 → 'c','d'.
        Assert.Equal(new[] { "a" }, groups[0].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
        Assert.Equal(new[] { "c", "d" }, groups[1].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
    }

    [Fact]
    public async Task Element_projection_stitches_by_the_full_composite_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dtos = (await ctx.Query<Grp>()
            .Select(g => new GrpDto { TenantId = g.TenantId, Code = g.Code, Items = g.Items.Select(i => new ItemDto { Name = i.Name }).ToList() })
            .ToListAsync()).OrderBy(d => d.TenantId).ToList();

        // Composite match: tenant 1 gets a,b; tenant 2 gets c,d — never crossed despite the shared Code.
        Assert.Equal(new[] { "a", "b" }, dtos[0].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
        Assert.Equal(new[] { "c", "d" }, dtos[1].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
    }
}
