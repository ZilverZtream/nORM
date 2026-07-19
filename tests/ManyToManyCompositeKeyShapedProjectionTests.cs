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
/// Hardens shaping a MANY-TO-MANY collection into a projection against a COMPOSITE left key. Two groups share
/// the same Code but differ in TenantId; the bridge correlation must match the full (TenantId, Code) tuple, or
/// a projection would cross-load the other group's related items. Covers the same composite-stitch contract the
/// relation path pins (<see cref="ShapedCollectionCompositeKeyContractTests"/>) for the m2m split-query loader.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyCompositeKeyShapedProjectionTests
{
    [Table("CkM2mGrp")]
    public class Grp
    {
        public int TenantId { get; set; }
        public int Code { get; set; }
        public List<Item> Items { get; set; } = new();
    }

    [Table("CkM2mItem")]
    public class Item
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class ItemView { public string Name { get; set; } = ""; }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CkM2mGrp (TenantId INTEGER NOT NULL, Code INTEGER NOT NULL, PRIMARY KEY (TenantId, Code));
                CREATE TABLE CkM2mItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CkM2mJoin (TenantId INTEGER NOT NULL, GrpCode INTEGER NOT NULL, ItemId INTEGER NOT NULL);
                INSERT INTO CkM2mGrp VALUES (1,100),(2,100);
                INSERT INTO CkM2mItem VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
                INSERT INTO CkM2mJoin VALUES (1,100,1),(1,100,2),(2,100,3),(2,100,4);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Grp>().HasKey(g => new { g.TenantId, g.Code });
            mb.Entity<Item>().HasKey(i => i.Id);
            mb.Entity<Grp>().HasMany<Item>(g => g.Items).WithMany()
                .UsingTable("CkM2mJoin", new[] { "TenantId", "GrpCode" }, new[] { "ItemId" },
                    g => new { g.TenantId, g.Code }, i => i.Id);
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Bare_m2m_projection_stitches_by_the_full_composite_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        var rows = ctx.Query<Grp>()
            .Select(g => new { g.TenantId, g.Code, Items = g.Items.ToList() })
            .ToList().OrderBy(g => g.TenantId).ToList();

        // Both groups have Code 100; a one-column stitch would merge their items. The full tuple keeps them apart.
        Assert.Equal(new[] { "a", "b" }, rows[0].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
        Assert.Equal(new[] { "c", "d" }, rows[1].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
    }

    [Fact]
    public async Task Element_projected_m2m_stitches_by_the_full_composite_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var rows = (await ((INormQueryable<Grp>)ctx.Query<Grp>())
            .Select(g => new { g.TenantId, g.Code, Items = g.Items.Select(i => new ItemView { Name = i.Name }).ToList() })
            .ToListAsync()).OrderBy(g => g.TenantId).ToList();

        Assert.Equal(new[] { "a", "b" }, rows[0].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
        Assert.Equal(new[] { "c", "d" }, rows[1].Items.OrderBy(i => i.Name).Select(i => i.Name).ToArray());
    }
}
