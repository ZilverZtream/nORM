using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial tenant-isolation contract for shaping an owned (OwnsMany) or many-to-many collection into a
/// projection: a row belonging to another tenant but forged to point at the current tenant's owner/post must
/// NOT leak into the projected collection. The owned loader scopes by the owned table's tenant column; the
/// m2m loader scopes both the bridge (via the left entity) and the related entity. Mirrors the relation path's
/// <c>child_load_does_not_leak_across_tenants</c> contract for the owned/m2m split-query loaders.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedOwnedM2mProjectionTenantIsolationTests
{
    private sealed class FixedTenant(int t) : ITenantProvider { public object GetCurrentTenantId() => t; }

    [Table("TiOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    // Owned line carries a tenant column so the owned load can fail closed and scope by tenant.
    public class Line { [Key] public int Id { get; set; } public int TenantId { get; set; } public string Sku { get; set; } = ""; }

    [Table("TiPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("TiTag")]
    public class Tag { [Key] public int Id { get; set; } public int TenantId { get; set; } public string Label { get; set; } = ""; }

    [Fact]
    public void Owned_shaped_projection_does_not_leak_across_tenants()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TiOrder (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL);
                CREATE TABLE TiLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, TenantId INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO TiOrder VALUES (1,1),(2,2);
                -- Order 1 (tenant 1) has a tenant-1 line AND a forged tenant-2 line pointing at it.
                INSERT INTO TiLine VALUES (1,1,1,'mine'),(2,1,2,'FORGED');
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext CtxFor(int t) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenant(t),
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "TiLine", foreignKey: "OrderId");
            }
        }, ownsConnection: false);

        using var ctx = CtxFor(1);
        var rows = ctx.Query<Order>().Select(o => new { o.Id, Lines = o.Lines.ToList() }).ToList();
        Assert.Single(rows);                                            // tenant 1 sees order 1 only
        Assert.Equal(new[] { "mine" }, rows[0].Lines.Select(l => l.Sku)); // the forged tenant-2 line is excluded
    }

    [Fact]
    public void ManyToMany_shaped_projection_does_not_leak_across_tenants()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TiPost (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL);
                CREATE TABLE TiTag (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Label TEXT NOT NULL);
                CREATE TABLE TiPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO TiPost VALUES (1,1),(2,2);
                INSERT INTO TiTag VALUES (1,1,'mine'),(2,2,'FORGED');
                -- Post 1 (tenant 1) is linked to its own tag AND to a forged tenant-2 tag.
                INSERT INTO TiPostTag VALUES (1,1),(1,2);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext CtxFor(int t) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenant(t),
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("TiPostTag", "PostId", "TagId");
            }
        }, ownsConnection: false);

        using var ctx = CtxFor(1);
        var rows = ctx.Query<Post>().Select(p => new { p.Id, Tags = p.Tags.ToList() }).ToList();
        Assert.Single(rows);                                              // tenant 1 sees post 1 only
        Assert.Equal(new[] { "mine" }, rows[0].Tags.Select(t => t.Label)); // the forged tenant-2 tag is excluded
    }
}
