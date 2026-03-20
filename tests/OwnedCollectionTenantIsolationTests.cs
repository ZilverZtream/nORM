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
/// Tests for X1: Owned-collection save/load paths must respect tenant boundaries.
/// Without the fix, DELETE and SELECT on owned child tables lack a tenant predicate,
/// allowing cross-tenant data destruction and leakage.
/// </summary>
public class OwnedCollectionTenantIsolationTests
{
    // ── Entity definitions ────────────────────────────────────────────────

    [Table("OctOwner")]
    private class OctOwner
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";
        public List<OctChild> Children { get; set; } = new();
    }

    [Table("OctChild")]
    private class OctChild
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Value { get; set; } = "";
        public string TenantId { get; set; } = "";
    }

    // ── Tenant provider ──────────────────────────────────────────────────

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private const string Ddl = @"
        CREATE TABLE OctOwner (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);
        CREATE TABLE OctChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, Value TEXT NOT NULL, TenantId TEXT NOT NULL);
    ";

    private static SqliteConnection CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = Ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb => mb.Entity<OctOwner>()
                .OwnsMany<OctChild>(o => o.Children, tableName: "OctChild", foreignKey: "OwnerId")
        });

    private static void SeedTwoTenants(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        // Both tenants have an owner with Id=1 (different names to distinguish)
        // and each has children with the same FK (OwnerId=1).
        cmd.CommandText = @"
            INSERT INTO OctOwner (Id, TenantId, Name) VALUES (1, 'A', 'OwnerA');
            INSERT INTO OctOwner (Id, TenantId, Name) VALUES (2, 'B', 'OwnerB');
            INSERT INTO OctChild (OwnerId, Value, TenantId) VALUES (1, 'ChildA1', 'A');
            INSERT INTO OctChild (OwnerId, Value, TenantId) VALUES (1, 'ChildA2', 'A');
            INSERT INTO OctChild (OwnerId, Value, TenantId) VALUES (1, 'ChildB1', 'B');
            INSERT INTO OctChild (OwnerId, Value, TenantId) VALUES (2, 'ChildB2', 'B');
        ";
        cmd.ExecuteNonQuery();
    }

    private static long CountChildren(SqliteConnection cn, string? tenantFilter = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = tenantFilter == null
            ? "SELECT COUNT(*) FROM OctChild"
            : $"SELECT COUNT(*) FROM OctChild WHERE TenantId = '{tenantFilter}'";
        return (long)cmd.ExecuteScalar()!;
    }

    // ── Tests ────────────────────────────────────────────────────────────

    [Fact]
    public void X1_Diagnostic_OwnedColumnsMappedCorrectly()
    {
        using var cn = CreateDb();
        using var ctx = CreateCtx(cn, "A");
        var map = ctx.GetMapping(typeof(OctOwner));
        Assert.Single(map.OwnedCollections);
        var oc = map.OwnedCollections[0];
        // OctChild has Id, Value, TenantId properties
        Assert.Contains(oc.Columns, c => c.PropName == "TenantId");
        Assert.NotNull(map.TenantColumn);
        Assert.Equal("TenantId", map.TenantColumn!.PropName);
    }

    [Fact]
    public async Task X1_Diagnostic_LoadWithoutTenant_WorksBaseline()
    {
        // Verify owned collection loading works at all (no tenant filter)
        using var cn = CreateDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO OctOwner (Id, TenantId, Name) VALUES (1, 'A', 'OwnerA');
            INSERT INTO OctChild (OwnerId, Value, TenantId) VALUES (1, 'ChildA1', 'A'), (1, 'ChildA2', 'A');
        ";
        cmd.ExecuteNonQuery();

        // No tenant provider — basic owned collection loading
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<OctOwner>()
                .OwnsMany<OctChild>(o => o.Children, tableName: "OctChild", foreignKey: "OwnerId")
        });
        var owners = await ctx.Query<OctOwner>().ToListAsync();
        Assert.Single(owners);
        Assert.Equal(2, owners[0].Children.Count);
    }

    [Fact]
    public async Task X1_Diagnostic_LoadWithTenant_SimpleCase()
    {
        // Single tenant, just verify loading works with tenant configured
        using var cn = CreateDb();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO OctOwner (Id, TenantId, Name) VALUES (1, 'A', 'OwnerA');
            INSERT INTO OctChild (OwnerId, Value, TenantId) VALUES (1, 'ChildA1', 'A'), (1, 'ChildA2', 'A');
        ";
        cmd.ExecuteNonQuery();

        // Verify the raw SQL works
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM OctChild WHERE OwnerId = 1 AND TenantId = 'A'";
        var rawCount = (long)check.ExecuteScalar()!;
        Assert.Equal(2, rawCount);

        using var ctx = CreateCtx(cn, "A");

        // Check the owned mapping
        var map = ctx.GetMapping(typeof(OctOwner));
        var oc = map.OwnedCollections[0];
        var tenantCol = oc.Columns.FirstOrDefault(c => c.PropName == "TenantId");
        Assert.NotNull(tenantCol);

        // Manually call LoadOwnedCollectionsAsync with a known owner
        var ownerObj = new OctOwner { Id = 1, Name = "OwnerA", TenantId = "A" };
        var ownerList = new System.Collections.ArrayList { ownerObj };
        await ctx.LoadOwnedCollectionsAsync(ownerList, map, default);
        Assert.Equal(2, ownerObj.Children.Count);
    }

    [Fact]
    public async Task X1_Load_OwnedCollection_OnlyReturnsCurrentTenantChildren()
    {
        // Arrange: two tenants, owner Id=1 in both, each with children FK=1
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Act: query as tenant A
        using var ctx = CreateCtx(cn, "A");

        // Verify mapping + tracking preconditions
        var map = ctx.GetMapping(typeof(OctOwner));
        Assert.True(map.OwnedCollections.Count > 0, "OwnedCollections should be populated");
        Assert.True(ctx.IsMapped(typeof(OctOwner)), "OctOwner should be mapped");

        var owners = await ctx.Query<OctOwner>().ToListAsync();
        Assert.Single(owners);
        var ownerA = owners[0];
        Assert.Equal("OwnerA", ownerA.Name);

        // Explicitly load owned collections to test the tenant-scoped load path
        // (query pipeline may not auto-load owned collections in all configurations)
        await ctx.LoadOwnedCollectionsAsync(
            new System.Collections.ArrayList(owners), map, default);

        Assert.Equal(2, ownerA.Children.Count);
        Assert.All(ownerA.Children, c => Assert.Equal("A", c.TenantId));
        Assert.Contains(ownerA.Children, c => c.Value == "ChildA1");
        Assert.Contains(ownerA.Children, c => c.Value == "ChildA2");
    }

    [Fact]
    public async Task X1_Load_OwnedCollection_TenantB_SeesOnlyOwnChildren()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        using var ctx = CreateCtx(cn, "B");
        var map = ctx.GetMapping(typeof(OctOwner));
        var owners = await ctx.Query<OctOwner>().ToListAsync();

        Assert.Single(owners);
        var ownerB = owners[0];
        Assert.Equal("OwnerB", ownerB.Name);

        await ctx.LoadOwnedCollectionsAsync(
            new System.Collections.ArrayList(owners), map, default);
        Assert.Single(ownerB.Children);
        Assert.Equal("ChildB2", ownerB.Children[0].Value);
    }

    [Fact]
    public async Task X1_Delete_OwnedCollection_DoesNotDestroyCrossTenantChildren()
    {
        // Arrange: seed both tenants with children for owner FK=1
        using var cn = CreateDb();
        SeedTwoTenants(cn);
        Assert.Equal(4, CountChildren(cn));

        // Act: update owner in tenant A (triggers DELETE+INSERT cycle for owned children)
        using var ctxA = CreateCtx(cn, "A");
        var ownersA = await ctxA.Query<OctOwner>().ToListAsync();
        Assert.Single(ownersA);
        var ownerA = ownersA[0];

        // Clear all children and save (Modified state triggers DELETE of owned items)
        ownerA.Children = new List<OctChild>();
        ctxA.Update(ownerA);
        await ctxA.SaveChangesAsync();

        // Assert: tenant A's children are gone, tenant B's children are untouched
        Assert.Equal(0, CountChildren(cn, "A"));
        Assert.Equal(2, CountChildren(cn, "B"));
    }

    [Fact]
    public async Task X1_Delete_Owner_DoesNotDestroyCrossTenantChildren()
    {
        // Arrange
        using var cn = CreateDb();
        SeedTwoTenants(cn);
        Assert.Equal(4, CountChildren(cn));

        // Act: delete owner in tenant A
        using var ctxA = CreateCtx(cn, "A");
        var ownersA = await ctxA.Query<OctOwner>().ToListAsync();
        Assert.Single(ownersA);
        ctxA.Remove(ownersA[0]);
        await ctxA.SaveChangesAsync();

        // Assert: tenant B's children with same FK are intact
        Assert.Equal(0, CountChildren(cn, "A"));
        Assert.Equal(2, CountChildren(cn, "B"));
    }

    [Fact]
    public async Task X1_Save_OwnedCollection_InsertAsTenantA_DoesNotAffectTenantB()
    {
        using var cn = CreateDb();
        SeedTwoTenants(cn);
        var initialB = CountChildren(cn, "B");

        // Act: insert new owner with children as tenant A
        using var ctxA = CreateCtx(cn, "A");
        var newOwner = new OctOwner
        {
            Id = 99,
            TenantId = "A",
            Name = "NewA",
            Children = new List<OctChild>
            {
                new OctChild { Value = "NewChild1", TenantId = "A" },
                new OctChild { Value = "NewChild2", TenantId = "A" }
            }
        };
        ctxA.Add(newOwner);
        await ctxA.SaveChangesAsync();

        // Assert: tenant B children unchanged
        Assert.Equal(initialB, CountChildren(cn, "B"));
        // And tenant A now has 2 more children
        Assert.Equal(4, CountChildren(cn, "A"));
    }

    [Fact]
    public async Task X1_Update_OwnedCollection_ReplacesOnlyCurrentTenantChildren()
    {
        // Arrange: seed both tenants
        using var cn = CreateDb();
        SeedTwoTenants(cn);

        // Act: update owner A's children (DELETE+INSERT cycle)
        using var ctxA = CreateCtx(cn, "A");
        var ownersA = await ctxA.Query<OctOwner>().ToListAsync();
        var ownerA = ownersA[0];

        // Replace children with new ones
        ownerA.Children = new List<OctChild>
        {
            new OctChild { Value = "ReplacedA1", TenantId = "A" }
        };
        ctxA.Update(ownerA);
        await ctxA.SaveChangesAsync();

        // Assert: tenant A has 1 child now (the replacement)
        Assert.Equal(1, CountChildren(cn, "A"));
        // Tenant B still has its 2 children
        Assert.Equal(2, CountChildren(cn, "B"));

        // Verify the remaining A child is the replacement
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Value FROM OctChild WHERE TenantId = 'A'";
        var val = (string)cmd.ExecuteScalar()!;
        Assert.Equal("ReplacedA1", val);
    }
}
