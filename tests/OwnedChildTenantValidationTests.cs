using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// SAVE1: Owned-collection inserts must validate the child's tenant ID before INSERT.
// Previously, insert path was not tenant-aware; null or mismatched child tenants were silently persisted.

public class OwnedChildTenantValidationTests
{
    [Table("OctvOwner")]
    private class OctvOwner
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";
        public List<OctvChild> Children { get; set; } = new();
    }

    [Table("OctvChild")]
    private class OctvChild
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Value { get; set; } = "";
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenant : ITenantProvider
    {
        public FixedTenant(string id) => Id = id;
        public string Id { get; }
        public object GetCurrentTenantId() => Id;
    }

    private const string Ddl =
        "CREATE TABLE OctvOwner (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);" +
        "CREATE TABLE OctvChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, Value TEXT NOT NULL, TenantId TEXT NOT NULL);";

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = Ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext MakeCtx(SqliteConnection cn, string tenantId) =>
        new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenant(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb => mb.Entity<OctvOwner>()
                .OwnsMany<OctvChild>(o => o.Children, tableName: "OctvChild", foreignKey: "OwnerId")
        });

    // ── SAVE1-1: Correct tenant passes through ────────────────────────────────

    [Fact]
    public async Task CorrectChildTenant_SavesSuccessfully()
    {
        using var cn = OpenDb();
        await using var ctx = MakeCtx(cn, "T1");

        var owner = new OctvOwner
        {
            Id = 1, TenantId = "T1", Name = "owner",
            Children = new List<OctvChild>
            {
                new() { Value = "child1", TenantId = "T1" }
            }
        };
        ctx.Add(owner);
        await ctx.SaveChangesAsync();

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM OctvChild WHERE TenantId = 'T1'";
        Assert.Equal(1L, verify.ExecuteScalar());
    }

    // ── SAVE1-2: Null child tenant throws ─────────────────────────────────────

    [Fact]
    public async Task NullChildTenant_ThrowsInvalidOperationException()
    {
        using var cn = OpenDb();
        await using var ctx = MakeCtx(cn, "T1");

        var owner = new OctvOwner
        {
            Id = 1, TenantId = "T1", Name = "owner",
            Children = new List<OctvChild>
            {
                new() { Value = "child", TenantId = "" } // empty string maps to null-like for DB
            }
        };
        // Override with genuinely null TenantId
        owner.Children[0].TenantId = null!;

        ctx.Add(owner);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── SAVE1-3: Mismatched child tenant throws ───────────────────────────────

    [Fact]
    public async Task MismatchedChildTenant_ThrowsInvalidOperationException()
    {
        using var cn = OpenDb();
        await using var ctx = MakeCtx(cn, "T1");

        var owner = new OctvOwner
        {
            Id = 1, TenantId = "T1", Name = "owner",
            Children = new List<OctvChild>
            {
                new() { Value = "child", TenantId = "T2" } // wrong tenant
            }
        };
        ctx.Add(owner);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("T2", ex.Message);
        Assert.Contains("T1", ex.Message);
    }

    // ── SAVE1-4: No tenant config → no validation (backward compat) ──────────

    [Fact]
    public async Task NoTenantConfig_ChildSavesWithoutValidation()
    {
        using var cn = OpenDb();
        // Context without TenantProvider — tenant validation must be skipped entirely
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<OctvOwner>()
                .OwnsMany<OctvChild>(o => o.Children, tableName: "OctvChild", foreignKey: "OwnerId")
        });

        var owner = new OctvOwner
        {
            Id = 1, TenantId = "T1", Name = "owner",
            Children = new List<OctvChild>
            {
                new() { Value = "child", TenantId = "T2" } // different tenant — no validation
            }
        };
        ctx.Add(owner);
        await ctx.SaveChangesAsync(); // must not throw
    }

    // ── SAVE1-5: Update path revalidates children ─────────────────────────────

    [Fact]
    public async Task Update_MismatchedChildTenant_ThrowsInvalidOperationException()
    {
        using var cn = OpenDb();
        await using var ctx = MakeCtx(cn, "T1");

        // Insert a valid owner+child
        var owner = new OctvOwner
        {
            Id = 2, TenantId = "T1", Name = "owner2",
            Children = new List<OctvChild> { new() { Value = "v", TenantId = "T1" } }
        };
        ctx.Add(owner);
        await ctx.SaveChangesAsync();

        // Now attach with a mismatched child tenant on update
        ctx.Attach(owner);
        owner.Name = "owner2_updated";
        owner.Children[0].TenantId = "T9"; // bad tenant on update

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("T9", ex.Message);
    }
}
