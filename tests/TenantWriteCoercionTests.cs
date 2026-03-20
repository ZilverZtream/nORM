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

// ══════════════════════════════════════════════════════════════════════════════
// X1 — Tenant type coercion mismatch in ValidateTenantContext
// ══════════════════════════════════════════════════════════════════════════════

[Table("TWC_Item")]
public class TWCItem
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public int TenantId { get; set; }

    public string Name { get; set; } = string.Empty;
}

public class TenantWriteCoercionTests
{
    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly object _id;
        public FixedTenantProvider(object id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private static (SqliteConnection cn, DbContext ctx) Build(object tenantId)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE TWC_Item (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TenantId INTEGER NOT NULL,
                Name TEXT NOT NULL
            );";
        cmd.ExecuteNonQuery();
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId"
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    // ── Test 1: long provider vs int entity column ──────────────────────────

    [Fact]
    public async Task LongProviderVsIntColumn_InsertSucceeds()
    {
        // TenantProvider returns (long)1 but entity column is int TenantId = 1
        var (cn, ctx) = Build((long)1);
        await using var _ = ctx; using var __ = cn;

        // This should NOT throw — coercion should match (long)1 to (int)1
        await ctx.InsertAsync(new TWCItem { TenantId = 1, Name = "coerced-long" });

        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("coerced-long", rows[0].Name);
    }

    // ── Test 2: string provider vs int entity column ────────────────────────

    [Fact]
    public async Task StringProviderVsIntColumn_InsertSucceeds()
    {
        // TenantProvider returns "1" but entity column is int TenantId = 1
        var (cn, ctx) = Build("1");
        await using var _ = ctx; using var __ = cn;

        // This should NOT throw — coercion should match "1" to (int)1
        await ctx.InsertAsync(new TWCItem { TenantId = 1, Name = "coerced-string" });

        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("coerced-string", rows[0].Name);
    }

    // ── Test 3: Mismatched tenant value still throws ────────────────────────

    [Fact]
    public async Task MismatchedTenantValue_StillThrows()
    {
        // TenantProvider returns (long)999 but entity sets TenantId = 1
        var (cn, ctx) = Build((long)999);
        await using var _ = ctx; using var __ = cn;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.InsertAsync(new TWCItem { TenantId = 1, Name = "wrong-tenant" }));
        Assert.Contains("Tenant context mismatch", ex.Message);
    }

    // ── Helpers for seeded tests ─────────────────────────────────────────────

    /// <summary>
    /// Builds a context AND seeds a row (Id=1, TenantId=1, Name="seed") via raw SQL
    /// so that update/delete tests have something to operate on.
    /// </summary>
    private static (SqliteConnection cn, DbContext ctx) BuildWithSeed(object tenantId)
    {
        var (cn, ctx) = Build(tenantId);
        using var seed = cn.CreateCommand();
        seed.CommandText = "INSERT INTO TWC_Item (TenantId, Name) VALUES (1, 'seed')";
        seed.ExecuteNonQuery();
        return (cn, ctx);
    }

    // ── Test 4: UpdateAsync (single-entity) with coerced tenant ──────────────

    [Fact]
    public async Task LongProviderVsIntColumn_UpdateAsync_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        var (cn, ctx) = BuildWithSeed((long)1);
        await using var _ = ctx; using var __ = cn;

        await ctx.UpdateAsync(new TWCItem { Id = 1, TenantId = 1, Name = "updated" });

        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("updated", rows[0].Name);
    }

    // ── Test 5: DeleteAsync (single-entity) with coerced tenant ──────────────

    [Fact]
    public async Task LongProviderVsIntColumn_DeleteAsync_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        var (cn, ctx) = BuildWithSeed((long)1);
        await using var _ = ctx; using var __ = cn;

        await ctx.DeleteAsync(new TWCItem { Id = 1, TenantId = 1, Name = "seed" });

        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Empty(rows);
    }

    // ── Test 6: SaveChangesAsync insert path with coerced tenant ─────────────

    [Fact]
    public async Task LongProviderVsIntColumn_SaveChangesAsync_Insert_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        var (cn, ctx) = Build((long)1);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new TWCItem { TenantId = 1, Name = "saved-insert" });
        await ctx.SaveChangesAsync();

        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("saved-insert", rows[0].Name);
    }

    // ── Test 7: SaveChangesAsync update path with coerced tenant ─────────────

    [Fact]
    public async Task LongProviderVsIntColumn_SaveChangesAsync_Update_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        // Seed a row, query it, modify, and save
        var (cn, ctx) = BuildWithSeed((long)1);
        await using var _ = ctx; using var __ = cn;

        var entity = (await ctx.Query<TWCItem>().ToListAsync()).Single();
        entity.Name = "saved-update";
        await ctx.SaveChangesAsync();

        // Re-query to verify
        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("saved-update", rows[0].Name);
    }

    // ── Test 8: SaveChangesAsync delete path with coerced tenant ─────────────

    [Fact]
    public async Task LongProviderVsIntColumn_SaveChangesAsync_Delete_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        // Seed a row, attach it as Unchanged, mark for deletion, and save
        var (cn, ctx) = BuildWithSeed((long)1);
        await using var _ = ctx; using var __ = cn;

        var entity = new TWCItem { Id = 1, TenantId = 1, Name = "seed" };
        ctx.Attach(entity);
        ctx.Remove(entity);
        await ctx.SaveChangesAsync();

        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Empty(rows);
    }

    // ── Test 9: BulkUpdateAsync with coerced tenant ──────────────────────────

    [Fact]
    public async Task LongProviderVsIntColumn_BulkUpdateAsync_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        var (cn, ctx) = BuildWithSeed((long)1);
        await using var _ = ctx; using var __ = cn;

        var result = await ctx.BulkUpdateAsync(new[]
        {
            new TWCItem { Id = 1, TenantId = 1, Name = "bulk-updated" }
        });

        Assert.Equal(1, result);
        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("bulk-updated", rows[0].Name);
    }

    // ── Test 10: BulkDeleteAsync with coerced tenant ─────────────────────────

    [Fact]
    public async Task LongProviderVsIntColumn_BulkDeleteAsync_Succeeds()
    {
        // TenantProvider returns (long)1, entity has int TenantId = 1
        var (cn, ctx) = BuildWithSeed((long)1);
        await using var _ = ctx; using var __ = cn;

        var result = await ctx.BulkDeleteAsync(new[]
        {
            new TWCItem { Id = 1, TenantId = 1, Name = "seed" }
        });

        Assert.Equal(1, result);
        var rows = await ctx.Query<TWCItem>().ToListAsync();
        Assert.Empty(rows);
    }

    // ── Test 11: Mismatched tenant still throws on SaveChangesAsync update ───

    [Fact]
    public async Task MismatchedTenant_SaveChangesAsync_Update_Throws()
    {
        // TenantProvider returns (long)2, but entity has TenantId = 1
        // The attach should work (entity is Unchanged), but modifying and saving
        // should throw because the entity's tenant doesn't match the provider's tenant.
        var (cn, ctx) = BuildWithSeed((long)2);
        await using var _ = ctx; using var __ = cn;

        var entity = new TWCItem { Id = 1, TenantId = 1, Name = "seed" };
        // Use UpdateAsync which calls ValidateTenantContext directly
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.UpdateAsync(new TWCItem { Id = 1, TenantId = 1, Name = "tampered" }));
        Assert.Contains("Tenant context mismatch", ex.Message);
    }
}
