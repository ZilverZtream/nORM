using System;
using System.Collections.Concurrent;
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
// Gate 4.5→5.0 — Parallel multi-tenant isolation stress tests
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that query result caching and query translation remain correctly
/// tenant-isolated under parallel load across multiple tenants.
///
/// Invariants:
///   TI-1  Each tenant's queries return only that tenant's rows under parallel load.
///   TI-2  Shared NormMemoryCacheProvider embeds tenant ID — no cross-contamination.
///   TI-3  Compiled query cache does not collapse tenant-specific plans.
///   TI-4  Concurrent SaveChanges for different tenants do not corrupt row counts.
/// </summary>
public class TenantIsolationStressTests
{
    [Table("TiRow")]
    private class TiRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int    Id       { get; set; }
        public int    TenantId { get; set; }
        public string Payload  { get; set; } = string.Empty;
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private static string DbName() => $"ti_{Guid.NewGuid():N}";

    private static SqliteConnection OpenShared(string dbName)
    {
        var cn = new SqliteConnection($"Data Source={dbName};Mode=Memory;Cache=Shared");
        cn.Open();
        return cn;
    }

    private static void CreateSchema(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE IF NOT EXISTS TiRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            " TenantId INTEGER NOT NULL, " +
            " Payload TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
    }

    private static void SeedRows(SqliteConnection cn, int tenantId, int count)
    {
        for (int r = 0; r < count; r++)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO TiRow (TenantId, Payload) VALUES (@t, @p)";
            cmd.Parameters.AddWithValue("@t", tenantId);
            cmd.Parameters.AddWithValue("@p", $"tenant{tenantId}_row{r}");
            cmd.ExecuteNonQuery();
        }
    }

    private static DbContext BuildCtx(SqliteConnection cn, int tenantId,
        IDbCacheProvider? cache = null)
    {
        var opts = new DbContextOptions
        {
            TenantProvider   = new FixedTenantProvider(tenantId),
            TenantColumnName = "TenantId"
        };
        if (cache != null) opts.CacheProvider = cache;
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // ── TI-1: Parallel reads — each tenant sees only its own rows ─────────────

    [Fact]
    public async Task ParallelTenantQueries_EachTenantSeesOnlyOwnRows()
    {
        var dbName = DbName();
        using var setupCn = OpenShared(dbName);
        CreateSchema(setupCn);

        const int tenants        = 4;
        const int rowsPerTenant  = 20;
        for (int t = 1; t <= tenants; t++)
            SeedRows(setupCn, t, rowsPerTenant);

        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(1, tenants).SelectMany(tenantId =>
            Enumerable.Range(0, 5).Select(_ => Task.Run(async () =>
            {
                using var cn  = OpenShared(dbName);
                await using var ctx = BuildCtx(cn, tenantId);
                for (int i = 0; i < 10; i++)
                {
                    var rows = await ctx.Query<TiRow>().ToListAsync();
                    foreach (var row in rows)
                        if (row.TenantId != tenantId)
                            errors.Add(
                                $"Tenant {tenantId} received row with TenantId={row.TenantId}");
                }
            }))).ToList();

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    // ── TI-2: Shared cache — no cross-tenant poisoning ────────────────────────

    [Fact]
    public async Task ParallelCachedQueries_SharedCache_NoTenantCrossContamination()
    {
        var dbName = DbName();
        using var setupCn = OpenShared(dbName);
        CreateSchema(setupCn);

        const int tenants = 3;
        for (int t = 1; t <= tenants; t++)
            SeedRows(setupCn, t, 5);

        var sharedCache = new NormMemoryCacheProvider(null);
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(1, tenants).SelectMany(tenantId =>
            Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                using var cn  = OpenShared(dbName);
                await using var ctx = BuildCtx(cn, tenantId, sharedCache);
                for (int i = 0; i < 5; i++)
                {
                    var rows = await ctx.Query<TiRow>().ToListAsync();
                    foreach (var row in rows)
                        if (row.TenantId != tenantId)
                            errors.Add(
                                $"Cache poisoning: tenant {tenantId} got TenantId={row.TenantId}");
                }
            }))).ToList();

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    // ── TI-3: Compiled query plans do not collapse across tenants ─────────────

    [Fact]
    public async Task CompiledQueries_ParallelTenants_NoPlanCollapse()
    {
        var dbName = DbName();
        using var setupCn = OpenShared(dbName);
        CreateSchema(setupCn);

        for (int t = 1; t <= 2; t++)
            SeedRows(setupCn, t, 5);

        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
        {
            int tenantId = (i % 2) + 1;
            using var cn  = OpenShared(dbName);
            await using var ctx = BuildCtx(cn, tenantId);

            var rows = await ctx.Query<TiRow>().ToListAsync();
            foreach (var row in rows)
                if (row.TenantId != tenantId)
                    errors.Add(
                        $"Plan collapse: tenant {tenantId} got TenantId={row.TenantId}");
        })).ToList();

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    // ── TI-4: Concurrent SaveChanges across tenants — correct row counts ───────

    [Fact]
    public async Task ConcurrentSaveChanges_DifferentTenants_NoCorruption()
    {
        var dbName = DbName();
        using var setupCn = OpenShared(dbName);
        CreateSchema(setupCn);

        const int tenants          = 4;
        const int insertsPerTenant = 10;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(1, tenants).Select(tenantId =>
            Task.Run(async () =>
            {
                using var cn  = OpenShared(dbName);
                await using var ctx = BuildCtx(cn, tenantId);
                for (int i = 0; i < insertsPerTenant; i++)
                {
                    try
                    {
                        ctx.Add(new TiRow { TenantId = tenantId, Payload = $"t{tenantId}_{i}" });
                        await ctx.SaveChangesAsync();
                    }
                    catch (Exception ex) { errors.Add(ex.Message); }
                }
            })).ToList();

        await Task.WhenAll(tasks);
        Assert.Empty(errors);

        for (int t = 1; t <= tenants; t++)
        {
            using var cmd = setupCn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM TiRow WHERE TenantId = @t";
            cmd.Parameters.AddWithValue("@t", t);
            var count = Convert.ToInt64(cmd.ExecuteScalar());
            Assert.Equal((long)insertsPerTenant, count);
        }
    }
}
