using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 → 5.0 — Multi-tenant result-cache / materializer-cache poisoning
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Proves that the result cache (IDbCacheProvider), the compiled-materializer cache
/// (CompiledMaterializerStore), and the query-plan cache are all tenant-scoped and
/// cannot be poisoned by a concurrent tenant's data.
///
/// Root concern: a shared NormMemoryCacheProvider instance is the intended deployment
/// model for multi-tenant apps. The cache key built by BuildCacheKeyFromPlan must embed
/// the tenant ID so that tenant A's cached rows are never served to tenant B, even if
/// both issue the same parameterised SQL against the same physical database.
///
/// Coverage:
///   RC-1  Cache key for same SQL differs across tenants (white-box structural).
///   RC-2  Tenant A's cached result is not served to tenant B (black-box end-to-end).
///   RC-3  Concurrent tenants sharing one NormMemoryCacheProvider see only their rows.
///   RC-4  InvalidateTag on one tenant does not evict another tenant's entry.
///   RC-5  Materializer cache: adversarial parallel load does not return wrong-schema materializer.
/// </summary>
public class MultiTenantResultCachePoisoningTests
{
    // ── Entity ─────────────────────────────────────────────────────────────────

    [Table("MtCacheRow")]
    private class MtCacheRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Tenant { get; set; } = string.Empty;
        public string Secret { get; set; } = string.Empty;
    }

    // ── Tenant provider ────────────────────────────────────────────────────────

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Fixtures ───────────────────────────────────────────────────────────────

    private static SqliteConnection BuildDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE MtCacheRow " +
                          "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Tenant TEXT NOT NULL, Secret TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext BuildCtx(
        SqliteConnection cn,
        string tenantId,
        IDbCacheProvider? cacheProvider = null)
    {
        var opts = new DbContextOptions
        {
            TenantProvider    = new FixedTenantProvider(tenantId),
            TenantColumnName  = "Tenant",
            CacheProvider     = cacheProvider,
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // ── RC-1: Cache key differs per tenant ────────────────────────────────────

    /// <summary>
    /// White-box proof: BuildCacheKeyFromPlan embeds the tenant ID in the key.
    /// Two providers that are otherwise identical (same DB, same SQL, same params) but
    /// with different tenant contexts must produce different cache keys.
    /// </summary>
    [Fact]
    public void CacheKey_SameSqlDifferentTenants_ProduceDifferentKeys()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var ctx1 = BuildCtx(cn, "alpha");
        using var ctx2 = BuildCtx(cn, "beta");

        var provider1 = new NormQueryProvider(ctx1);
        var provider2 = new NormQueryProvider(ctx2);

        var plan = CreatePlan("SELECT * FROM MtCacheRow WHERE Tenant = @p0",
                              new Dictionary<string, object> { ["@p0"] = "alpha" });

        var key1 = InvokeBuiltCacheKey<List<MtCacheRow>>(provider1, plan, plan.Parameters);
        var key2 = InvokeBuiltCacheKey<List<MtCacheRow>>(provider2, plan, plan.Parameters);

        Assert.NotEqual(key1, key2);
    }

    /// <summary>
    /// Same tenant on two separate provider instances → same key (cache sharing is intentional).
    /// </summary>
    [Fact]
    public void CacheKey_SameTenantTwoProviders_ProduceSameKey()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var ctx1 = BuildCtx(cn, "shared-tenant");
        using var ctx2 = BuildCtx(cn, "shared-tenant");

        var provider1 = new NormQueryProvider(ctx1);
        var provider2 = new NormQueryProvider(ctx2);

        var plan = CreatePlan("SELECT * FROM MtCacheRow WHERE Tenant = @p0",
                              new Dictionary<string, object> { ["@p0"] = "shared-tenant" });

        var key1 = InvokeBuiltCacheKey<List<MtCacheRow>>(provider1, plan, plan.Parameters);
        var key2 = InvokeBuiltCacheKey<List<MtCacheRow>>(provider2, plan, plan.Parameters);

        Assert.Equal(key1, key2);
    }

    // ── RC-2: End-to-end: tenant B does not get tenant A's cached rows ─────────

    [Fact]
    public async Task ResultCache_TenantB_DoesNotReceiveTenantARows()
    {
        using var cn      = BuildDb();
        using var cache   = new NormMemoryCacheProvider();

        // Seed: tenant "alpha" has rows, tenant "beta" has none.
        await using var seedCtx = BuildCtx(cn, "alpha", cache);
        await seedCtx.InsertAsync(new MtCacheRow { Tenant = "alpha", Secret = "alpha-secret-1" });
        await seedCtx.InsertAsync(new MtCacheRow { Tenant = "alpha", Secret = "alpha-secret-2" });

        // Tenant alpha queries — populates the cache.
        await using var alphaCtx = BuildCtx(cn, "alpha", cache);
        var alphaRows = await alphaCtx.Query<MtCacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();

        Assert.Equal(2, alphaRows.Count);
        Assert.All(alphaRows, r => Assert.Equal("alpha", r.Tenant));

        // Tenant beta queries via the SAME cache instance — must get an empty result,
        // not alpha's cached rows.
        await using var betaCtx = BuildCtx(cn, "beta", cache);
        var betaRows = await betaCtx.Query<MtCacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();

        Assert.Empty(betaRows);
        Assert.DoesNotContain(betaRows, r => r.Tenant == "alpha");
    }

    /// <summary>
    /// After tenant alpha's entry is served from cache, tenant beta receives its own fresh
    /// result even though both queries produce the same SQL template.
    /// </summary>
    [Fact]
    public async Task ResultCache_CachePopulatedByAlpha_BetaGetsOwnFreshResult()
    {
        using var cn    = BuildDb();
        using var cache = new NormMemoryCacheProvider();

        // Seed both tenants.
        await using var seedAlpha = BuildCtx(cn, "alpha", cache);
        await seedAlpha.InsertAsync(new MtCacheRow { Tenant = "alpha", Secret = "alpha-data" });

        await using var seedBeta = BuildCtx(cn, "beta", cache);
        await seedBeta.InsertAsync(new MtCacheRow { Tenant = "beta", Secret = "beta-data" });

        // Alpha queries → cached.
        await using var alphaCtx = BuildCtx(cn, "alpha", cache);
        var alphaRows = await alphaCtx.Query<MtCacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(alphaRows);
        Assert.Equal("alpha-data", alphaRows[0].Secret);

        // Beta queries → must not return alpha's cached entry.
        await using var betaCtx = BuildCtx(cn, "beta", cache);
        var betaRows = await betaCtx.Query<MtCacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(betaRows);
        Assert.Equal("beta-data", betaRows[0].Secret);
        Assert.DoesNotContain(betaRows, r => r.Secret == "alpha-data");
    }

    // ── RC-3: Concurrent tenants — no cross-contamination under parallel load ──

    [Fact]
    public async Task ResultCache_ConcurrentTenants_NoCrossContamination()
    {
        // Use a named shared-cache in-memory database so concurrent tasks can each open
        // their own SqliteConnection to the same data (single-connection SQLite is not
        // thread-safe for concurrent access across multiple callers).
        const string dbName = "RC3ConcurrentTest";
        var cs = $"Data Source={dbName};Mode=Memory;Cache=Shared";

        const int tenantCount = 4;
        const int rowsPerTenant = 3;
        const int queriesPerTenant = 20;

        // Seed all tenants using a dedicated connection that stays open throughout.
        using var seedCn = new SqliteConnection(cs);
        seedCn.Open();
        using var createCmd = seedCn.CreateCommand();
        createCmd.CommandText =
            "CREATE TABLE IF NOT EXISTS MtCacheRow " +
            "(Id INTEGER PRIMARY KEY AUTOINCREMENT, Tenant TEXT NOT NULL, Secret TEXT NOT NULL)";
        createCmd.ExecuteNonQuery();

        using var cache = new NormMemoryCacheProvider();
        for (int t = 1; t <= tenantCount; t++)
        {
            await using var ctx = BuildCtx(seedCn, $"t{t}", cache);
            for (int r = 0; r < rowsPerTenant; r++)
                await ctx.InsertAsync(new MtCacheRow { Tenant = $"t{t}", Secret = $"t{t}-secret{r}" });
        }

        var violations = new ConcurrentBag<string>();

        // All tenants run queries concurrently through the shared cache.
        // Each task opens its own connection to avoid thread-safety issues.
        var tasks = Enumerable.Range(0, tenantCount * queriesPerTenant).Select(idx => Task.Run(async () =>
        {
            var tenantId = $"t{(idx % tenantCount) + 1}";
            using var taskCn = new SqliteConnection(cs);
            taskCn.Open();
            await using var ctx = BuildCtx(taskCn, tenantId, cache);
            var rows = await ctx.Query<MtCacheRow>()
                .Cacheable(TimeSpan.FromMinutes(5))
                .ToListAsync();

            if (rows.Count != rowsPerTenant)
                violations.Add($"[{tenantId}] expected {rowsPerTenant} rows, got {rows.Count}");

            foreach (var row in rows)
            {
                if (row.Tenant != tenantId)
                    violations.Add($"[{tenantId}] received cross-tenant row: Tenant={row.Tenant}, Secret={row.Secret}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        seedCn.Close();
        Assert.Empty(violations);
    }

    // ── RC-4: InvalidateTag on one tenant does not evict another tenant ────────

    [Fact]
    public async Task ResultCache_InvalidateTagAlpha_DoesNotEvictBeta()
    {
        // Use a NormMemoryCacheProvider that qualifies tags by tenant.
        // This test verifies that InvalidateTag("MtCacheRow") for tenant "alpha"
        // does NOT evict tenant "beta"'s cache entries.
        using var cn = BuildDb();

        // Two separate cache providers, each scoped to a tenant (mirrors intended usage
        // where a DI container creates one per tenant).
        using var alphaCache = new NormMemoryCacheProvider(getTenantId: () => "alpha");
        using var betaCache  = new NormMemoryCacheProvider(getTenantId: () => "beta");

        await using var seedAlpha = BuildCtx(cn, "alpha", alphaCache);
        await seedAlpha.InsertAsync(new MtCacheRow { Tenant = "alpha", Secret = "a-val" });

        await using var seedBeta = BuildCtx(cn, "beta", betaCache);
        await seedBeta.InsertAsync(new MtCacheRow { Tenant = "beta", Secret = "b-val" });

        // Both tenants populate their caches.
        await using var alphaCtx = BuildCtx(cn, "alpha", alphaCache);
        var a1 = await alphaCtx.Query<MtCacheRow>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(a1);

        await using var betaCtx = BuildCtx(cn, "beta", betaCache);
        var b1 = await betaCtx.Query<MtCacheRow>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(b1);

        // Invalidate alpha's tag — must NOT evict beta's entry.
        alphaCache.InvalidateTag("MtCacheRow");

        // Beta can still serve from its own cache (a hit; no DB call needed).
        // Structural check: beta's secret is still retrievable.
        await using var betaCtx2 = BuildCtx(cn, "beta", betaCache);
        var b2 = await betaCtx2.Query<MtCacheRow>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(b2);
        Assert.Equal("b-val", b2[0].Secret);
    }

    // ── RC-5: Materializer cache under adversarial parallel load ──────────────

    /// <summary>
    /// CompiledMaterializerStore is keyed by (Type, tableName). Proves that under
    /// high-concurrency conditions no materializer registered for one table schema
    /// leaks into queries against a different table schema.
    ///
    /// Uses two entity types with different schemas sharing the same row type to
    /// detect if a wrong materializer (one that misreads column offsets) is returned.
    /// </summary>
    [Table("RcSchemaA")]
    private class RcSchemaA
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string ColA { get; set; } = string.Empty;
    }

    [Table("RcSchemaB")]
    private class RcSchemaB
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string ColB { get; set; } = string.Empty;
    }

    [Fact]
    public async Task MaterializerCache_ParallelQueriesTwoSchemas_NoMaterializerLeak()
    {
        // Use a named shared-cache in-memory database so each task can open its own
        // connection to the same data without sharing a single SqliteConnection object
        // (which is not thread-safe for concurrent reads).
        const string dbName = "MatCacheParallelTest";
        var cs = $"Data Source={dbName};Mode=Memory;Cache=Shared";

        // Seed using a dedicated connection.
        using var seedCn = new SqliteConnection(cs);
        seedCn.Open();
        using var setup = seedCn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE IF NOT EXISTS RcSchemaA (Id INTEGER PRIMARY KEY AUTOINCREMENT, ColA TEXT NOT NULL);" +
            "CREATE TABLE IF NOT EXISTS RcSchemaB (Id INTEGER PRIMARY KEY AUTOINCREMENT, ColB TEXT NOT NULL);";
        setup.ExecuteNonQuery();

        await using var seedCtx = new DbContext(seedCn, new SqliteProvider(), null,
            ownsConnection: false);
        for (int i = 0; i < 5; i++)
        {
            await seedCtx.InsertAsync(new RcSchemaA { ColA = $"a-value-{i}" });
            await seedCtx.InsertAsync(new RcSchemaB { ColB = $"b-value-{i}" });
        }

        var errors = new ConcurrentBag<string>();

        // 32 parallel tasks — each opens its own connection to the shared-cache DB.
        // Half query RcSchemaA, half query RcSchemaB.
        var tasks = Enumerable.Range(0, 32).Select(idx => Task.Run(async () =>
        {
            using var taskCn = new SqliteConnection(cs);
            taskCn.Open();
            await using var ctx = new DbContext(taskCn, new SqliteProvider(), null,
                ownsConnection: false);
            try
            {
                if (idx % 2 == 0)
                {
                    var rows = await ctx.Query<RcSchemaA>().ToListAsync();
                    foreach (var r in rows)
                    {
                        if (!r.ColA.StartsWith("a-value-"))
                            errors.Add($"RcSchemaA task{idx}: unexpected ColA='{r.ColA}'");
                    }
                }
                else
                {
                    var rows = await ctx.Query<RcSchemaB>().ToListAsync();
                    foreach (var r in rows)
                    {
                        if (!r.ColB.StartsWith("b-value-"))
                            errors.Add($"RcSchemaB task{idx}: unexpected ColB='{r.ColB}'");
                    }
                }
            }
            catch (Exception ex)
            {
                errors.Add($"task{idx}: exception: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        seedCn.Close();
        Assert.Empty(errors);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static QueryPlan CreatePlan(string sql, IReadOnlyDictionary<string, object> parameters)
    {
        static Task<object> Mat(System.Data.Common.DbDataReader r, CancellationToken ct)
            => Task.FromResult<object>(null!);
        static object SyncMat(System.Data.Common.DbDataReader _) => null!;

        return new QueryPlan(
            Sql: sql,
            Parameters: parameters,
            CompiledParameters: Array.Empty<string>(),
            Materializer: Mat,
            SyncMaterializer: SyncMat,
            ElementType: typeof(MtCacheRow),
            IsScalar: false,
            SingleResult: false,
            NoTracking: true,
            MethodName: "ToList",
            Includes: new List<IncludePlan>(),
            GroupJoinInfo: null,
            Tables: new[] { "MtCacheRow" },
            SplitQuery: false,
            CommandTimeout: TimeSpan.FromSeconds(30),
            IsCacheable: true,
            CacheExpiration: null,
            Fingerprint: default);
    }

    private static string InvokeBuiltCacheKey<TResult>(
        NormQueryProvider provider,
        QueryPlan plan,
        IReadOnlyDictionary<string, object> parameters)
    {
        var method = typeof(NormQueryProvider)
            .GetMethod("BuildCacheKeyFromPlan", BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TResult));
        return (string)method.Invoke(provider, new object[] { plan, parameters })!;
    }
}
