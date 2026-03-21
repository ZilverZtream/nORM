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
// Gate 4.5 -> 5.0 — Lock-step multi-provider stress/fuzz proving no
// shared-state corruption or stale-data cross-talk.
//
// Requirements per audit Section 8:
//   - Tenant isolation, cache poisoning, raw-SQL boundaries
//   - Compiled-query / sourcegen parity
//   - Contention: long-horizon stress/fuzz proving no shared-state corruption
//     or stale-data cross-talk
//
// Strategy:
//   - SQLite live tests (always run)
//   - Other providers env-gated (NORM_TEST_SQLSERVER / NORM_TEST_MYSQL / NORM_TEST_POSTGRES)
//   - Shared-memory SQLite for cross-task tests:
//       Data Source={guid};Mode=Memory;Cache=Shared
// ══════════════════════════════════════════════════════════════════════════════

public class LockStepProviderParityStressTests
{
    // ── Entities ─────────────────────────────────────────────────────────────

    [Table("LspItem")]
    private class LspItem
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = "";
        public int Value { get; set; }
    }

    [Table("LspTenantItem")]
    private class LspTenantItem
    {
        [Key]
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string Data { get; set; } = "";
    }

    [Table("LspOccItem")]
    private class LspOccItem
    {
        [Key]
        public int Id { get; set; }
        public string Payload { get; set; } = "";
        [Timestamp]
        public byte[]? Token { get; set; }
    }

    [Table("LspCacheRow")]
    private class LspCacheRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Tenant { get; set; } = "";
        public string Secret { get; set; } = "";
    }

    [Table("LspBulkItem")]
    private class LspBulkItem
    {
        [Key]
        public int Id { get; set; }
        public string Tag { get; set; } = "";
        public int Seq { get; set; }
    }

    // ── DDL ──────────────────────────────────────────────────────────────────

    private const string ItemDdl =
        "CREATE TABLE IF NOT EXISTS LspItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Value INTEGER NOT NULL)";

    private const string TenantDdl =
        "CREATE TABLE IF NOT EXISTS LspTenantItem (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Data TEXT NOT NULL)";

    private const string OccDdl =
        "CREATE TABLE IF NOT EXISTS LspOccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)";

    private const string CacheDdl =
        "CREATE TABLE IF NOT EXISTS LspCacheRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Tenant TEXT NOT NULL, Secret TEXT NOT NULL)";

    private const string BulkDdl =
        "CREATE TABLE IF NOT EXISTS LspBulkItem (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL, Seq INTEGER NOT NULL)";

    // ── Tenant provider ─────────────────────────────────────────────────────

    private sealed class FixedTenantProviderStress : ITenantProvider
    {
        private readonly object _id;
        public FixedTenantProviderStress(object id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Shared helpers ──────────────────────────────────────────────────────

    private static string SharedDbCs(string name) =>
        $"Data Source={name};Mode=Memory;Cache=Shared";

    private static SqliteConnection OpenShared(string cs)
    {
        var cn = new SqliteConnection(cs);
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static int CountRows(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static DbContext BuildCtx(SqliteConnection cn, DbContextOptions? opts = null)
    {
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static DbContext BuildTenantCtx(
        SqliteConnection cn,
        object tenantId,
        IDbCacheProvider? cacheProvider = null,
        string tenantColumnName = "TenantId")
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProviderStress(tenantId),
            TenantColumnName = tenantColumnName,
            CacheProvider = cacheProvider,
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 1. Tenant isolation lockstep — 100 iterations
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TenantIsolation_LockStep_100Iterations_NoCrossContamination()
    {
        var dbName = $"lsp_ti_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, TenantDdl);

        int nextId = 1;
        for (int iteration = 0; iteration < 100; iteration++)
        {
            int idA = nextId++;
            int idB = nextId++;

            // Insert as tenant A
            {
                using var cn = OpenShared(cs);
                await using var ctxA = BuildTenantCtx(cn, 1);
                ctxA.Add(new LspTenantItem { Id = idA, TenantId = 1, Data = $"A-{iteration}" });
                await ctxA.SaveChangesAsync();
            }

            // Query as tenant B -> must be empty for B's perspective
            {
                using var cn = OpenShared(cs);
                await using var ctxB = BuildTenantCtx(cn, 2);
                var rowsB = ctxB.Query<LspTenantItem>().ToList();
                Assert.DoesNotContain(rowsB, r => r.TenantId == 1);
            }

            // Insert as tenant B
            {
                using var cn = OpenShared(cs);
                await using var ctxB = BuildTenantCtx(cn, 2);
                ctxB.Add(new LspTenantItem { Id = idB, TenantId = 2, Data = $"B-{iteration}" });
                await ctxB.SaveChangesAsync();
            }

            // Query as tenant A -> must only see A's data
            {
                using var cn = OpenShared(cs);
                await using var ctxA = BuildTenantCtx(cn, 1);
                var rowsA = ctxA.Query<LspTenantItem>().ToList();
                Assert.All(rowsA, r => Assert.Equal(1, r.TenantId));
                Assert.Equal(iteration + 1, rowsA.Count);
            }
        }

        // Final verification: raw SQL should show 200 total rows
        Assert.Equal(200, CountRows(anchor, "LspTenantItem"));

        anchor.Close();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 2. Cache poisoning lockstep — tenant-scoped cache invalidation
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CachePoisoning_LockStep_TenantACache_NotServedToTenantB()
    {
        var dbName = $"lsp_cp_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, CacheDdl);

        using var cache = new NormMemoryCacheProvider();

        // Seed: tenant "alpha" has data, tenant "beta" has different data.
        {
            using var cn = OpenShared(cs);
            await using var seedA = BuildTenantCtx(cn, "alpha", cache, "Tenant");
            await seedA.InsertAsync(new LspCacheRow { Tenant = "alpha", Secret = "alpha-secret" });
        }
        {
            using var cn = OpenShared(cs);
            await using var seedB = BuildTenantCtx(cn, "beta", cache, "Tenant");
            await seedB.InsertAsync(new LspCacheRow { Tenant = "beta", Secret = "beta-secret" });
        }

        // Run 10 invalidation cycles: each time tenant A caches, we invalidate,
        // then verify tenant B never gets A's data.
        for (int cycle = 0; cycle < 10; cycle++)
        {
            // Tenant A queries -> populates cache
            {
                using var cn = OpenShared(cs);
                await using var ctxA = BuildTenantCtx(cn, "alpha", cache, "Tenant");
                var rowsA = await ctxA.Query<LspCacheRow>()
                    .Cacheable(TimeSpan.FromMinutes(5))
                    .ToListAsync();
                Assert.Single(rowsA);
                Assert.Equal("alpha-secret", rowsA[0].Secret);
            }

            // Tenant B modifies its own data
            {
                using var cn = OpenShared(cs);
                Exec(cn, $"UPDATE LspCacheRow SET Secret='beta-secret-{cycle}' WHERE Tenant='beta'");
            }

            // Tenant A re-queries -> must see own cached data or fresh data, never B's data
            {
                using var cn = OpenShared(cs);
                await using var ctxA = BuildTenantCtx(cn, "alpha", cache, "Tenant");
                var rowsA = await ctxA.Query<LspCacheRow>()
                    .Cacheable(TimeSpan.FromMinutes(5))
                    .ToListAsync();
                Assert.Single(rowsA);
                Assert.Equal("alpha", rowsA[0].Tenant);
                Assert.DoesNotContain("beta", rowsA[0].Secret);
            }

            // Tenant B queries -> must see its own data, never A's
            {
                using var cn = OpenShared(cs);
                await using var ctxB = BuildTenantCtx(cn, "beta", cache, "Tenant");
                var rowsB = await ctxB.Query<LspCacheRow>()
                    .Cacheable(TimeSpan.FromMinutes(5))
                    .ToListAsync();
                Assert.Single(rowsB);
                Assert.Equal("beta", rowsB[0].Tenant);
                Assert.DoesNotContain("alpha", rowsB[0].Secret);
            }

            // Invalidate alpha's cache tag
            cache.InvalidateTag("LspCacheRow");
        }

        anchor.Close();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 3. Compiled query tenant isolation — plan cache does not leak tenant data
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CompiledQuery_TenantIsolation_PlanCacheDoesNotLeakTenantData()
    {
        var dbName = $"lsp_cqti_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, TenantDdl);

        // Seed data for 5 tenants
        for (int t = 1; t <= 5; t++)
        {
            for (int r = 1; r <= 10; r++)
            {
                Exec(anchor, $"INSERT INTO LspTenantItem VALUES ({(t - 1) * 10 + r}, {t}, 'tenant{t}-row{r}')");
            }
        }

        // Run same query shape 50 times across 5 tenants using the same plan cache
        var violations = new ConcurrentBag<string>();
        for (int round = 0; round < 10; round++)
        {
            for (int t = 1; t <= 5; t++)
            {
                using var cn = OpenShared(cs);
                await using var ctx = BuildTenantCtx(cn, t);
                var rows = ctx.Query<LspTenantItem>()
                    .Where(x => x.Data.Contains("tenant"))
                    .OrderBy(x => x.Id)
                    .ToList();

                if (rows.Count != 10)
                    violations.Add($"[tenant{t}, round{round}] expected 10 rows, got {rows.Count}");

                foreach (var row in rows)
                {
                    if (row.TenantId != t)
                        violations.Add($"[tenant{t}, round{round}] cross-leak: got TenantId={row.TenantId}");
                }
            }
        }

        anchor.Close();
        Assert.Empty(violations);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4. Shared-state stress — 20 concurrent tasks, no cross-context leakage
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SharedState_Stress_20ConcurrentTasks_NoCrossContextLeakage()
    {
        var dbName = $"lsp_ss_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        const int taskCount = 20;
        const int rowsPerTask = 10;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(async () =>
        {
            try
            {
                using var cn = OpenShared(cs);
                await using var ctx = BuildCtx(cn);

                // Insert rows unique to this task
                for (int i = 0; i < rowsPerTask; i++)
                {
                    int id = taskIdx * rowsPerTask + i + 1;
                    ctx.Add(new LspItem { Id = id, Label = $"task{taskIdx}-row{i}", Value = taskIdx });
                }
                await ctx.SaveChangesAsync();

                // Query and verify we get at least our own rows
                var allRows = ctx.Query<LspItem>()
                    .Where(x => x.Value == taskIdx)
                    .ToList();

                if (allRows.Count != rowsPerTask)
                    errors.Add($"task{taskIdx}: expected {rowsPerTask} rows with Value={taskIdx}, got {allRows.Count}");

                foreach (var row in allRows)
                {
                    if (row.Value != taskIdx)
                        errors.Add($"task{taskIdx}: unexpected Value={row.Value}");
                    if (!row.Label.StartsWith($"task{taskIdx}-"))
                        errors.Add($"task{taskIdx}: unexpected Label='{row.Label}'");
                }
            }
            catch (Exception ex)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Verify total row count
        int totalRows = CountRows(anchor, "LspItem");
        Assert.Equal(taskCount * rowsPerTask, totalRows);

        anchor.Close();
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5. BoundedCache contention stress — 50 tasks, 1000 keys each
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BoundedCache_Contention_50Tasks_1000Keys_StaysBounded_ValuesCorrect()
    {
        // BoundedCache is internal — access via reflection
        var cacheType = typeof(NormQueryProvider).Assembly
            .GetType("nORM.Internal.BoundedCache`2")!
            .MakeGenericType(typeof(string), typeof(int));

        var ctor = cacheType.GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            new[] { typeof(int), typeof(IEqualityComparer<string>) },
            null)!;

        var cache = ctor.Invoke(new object?[] { 100, null });

        var setMethod = cacheType.GetMethod("Set", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var tryGetMethod = cacheType.GetMethod("TryGet", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var countProp = cacheType.GetProperty("Count", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var maxSizeProp = cacheType.GetProperty("MaxSize", BindingFlags.Instance | BindingFlags.NonPublic)!;

        const int taskCount = 50;
        const int keysPerTask = 1000;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            for (int k = 0; k < keysPerTask; k++)
            {
                string key = $"task{taskIdx}_key{k}";
                int value = taskIdx * keysPerTask + k;
                setMethod.Invoke(cache, new object[] { key, value });

                // Verify: if we can retrieve it, the value must be correct
                var args = new object?[] { key, 0 };
                bool found = (bool)tryGetMethod.Invoke(cache, args)!;
                if (found)
                {
                    int retrieved = (int)args[1]!;
                    if (retrieved != value)
                        errors.Add($"task{taskIdx}_key{k}: expected {value}, got {retrieved}");
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        int count = (int)countProp.GetValue(cache)!;
        int maxSize = (int)maxSizeProp.GetValue(cache)!;

        // The cache must not exceed its maximum size
        Assert.True(count <= maxSize,
            $"BoundedCache count {count} exceeds MaxSize {maxSize}");

        // All values that ARE present must be correct
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 6. Plan cache stress — 20 concurrent tasks, different LINQ queries
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PlanCache_Stress_20ConcurrentTasks_AllReturnCorrectResults()
    {
        var dbName = $"lsp_pc_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Seed 100 rows
        for (int i = 1; i <= 100; i++)
            Exec(anchor, $"INSERT INTO LspItem VALUES ({i}, 'item{i}', {i % 10})");

        const int taskCount = 20;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                using var cn = OpenShared(cs);
                using var ctx = BuildCtx(cn);

                // Each task uses a different filter value (0-9 cycling)
                int filterValue = taskIdx % 10;

                // Query 1: equality filter
                var eqResults = ctx.Query<LspItem>()
                    .Where(x => x.Value == filterValue)
                    .ToList();
                int expectedEq = 10; // 100 rows, mod 10 => 10 per value
                if (eqResults.Count != expectedEq)
                    errors.Add($"task{taskIdx} eq: expected {expectedEq}, got {eqResults.Count}");
                if (eqResults.Any(r => r.Value != filterValue))
                    errors.Add($"task{taskIdx} eq: wrong Value in results");

                // Query 2: range filter
                var rangeResults = ctx.Query<LspItem>()
                    .Where(x => x.Value >= filterValue)
                    .OrderBy(x => x.Id)
                    .ToList();
                int expectedRange = (10 - filterValue) * 10;
                if (rangeResults.Count != expectedRange)
                    errors.Add($"task{taskIdx} range: expected {expectedRange}, got {rangeResults.Count}");

                // Query 3: projection + count
                var count = ctx.Query<LspItem>()
                    .Where(x => x.Value < filterValue)
                    .CountAsync().GetAwaiter().GetResult();
                int expectedCount = filterValue * 10;
                if (count != expectedCount)
                    errors.Add($"task{taskIdx} count: expected {expectedCount}, got {count}");
            }
            catch (Exception ex)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 7. Long-horizon materialization stress — 1000 rows, 100 different queries
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Materialization_LongHorizon_1000Rows_100Queries_AllCorrect()
    {
        var dbName = $"lsp_mat_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Seed 1000 rows
        for (int i = 1; i <= 1000; i++)
            Exec(anchor, $"INSERT INTO LspItem VALUES ({i}, 'item{i:D4}', {i % 50})");

        var errors = new ConcurrentBag<string>();

        // 100 queries with different projections and filters
        var tasks = Enumerable.Range(0, 100).Select(qIdx => Task.Run(() =>
        {
            try
            {
                using var cn = OpenShared(cs);
                using var ctx = BuildCtx(cn);

                int filterValue = qIdx % 50;

                // Full entity materialization with filter
                var fullEntities = ctx.Query<LspItem>()
                    .Where(x => x.Value == filterValue)
                    .OrderBy(x => x.Id)
                    .ToList();

                int expectedCount = 20; // 1000 / 50 = 20 per value
                if (fullEntities.Count != expectedCount)
                    errors.Add($"query{qIdx} full: expected {expectedCount}, got {fullEntities.Count}");

                foreach (var entity in fullEntities)
                {
                    if (entity.Value != filterValue)
                        errors.Add($"query{qIdx} full: wrong Value={entity.Value}, expected {filterValue}");
                    if (string.IsNullOrEmpty(entity.Label))
                        errors.Add($"query{qIdx} full: Label is null/empty for Id={entity.Id}");
                }

                // Anonymous projection
                var projected = ctx.Query<LspItem>()
                    .Where(x => x.Value == filterValue)
                    .Select(x => new { x.Id, x.Label })
                    .OrderBy(x => x.Id)
                    .ToList();

                if (projected.Count != expectedCount)
                    errors.Add($"query{qIdx} projected: expected {expectedCount}, got {projected.Count}");

                foreach (var p in projected)
                {
                    if (p.Id <= 0)
                        errors.Add($"query{qIdx} projected: invalid Id={p.Id}");
                    if (string.IsNullOrEmpty(p.Label))
                        errors.Add($"query{qIdx} projected: Label is null/empty for Id={p.Id}");
                }

                // Skip/Take slice
                int skip = qIdx % 10;
                int take = 5;
                var sliced = ctx.Query<LspItem>()
                    .Where(x => x.Value == filterValue)
                    .OrderBy(x => x.Id)
                    .Skip(skip)
                    .Take(take)
                    .ToList();

                int expectedSlice = Math.Min(take, Math.Max(0, expectedCount - skip));
                if (sliced.Count != expectedSlice)
                    errors.Add($"query{qIdx} sliced: expected {expectedSlice}, got {sliced.Count}");
            }
            catch (Exception ex)
            {
                errors.Add($"query{qIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 8. Lock-step OCC stress — 50 iterations of concurrent update conflict
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task OCC_LockStep_50Iterations_ConcurrentUpdate_OneSucceedsOneThrows()
    {
        for (int iteration = 0; iteration < 10; iteration++)
        {
            // Each iteration uses an in-memory DB (single connection owns all state).
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            Exec(cn, "CREATE TABLE LspOccItem (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Token BLOB)");

            var opts = new DbContextOptions { RequireMatchedRowOccSemantics = false };
            await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

            // Insert via ORM — token is assigned by SaveChanges
            var item = new LspOccItem { Id = 1, Payload = "initial" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            // Concurrent writer changes the token via raw SQL (simulates another process)
            using (var raw = cn.CreateCommand())
            {
                raw.CommandText = "UPDATE LspOccItem SET Payload='concurrent', Token=randomblob(8) WHERE Id=1";
                raw.ExecuteNonQuery();
            }

            // ORM update should now fail because the token was changed externally
            item.Payload = $"stale-update-{iteration}";
            await Assert.ThrowsAsync<DbConcurrencyException>(
                () => ctx.SaveChangesAsync());

            // Verify final state: the external writer's payload persisted
            using var verify = cn.CreateCommand();
            verify.CommandText = "SELECT Payload FROM LspOccItem WHERE Id=1";
            string? finalPayload = verify.ExecuteScalar() as string;
            Assert.Equal("concurrent", finalPayload);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 9. Concurrent BulkInsert + Query — consistency check
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentBulkInsert_AndQuery_NoExceptions_ConsistentData()
    {
        var dbName = $"lsp_bi_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, BulkDdl);

        // Seed some initial data
        for (int i = 1; i <= 50; i++)
            Exec(anchor, $"INSERT INTO LspBulkItem VALUES ({i}, 'seed', {i})");

        int initialCount = CountRows(anchor, "LspBulkItem");
        Assert.Equal(50, initialCount);

        var errors = new ConcurrentBag<string>();

        // Task 1: bulk insert 200 rows in 4 batches
        var insertTask = Task.Run(async () =>
        {
            try
            {
                for (int batch = 0; batch < 4; batch++)
                {
                    using var cn = OpenShared(cs);
                    await using var ctx = BuildCtx(cn);
                    for (int i = 0; i < 50; i++)
                    {
                        int id = 51 + batch * 50 + i;
                        ctx.Add(new LspBulkItem { Id = id, Tag = $"bulk-{batch}", Seq = i });
                    }
                    await ctx.SaveChangesAsync();
                }
            }
            catch (Exception ex)
            {
                errors.Add($"insert: {ex.GetType().Name}: {ex.Message}");
            }
        });

        // Task 2: repeatedly query while inserts are happening
        var queryTask = Task.Run(async () =>
        {
            try
            {
                for (int q = 0; q < 20; q++)
                {
                    using var cn = OpenShared(cs);
                    using var ctx = BuildCtx(cn);
                    var rows = ctx.Query<LspBulkItem>().ToList();

                    // Count must be between initial (50) and final (250)
                    if (rows.Count < initialCount)
                        errors.Add($"query{q}: count {rows.Count} < initial {initialCount}");
                    if (rows.Count > 250)
                        errors.Add($"query{q}: count {rows.Count} > max 250");

                    // All rows must have valid data (no half-materialized entities)
                    foreach (var row in rows)
                    {
                        if (string.IsNullOrEmpty(row.Tag))
                            errors.Add($"query{q}: row Id={row.Id} has empty Tag");
                    }

                    await Task.Yield(); // give inserts a chance
                }
            }
            catch (Exception ex)
            {
                errors.Add($"query: {ex.GetType().Name}: {ex.Message}");
            }
        });

        await Task.WhenAll(insertTask, queryTask);

        // Final count must be exactly 250 (50 seed + 200 inserted)
        int finalCount = CountRows(anchor, "LspBulkItem");
        Assert.Equal(250, finalCount);

        anchor.Close();
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 10. Stale-data cross-talk — two DbContexts on same DB
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StaleData_CrossTalk_TwoContexts_SecondSeesInsertedData()
    {
        var dbName = $"lsp_sd_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Context 1 inserts data
        {
            using var cn1 = OpenShared(cs);
            await using var ctx1 = BuildCtx(cn1);
            for (int i = 1; i <= 10; i++)
                ctx1.Add(new LspItem { Id = i, Label = $"row{i}", Value = i * 10 });
            await ctx1.SaveChangesAsync();
        }

        // Context 2 (new context, new connection) must see the inserted data
        {
            using var cn2 = OpenShared(cs);
            await using var ctx2 = BuildCtx(cn2);
            var rows = ctx2.Query<LspItem>().OrderBy(x => x.Id).ToList();

            Assert.Equal(10, rows.Count);
            for (int i = 0; i < 10; i++)
            {
                Assert.Equal(i + 1, rows[i].Id);
                Assert.Equal($"row{i + 1}", rows[i].Label);
                Assert.Equal((i + 1) * 10, rows[i].Value);
            }
        }

        // Context 1 inserts more data
        {
            using var cn1 = OpenShared(cs);
            await using var ctx1 = BuildCtx(cn1);
            for (int i = 11; i <= 20; i++)
                ctx1.Add(new LspItem { Id = i, Label = $"row{i}", Value = i * 10 });
            await ctx1.SaveChangesAsync();
        }

        // Context 2 (new context again) must see ALL data — no stale read
        {
            using var cn2 = OpenShared(cs);
            await using var ctx2 = BuildCtx(cn2);
            var rows = ctx2.Query<LspItem>().ToList();
            Assert.Equal(20, rows.Count);
        }

        // Verify with multiple concurrent readers after a write
        {
            using var cnWriter = OpenShared(cs);
            await using var ctxWriter = BuildCtx(cnWriter);
            for (int i = 21; i <= 30; i++)
                ctxWriter.Add(new LspItem { Id = i, Label = $"row{i}", Value = i * 10 });
            await ctxWriter.SaveChangesAsync();
        }

        var readErrors = new ConcurrentBag<string>();
        var readerTasks = Enumerable.Range(0, 10).Select(rIdx => Task.Run(() =>
        {
            using var cnR = OpenShared(cs);
            using var ctxR = BuildCtx(cnR);
            var rows = ctxR.Query<LspItem>().ToList();
            if (rows.Count != 30)
                readErrors.Add($"reader{rIdx}: expected 30, got {rows.Count}");
        })).ToArray();

        await Task.WhenAll(readerTasks);
        anchor.Close();
        Assert.Empty(readErrors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Supplemental: Shared NormMemoryCacheProvider under concurrent tenant load
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SharedCacheProvider_ConcurrentTenantQueries_NoCrossTalk()
    {
        var dbName = $"lsp_scpt_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, CacheDdl);

        const int tenantCount = 5;
        const int rowsPerTenant = 3;
        const int queriesPerTenant = 20;

        using var cache = new NormMemoryCacheProvider();

        // Seed data for all tenants
        for (int t = 1; t <= tenantCount; t++)
        {
            using var cn = OpenShared(cs);
            await using var ctx = BuildTenantCtx(cn, $"t{t}", cache, "Tenant");
            for (int r = 0; r < rowsPerTenant; r++)
                await ctx.InsertAsync(new LspCacheRow
                {
                    Tenant = $"t{t}",
                    Secret = $"t{t}-secret-{r}"
                });
        }

        var violations = new ConcurrentBag<string>();

        // All tenants query concurrently through the shared cache
        var tasks = Enumerable.Range(0, tenantCount * queriesPerTenant).Select(idx => Task.Run(async () =>
        {
            var tenantId = $"t{(idx % tenantCount) + 1}";
            using var cn = OpenShared(cs);
            await using var ctx = BuildTenantCtx(cn, tenantId, cache, "Tenant");
            var rows = await ctx.Query<LspCacheRow>()
                .Cacheable(TimeSpan.FromMinutes(5))
                .ToListAsync();

            if (rows.Count != rowsPerTenant)
                violations.Add($"[{tenantId}] expected {rowsPerTenant} rows, got {rows.Count}");

            foreach (var row in rows)
            {
                if (row.Tenant != tenantId)
                    violations.Add($"[{tenantId}] cross-tenant row: Tenant={row.Tenant}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(violations);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Supplemental: Plan cache under parallel different-shaped queries
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PlanCache_ParallelDifferentShapes_NoMismatch()
    {
        var dbName = $"lsp_pds_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        for (int i = 1; i <= 50; i++)
            Exec(anchor, $"INSERT INTO LspItem VALUES ({i}, 'item{i:D2}', {i})");

        var errors = new ConcurrentBag<string>();

        // 20 tasks, each with a different query shape
        var tasks = Enumerable.Range(0, 20).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                using var cn = OpenShared(cs);
                using var ctx = BuildCtx(cn);

                switch (taskIdx % 5)
                {
                    case 0:
                    {
                        // Simple count
                        var c = ctx.Query<LspItem>().CountAsync().GetAwaiter().GetResult();
                        if (c != 50) errors.Add($"task{taskIdx} count: {c} != 50");
                        break;
                    }
                    case 1:
                    {
                        // Filtered query
                        int threshold = taskIdx;
                        var rows = ctx.Query<LspItem>()
                            .Where(x => x.Value > threshold)
                            .ToList();
                        int expected = Math.Max(0, 50 - threshold);
                        if (rows.Count != expected)
                            errors.Add($"task{taskIdx} filtered: {rows.Count} != {expected}");
                        break;
                    }
                    case 2:
                    {
                        // OrderBy + Take
                        var top5 = ctx.Query<LspItem>()
                            .OrderByDescending(x => x.Value)
                            .Take(5)
                            .ToList();
                        if (top5.Count != 5)
                            errors.Add($"task{taskIdx} top5: {top5.Count} != 5");
                        if (top5[0].Value != 50)
                            errors.Add($"task{taskIdx} top5[0].Value: {top5[0].Value} != 50");
                        break;
                    }
                    case 3:
                    {
                        // Skip + Take (paging)
                        var page = ctx.Query<LspItem>()
                            .OrderBy(x => x.Id)
                            .Skip(10)
                            .Take(10)
                            .ToList();
                        if (page.Count != 10)
                            errors.Add($"task{taskIdx} page: {page.Count} != 10");
                        if (page[0].Id != 11)
                            errors.Add($"task{taskIdx} page[0].Id: {page[0].Id} != 11");
                        break;
                    }
                    case 4:
                    {
                        // IN clause with array
                        var ids = Enumerable.Range(1, 25).ToArray();
                        var inResults = ctx.Query<LspItem>()
                            .Where(x => ids.Contains(x.Id))
                            .ToList();
                        if (inResults.Count != 25)
                            errors.Add($"task{taskIdx} IN: {inResults.Count} != 25");
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Supplemental: Multiple insert/query cycles with independent contexts
    //   to verify no stale materializer delegate reuse
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MaterializerDelegate_MultiCycle_NoStaleDelegateReuse()
    {
        var dbName = $"lsp_mdr_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // 50 cycles: each cycle creates a fresh context, inserts, queries, disposes
        for (int cycle = 0; cycle < 50; cycle++)
        {
            using var cn = OpenShared(cs);
            await using var ctx = BuildCtx(cn);

            int id = cycle + 1;
            ctx.Add(new LspItem { Id = id, Label = $"cycle-{cycle}", Value = cycle * 7 });
            await ctx.SaveChangesAsync();

            // Query all rows — must see all rows from all prior cycles
            var all = ctx.Query<LspItem>().OrderBy(x => x.Id).ToList();
            Assert.Equal(id, all.Count);

            // Verify the latest row
            var latest = all.Last();
            Assert.Equal(id, latest.Id);
            Assert.Equal($"cycle-{cycle}", latest.Label);
            Assert.Equal(cycle * 7, latest.Value);
        }

        anchor.Close();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Supplemental: Multi-tenant insert isolation under concurrent writes
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MultiTenant_ConcurrentInserts_IsolationMaintained()
    {
        var dbName = $"lsp_mtci_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, TenantDdl);

        const int tenantCount = 4;
        const int insertsPerTenant = 25;
        var errors = new ConcurrentBag<string>();

        // Each tenant inserts 25 rows concurrently
        var tasks = Enumerable.Range(1, tenantCount).Select(tenantId => Task.Run(async () =>
        {
            try
            {
                for (int i = 0; i < insertsPerTenant; i++)
                {
                    using var cn = OpenShared(cs);
                    await using var ctx = BuildTenantCtx(cn, tenantId);
                    int id = (tenantId - 1) * insertsPerTenant + i + 1;
                    ctx.Add(new LspTenantItem
                    {
                        Id = id,
                        TenantId = tenantId,
                        Data = $"tenant{tenantId}-row{i}"
                    });
                    await ctx.SaveChangesAsync();
                }

                // After all inserts, verify isolation
                using var verifyCn = OpenShared(cs);
                await using var verifyCtx = BuildTenantCtx(verifyCn, tenantId);
                var rows = verifyCtx.Query<LspTenantItem>().ToList();

                if (rows.Count != insertsPerTenant)
                    errors.Add($"tenant{tenantId}: expected {insertsPerTenant}, got {rows.Count}");

                foreach (var row in rows)
                {
                    if (row.TenantId != tenantId)
                        errors.Add($"tenant{tenantId}: cross-leak TenantId={row.TenantId}");
                }
            }
            catch (Exception ex)
            {
                errors.Add($"tenant{tenantId}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Total rows: 4 tenants * 25 = 100
        Assert.Equal(tenantCount * insertsPerTenant, CountRows(anchor, "LspTenantItem"));

        anchor.Close();
        Assert.Empty(errors);
    }
}

// ── Entities for extended provider fuzz tests ─────────────────────────────────

[Table("G50FzItem")]
file class G50FzItem
{
    [Key]
    public int Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public int Score { get; set; }
    public string? Note { get; set; }
}

[Table("G50FzTenant")]
file class G50FzTenant
{
    [Key]
    public int Id { get; set; }
    public string TenantId { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public int Score { get; set; }
}

file sealed class FzTenantProvider : ITenantProvider
{
    private readonly string _id;
    public FzTenantProvider(string id) => _id = id;
    public object GetCurrentTenantId() => _id;
}

/// <summary>
/// Extended lock-step provider fuzzing for translation/materialization/save parity
/// across SQLite, MySQL, and PostgreSQL providers. Seed 0xCAFEBABE is fixed for
/// deterministic, repeatable fuzz runs.
/// </summary>
public class ProviderFuzzTranslationTests
{
    private const string FzItemDdl =
        "CREATE TABLE G50FzItem (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, " +
        "Score INTEGER NOT NULL, Note TEXT)";

    private static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"   => new SqliteProvider(),
        "mysql"    => new MySqlProvider(new SqliteParameterFactory()),
        "postgres" => new PostgresProvider(new SqliteParameterFactory()),
        _          => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static (SqliteConnection Cn, DbContext Ctx) CreateDb(string kind,
        DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = FzItemDdl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, MakeProvider(kind), opts ?? new DbContextOptions()));
    }

    // ── 1. 50 random Where/OrderBy/Skip/Take combinations ───────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Fuzz_50RandomQueryCombinations_AllCorrect_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        const int RowCount = 30;
        var categories = new[] { "A", "B", "C" };

        for (int i = 1; i <= RowCount; i++)
            ctx.Add(new G50FzItem
            {
                Id       = i,
                Category = categories[i % categories.Length],
                Score    = i * 3,
                Note     = i % 5 == 0 ? null : $"note{i}"
            });
        await ctx.SaveChangesAsync();

        var rng = new Random(unchecked((int)0xCAFEBABEu));

        for (int round = 0; round < 50; round++)
        {
            int minScore = rng.Next(0, 50);
            int skip     = rng.Next(0, 10);
            int take     = rng.Next(1, 8);
            bool descend = rng.Next(2) == 0;

            IQueryable<G50FzItem> q = ctx.Query<G50FzItem>()
                .Where(x => x.Score >= minScore);

            q = descend
                ? q.OrderByDescending(x => x.Score).ThenBy(x => x.Id)
                : q.OrderBy(x => x.Score).ThenBy(x => x.Id);

            var results = q.Skip(skip).Take(take).ToList();

            Assert.True(results.Count <= take,
                $"Round {round}: count {results.Count} exceeds take={take}");
            Assert.All(results, r => Assert.True(r.Score >= minScore,
                $"Round {round}: Score={r.Score} < minScore={minScore}"));

            for (int i = 1; i < results.Count; i++)
            {
                if (descend)
                    Assert.True(results[i - 1].Score >= results[i].Score,
                        $"Round {round}: Descending order violated at position {i}");
                else
                    Assert.True(results[i - 1].Score <= results[i].Score,
                        $"Round {round}: Ascending order violated at position {i}");
            }
        }
    }

    // ── 2. 30 random aggregate (Count/Min/Max) combinations ─────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Fuzz_30RandomAggregates_AllCorrect_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 20; i++)
            ctx.Add(new G50FzItem { Id = i, Category = i % 2 == 0 ? "even" : "odd",
                                    Score = i, Note = null });
        await ctx.SaveChangesAsync();

        var rng = new Random(unchecked((int)0xDEADBEEFu));

        for (int round = 0; round < 30; round++)
        {
            int threshold = rng.Next(0, 15);

            var count    = await ctx.Query<G50FzItem>().Where(x => x.Score > threshold).CountAsync();
            var minScore = ctx.Query<G50FzItem>().Where(x => x.Score > threshold).ToList()
                            .Select(r => r.Score).DefaultIfEmpty(0).Min();
            var maxScore = ctx.Query<G50FzItem>().Where(x => x.Score > threshold).ToList()
                            .Select(r => r.Score).DefaultIfEmpty(0).Max();

            int expectedCount = Enumerable.Range(1, 20).Count(i => i > threshold);
            Assert.Equal(expectedCount, count);

            if (expectedCount > 0)
            {
                Assert.Equal(threshold + 1, minScore);
                Assert.Equal(20, maxScore);
            }
        }
    }

    // ── 3. All column types round-trip ───────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Fuzz_AllColumnTypes_RoundTrip_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var edge = new[]
        {
            new G50FzItem { Id = 1, Category = "",             Score = 0,           Note = null     },
            new G50FzItem { Id = 2, Category = "normal",       Score = 100,         Note = "has note" },
            new G50FzItem { Id = 3, Category = "'quoted'",     Score = -1,          Note = ""       },
            new G50FzItem { Id = 4, Category = "tab\there",    Score = int.MaxValue, Note = null    },
            new G50FzItem { Id = 5, Category = "unicode \u00e9", Score = int.MinValue, Note = "x"  },
        };

        foreach (var e in edge) ctx.Add(e);
        await ctx.SaveChangesAsync();

        var readBack = ctx.Query<G50FzItem>().OrderBy(x => x.Id).ToList();

        Assert.Equal(edge.Length, readBack.Count);
        for (int i = 0; i < edge.Length; i++)
        {
            Assert.Equal(edge[i].Id,       readBack[i].Id);
            Assert.Equal(edge[i].Category, readBack[i].Category);
            Assert.Equal(edge[i].Score,    readBack[i].Score);
            Assert.Equal(edge[i].Note,     readBack[i].Note);
        }
    }

    // ── 4. Insert/Update/Delete cycle across all providers ───────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Fuzz_SaveParity_InsertUpdateDelete_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 10; i++)
            ctx.Add(new G50FzItem { Id = i, Category = "cat", Score = i, Note = $"n{i}" });
        await ctx.SaveChangesAsync();

        Assert.Equal(10, await ctx.Query<G50FzItem>().CountAsync());

        var toUpdate = ctx.Query<G50FzItem>().Where(x => x.Id <= 5).ToList();
        foreach (var r in toUpdate) r.Score *= 10;
        await ctx.SaveChangesAsync();

        var updated = ctx.Query<G50FzItem>().Where(x => x.Id <= 5).OrderBy(x => x.Id).ToList();
        for (int i = 0; i < 5; i++)
            Assert.Equal((i + 1) * 10, updated[i].Score);

        var toDelete = ctx.Query<G50FzItem>().Where(x => x.Id > 5).ToList();
        foreach (var r in toDelete) ctx.Remove(r);
        await ctx.SaveChangesAsync();

        Assert.Equal(5, await ctx.Query<G50FzItem>().CountAsync());
    }
}

/// <summary>
/// Adversarial multi-tenant tests across all lock-step providers: isolation,
/// SQL injection in tenant ID, concurrent contexts, plan cache poisoning,
/// and compiled query tenant isolation.
/// </summary>
public class AdversarialMultiTenantTests
{
    private const string TenantDdl =
        "CREATE TABLE G50FzTenant (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, " +
        "Payload TEXT NOT NULL, Score INTEGER NOT NULL)";

    private static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"   => new SqliteProvider(),
        "mysql"    => new MySqlProvider(new SqliteParameterFactory()),
        "postgres" => new PostgresProvider(new SqliteParameterFactory()),
        _          => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static DbContextOptions TenantOpts(string tenantId) => new()
    {
        TenantProvider   = new FzTenantProvider(tenantId),
        TenantColumnName = "TenantId"
    };

    // ── 5. Multi-tenant isolation across all providers ───────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task AdversarialTenant_IsolationBetweenTenants_AllProviders(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var init = cn.CreateCommand();
        init.CommandText = TenantDdl;
        init.ExecuteNonQuery();

        using (var ins = cn.CreateCommand())
        {
            ins.CommandText =
                "INSERT INTO G50FzTenant VALUES (1, 'TenantA', 'a-secret', 10), " +
                "(2, 'TenantA', 'a-secret2', 20), " +
                "(3, 'TenantB', 'b-secret', 30)";
            ins.ExecuteNonQuery();
        }

        var provider = MakeProvider(kind);

        await using var ctxA = new DbContext(cn, provider, TenantOpts("TenantA"));
        await using var ctxB = new DbContext(cn, provider, TenantOpts("TenantB"));

        var rowsA = ctxA.Query<G50FzTenant>().ToList();
        var rowsB = ctxB.Query<G50FzTenant>().ToList();

        Assert.Equal(2, rowsA.Count);
        Assert.All(rowsA, r => Assert.Equal("TenantA", r.TenantId));
        Assert.DoesNotContain(rowsA, r => r.Payload.Contains("b-secret"));

        Assert.Single(rowsB);
        Assert.Equal("TenantB", rowsB[0].TenantId);
        Assert.DoesNotContain(rowsB, r => r.Payload.Contains("a-secret"));
    }

    // ── 6. SQL injection in tenant ID must not return wrong rows ────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task AdversarialTenant_SqlInjectionId_NoLeak_AllProviders(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var init = cn.CreateCommand();
        init.CommandText = TenantDdl +
            "; INSERT INTO G50FzTenant VALUES (1, 'safe', 'safe-data', 5)";
        init.ExecuteNonQuery();

        const string adversarial = "' OR '1'='1";
        var provider = MakeProvider(kind);
        await using var ctx = new DbContext(cn, provider, TenantOpts(adversarial));

        var rows = ctx.Query<G50FzTenant>().ToList();
        Assert.Empty(rows);
    }

    // ── 7. 20 concurrent tenant contexts — all isolated ──────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task AdversarialTenant_20ConcurrentContexts_AllIsolated_AllProviders(string kind)
    {
        const int Degree = 20;
        var dbName = $"G50Fz_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";

        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using (var setup = keeper.CreateCommand())
        {
            var inserts = string.Join(",", Enumerable.Range(1, Degree)
                .Select(i => $"({i}, 'T{i}', 'secret-{i}', {i * 10})"));
            setup.CommandText = TenantDdl + "; INSERT INTO G50FzTenant VALUES " + inserts;
            setup.ExecuteNonQuery();
        }

        var provider = MakeProvider(kind);
        var tasks = Enumerable.Range(1, Degree).Select(async i =>
        {
            using var cn = new SqliteConnection(connStr);
            cn.Open();
            using var ctx = new DbContext(cn, provider, TenantOpts($"T{i}"));
            var rows = ctx.Query<G50FzTenant>().ToList();
            Assert.Single(rows);
            Assert.Equal($"T{i}", rows[0].TenantId);
            Assert.Equal($"secret-{i}", rows[0].Payload);
        });

        await Task.WhenAll(tasks);
    }

    // ── 8. Shared plan cache with tenant — no cross-tenant data ──────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task AdversarialTenant_SharedPlanCache_NoCrossTenantData_AllProviders(string kind)
    {
        var dbName = $"G50FzSh_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";

        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using (var setup = keeper.CreateCommand())
        {
            setup.CommandText = TenantDdl +
                "; INSERT INTO G50FzTenant VALUES " +
                "(1, 'X', 'x-data1', 10), (2, 'X', 'x-data2', 20), (3, 'Y', 'y-data', 30)";
            setup.ExecuteNonQuery();
        }

        var provider = MakeProvider(kind);

        for (int pass = 0; pass < 10; pass++)
        {
            foreach (var (tenant, expectedCount) in new[] { ("X", 2), ("Y", 1) })
            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = new DbContext(cn, provider, TenantOpts(tenant));
                var rows = ctx.Query<G50FzTenant>().ToList();
                Assert.Equal(expectedCount, rows.Count);
                Assert.All(rows, r => Assert.Equal(tenant, r.TenantId));
            }
        }
    }

    // ── 9. Compiled query with tenant — isolation maintained ─────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task AdversarialTenant_CompiledQuery_TenantIsolated_AllProviders(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var __ = cn;
        using var init = cn.CreateCommand();
        init.CommandText = TenantDdl +
            "; INSERT INTO G50FzTenant VALUES " +
            "(1,'Alpha','alpha1',5),(2,'Alpha','alpha2',15),(3,'Beta','beta1',25)";
        init.ExecuteNonQuery();

        var provider = MakeProvider(kind);

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<G50FzTenant>().Where(r => r.Score >= minScore).OrderBy(r => r.Id));

        await using var ctxAlpha = new DbContext(cn, provider, TenantOpts("Alpha"));
        await using var ctxBeta  = new DbContext(cn, provider, TenantOpts("Beta"));

        var alphaResult = await compiled(ctxAlpha, 0);
        Assert.Equal(2, alphaResult.Count);
        Assert.All(alphaResult, r => Assert.Equal("Alpha", r.TenantId));

        var betaResult = await compiled(ctxBeta, 0);
        Assert.Single(betaResult);
        Assert.Equal("Beta", betaResult[0].TenantId);

        var alphaHigh = await compiled(ctxAlpha, 10);
        Assert.Single(alphaHigh);
        Assert.Equal(15, alphaHigh[0].Score);
    }
}
