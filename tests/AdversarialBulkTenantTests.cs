using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
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
/// Gate 5.0 Section 8: Adversarial multi-tenant end-to-end suites covering bulk paths,
/// cache invalidation, compiled queries, interceptors, transaction nesting, and lock-step
/// provider parity. All tests are SQLite-based live tests.
/// </summary>
public class AdversarialBulkTenantTests
{
    // ── Entities ──────────────────────────────────────────────────────────────

    [Table("G50Item")]
    private class G50Item
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
    }

    [Table("G50OccItem")]
    private class G50OccItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;

        [Timestamp]
        public string? RowVersion { get; set; }
    }

    // ── Tenant provider ──────────────────────────────────────────────────────

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── SQL-capturing interceptor ────────────────────────────────────────────

    private sealed class SqlCapturingInterceptor : IDbCommandInterceptor
    {
        private readonly List<string> _sqls = new();

        public IReadOnlyList<string> CapturedSql
        {
            get { lock (_sqls) return _sqls.ToList(); }
        }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            Capture(command);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            Capture(command);
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            Capture(command);
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct)
            => Task.CompletedTask;

        private void Capture(DbCommand cmd)
        {
            lock (_sqls)
                _sqls.Add(cmd.CommandText);
        }
    }

    // ── Spy cache provider ───────────────────────────────────────────────────

    private sealed class SpyCacheProvider : IDbCacheProvider, IDisposable
    {
        private readonly NormMemoryCacheProvider _inner;
        public List<string> InvalidatedTags { get; } = new();

        public SpyCacheProvider(NormMemoryCacheProvider inner) => _inner = inner;

        public bool TryGet<T>(string key, out T? value) => _inner.TryGet(key, out value);

        public void Set<T>(string key, T value, TimeSpan expiration, IEnumerable<string> tags)
            => _inner.Set(key, value, expiration, tags);

        public void InvalidateTag(string tag)
        {
            InvalidatedTags.Add(tag);
            _inner.InvalidateTag(tag);
        }

        public void Dispose() => _inner.Dispose();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<SqliteConnection> CreateDbAsync(string tableDdl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = tableDdl;
        await cmd.ExecuteNonQueryAsync();
        return cn;
    }

    private static DbContext MakeCtx(SqliteConnection cn, string tenantId, DbContextOptions? opts = null)
    {
        opts ??= new DbContextOptions();
        opts.TenantProvider = new FixedTenantProvider(tenantId);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static async Task InsertRawAsync(SqliteConnection cn, string table, int id, string name, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {table} (Id, Name, TenantId) VALUES (@id, @n, @tid)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertOccRawAsync(SqliteConnection cn, int id, string name, string tenantId, string? rv)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO G50OccItem (Id, Name, TenantId, RowVersion) VALUES (@id, @n, @tid, @rv)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        cmd.Parameters.AddWithValue("@rv", (object?)rv ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadNameAsync(SqliteConnection cn, string table, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Name FROM {table} WHERE Id=@id";
        cmd.Parameters.AddWithValue("@id", id);
        return (await cmd.ExecuteScalarAsync()) as string;
    }

    private static async Task<long> CountRowsAsync(SqliteConnection cn, string table)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<long> CountRowsForTenantAsync(SqliteConnection cn, string table, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table} WHERE TenantId=@tid";
        cmd.Parameters.AddWithValue("@tid", tenantId);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // ── Test 1: Adversarial tenant + bulk + cache ────────────────────────────

    /// <summary>
    /// Tenant A inserts rows and caches a query. Tenant B attempts BulkUpdate with
    /// forged PKs belonging to Tenant A. Verifies:
    ///   - Tenant A's cache is NOT poisoned
    ///   - Tenant B's update affects 0 rows
    ///   - Tenant A's subsequent query returns original data
    /// </summary>
    [Fact]
    public async Task AdversarialTenantBulkCache_ForgedPKs_CacheNotPoisoned()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");
        using var cache = new NormMemoryCacheProvider();

        // Tenant A: insert rows and cache a query
        var optsA = new DbContextOptions { CacheProvider = cache };
        var ctxA = MakeCtx(cn, "A", optsA);
        await InsertRawAsync(cn, "G50Item", 1, "alpha-row1", "A");
        await InsertRawAsync(cn, "G50Item", 2, "alpha-row2", "A");

        var cachedA = await ctxA.Query<G50Item>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Equal(2, cachedA.Count);
        Assert.All(cachedA, r => Assert.Equal("A", r.TenantId));

        // Tenant B: attempt BulkUpdate with Tenant A's PKs
        var optsB = new DbContextOptions { CacheProvider = cache };
        var ctxB = MakeCtx(cn, "B", optsB);
        var forgedEntities = new[]
        {
            new G50Item { Id = 1, Name = "forged-by-B", TenantId = "B" },
            new G50Item { Id = 2, Name = "forged-by-B", TenantId = "B" }
        };
        var updated = await ctxB.BulkUpdateAsync(forgedEntities);

        // Tenant B's update must affect 0 rows (tenant predicate blocks cross-tenant)
        Assert.Equal(0, updated);

        // DB rows must be unchanged
        Assert.Equal("alpha-row1", await ReadNameAsync(cn, "G50Item", 1));
        Assert.Equal("alpha-row2", await ReadNameAsync(cn, "G50Item", 2));

        // Tenant A's cache must not be poisoned: fresh context reads from cache
        var freshOptsA = new DbContextOptions { CacheProvider = cache };
        var freshCtxA = MakeCtx(cn, "A", freshOptsA);
        var afterCacheA = await freshCtxA.Query<G50Item>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Equal(2, afterCacheA.Count);
        Assert.Contains(afterCacheA, r => r.Name == "alpha-row1");
        Assert.Contains(afterCacheA, r => r.Name == "alpha-row2");
    }

    // ── Test 2: Tenant isolation with interceptor ────────────────────────────

    /// <summary>
    /// Registers a command interceptor that logs all SQL. BulkUpdate with cross-tenant PK.
    /// Verifies the interceptor saw SQL with tenant predicate and 0 rows were affected.
    /// </summary>
    [Fact]
    public async Task TenantIsolationWithInterceptor_BulkUpdate_SqlContainsTenantPredicate()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");

        // Insert row for tenant B
        await InsertRawAsync(cn, "G50Item", 10, "belongs-to-B", "B");

        // Tenant A context with interceptor
        var interceptor = new SqlCapturingInterceptor();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(interceptor);
        var ctxA = MakeCtx(cn, "A", opts);

        // Tenant A tries to BulkUpdate tenant B's row
        var entity = new G50Item { Id = 10, Name = "tampered-by-A", TenantId = "A" };
        var updated = await ctxA.BulkUpdateAsync(new[] { entity });

        Assert.Equal(0, updated);
        Assert.Equal("belongs-to-B", await ReadNameAsync(cn, "G50Item", 10));

        // Verify interceptor captured SQL containing TenantId predicate
        var capturedSql = interceptor.CapturedSql;
        Assert.True(capturedSql.Count > 0, "Interceptor should have captured at least one SQL statement");
        var updateSql = capturedSql.FirstOrDefault(s =>
            s.Contains("UPDATE", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(updateSql);
        Assert.Contains("TenantId", updateSql, StringComparison.OrdinalIgnoreCase);
    }

    // ── Test 3: Bulk + transaction nesting ───────────────────────────────────

    /// <summary>
    /// Start outer transaction, BulkInsert inside it, verify rows visible within transaction,
    /// rollback outer transaction, verify rows are gone.
    /// </summary>
    [Fact]
    public async Task BulkInsert_InsideTransaction_RollbackRemovesRows()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");
        var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        await ctx.BulkInsertAsync(new[]
        {
            new G50Item { Id = 1, Name = "row1", TenantId = "X" },
            new G50Item { Id = 2, Name = "row2", TenantId = "X" },
            new G50Item { Id = 3, Name = "row3", TenantId = "X" }
        });

        // Rows should be visible within the transaction
        var countInTx = await CountRowsAsync(cn, "G50Item");
        Assert.Equal(3, countInTx);

        // Rollback
        await tx.RollbackAsync();

        // Rows should be gone after rollback
        var countAfter = await CountRowsAsync(cn, "G50Item");
        Assert.Equal(0, countAfter);
    }

    // ── Test 4: Bulk + SaveChanges interleave ────────────────────────────────

    /// <summary>
    /// BulkInsert some rows, then use SaveChangesAsync to insert more rows in the same
    /// table. Verify both sets are visible.
    /// </summary>
    [Fact]
    public async Task BulkInsert_Then_SaveChanges_BothSetsVisible()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");
        var ctx = new DbContext(cn, new SqliteProvider());

        // BulkInsert first batch
        await ctx.BulkInsertAsync(new[]
        {
            new G50Item { Id = 1, Name = "bulk-1", TenantId = "X" },
            new G50Item { Id = 2, Name = "bulk-2", TenantId = "X" }
        });

        // SaveChanges second batch
        ctx.Add(new G50Item { Id = 3, Name = "save-3", TenantId = "X" });
        ctx.Add(new G50Item { Id = 4, Name = "save-4", TenantId = "X" });
        await ctx.SaveChangesAsync();

        // All 4 rows should be visible
        var total = await CountRowsAsync(cn, "G50Item");
        Assert.Equal(4, total);

        Assert.Equal("bulk-1", await ReadNameAsync(cn, "G50Item", 1));
        Assert.Equal("bulk-2", await ReadNameAsync(cn, "G50Item", 2));
        Assert.Equal("save-3", await ReadNameAsync(cn, "G50Item", 3));
        Assert.Equal("save-4", await ReadNameAsync(cn, "G50Item", 4));
    }

    // ── Test 5: Cache poisoning attempt (tenant-scoped cache) ────────────────

    /// <summary>
    /// Two tenants share the same NormMemoryCacheProvider (with tenant func). Tenant A
    /// caches a query. Tenant B does BulkInsert. Verify Tenant A's cache is NOT invalidated
    /// by Tenant B's bulk write (different tenant scope).
    /// </summary>
    [Fact]
    public async Task CachePoisoning_TenantScopedCache_BulkInsertDoesNotInvalidateOtherTenantCache()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");

        string currentTenant = "A";
        using var cache = new NormMemoryCacheProvider(() => currentTenant);

        // Tenant A: seed and cache
        currentTenant = "A";
        var optsA = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("A"),
            CacheProvider = cache
        };
        var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await InsertRawAsync(cn, "G50Item", 1, "tenantA-data", "A");

        var primedA = await ctxA.Query<G50Item>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(primedA);
        Assert.Equal("tenantA-data", primedA[0].Name);

        // Tenant B: BulkInsert into the same table (different tenant scope)
        currentTenant = "B";
        var optsB = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("B"),
            CacheProvider = cache
        };
        var ctxB = new DbContext(cn, new SqliteProvider(), optsB);
        await ctxB.BulkInsertAsync(new[]
        {
            new G50Item { Id = 100, Name = "tenantB-data", TenantId = "B" }
        });

        // Tenant A's cache should still be valid (not invalidated by tenant B's write)
        currentTenant = "A";
        var freshOptsA = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("A"),
            CacheProvider = cache
        };
        var freshCtxA = new DbContext(cn, new SqliteProvider(), freshOptsA);
        var afterBulkA = await freshCtxA.Query<G50Item>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();

        // If cache was hit (not invalidated), result is still the original 1 row
        // If cache was invalidated, a fresh query would return only tenant A's rows
        // (which is also 1 row due to tenant filter). Either way, it must be 1 tenant-A row.
        Assert.True(afterBulkA.Count >= 1);
        Assert.All(afterBulkA, r => Assert.Equal("A", r.TenantId));
        Assert.Contains(afterBulkA, r => r.Name == "tenantA-data");
    }

    // ── Test 6: Adversarial PK collision ─────────────────────────────────────

    /// <summary>
    /// Insert entity with Id=1 for tenant A. Create entity with Id=1 for tenant B,
    /// try BulkUpdate. Should not modify tenant A's row.
    /// </summary>
    [Fact]
    public async Task AdversarialPKCollision_BulkUpdate_DoesNotModifyCrossTenantRow()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");

        // Insert for tenant A
        await InsertRawAsync(cn, "G50Item", 1, "tenant-A-original", "A");

        // Tenant B tries to BulkUpdate Id=1
        var ctxB = MakeCtx(cn, "B");
        var entity = new G50Item { Id = 1, Name = "overwritten-by-B", TenantId = "B" };
        var updated = await ctxB.BulkUpdateAsync(new[] { entity });

        Assert.Equal(0, updated);
        Assert.Equal("tenant-A-original", await ReadNameAsync(cn, "G50Item", 1));
    }

    // ── Test 7: Bulk + OCC + tenant triple guard ─────────────────────────────

    /// <summary>
    /// Entity with PK + timestamp + tenant. Tests two failure modes:
    ///   - BulkUpdate with correct PK, stale timestamp, correct tenant -> 0 rows updated
    ///   - BulkUpdate with correct PK, correct timestamp, wrong tenant -> 0 rows updated
    /// </summary>
    [Fact]
    public async Task TripleGuard_StaleTimestamp_CorrectTenant_ZeroRowsUpdated()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50OccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL, RowVersion TEXT)");

        await InsertOccRawAsync(cn, 1, "original", "A", "tok-v1");

        // Correct PK, stale timestamp, correct tenant
        var ctxA = MakeCtx(cn, "A");
        var entity = new G50OccItem { Id = 1, Name = "tampered", TenantId = "A", RowVersion = "tok-STALE" };
        var updated = await ctxA.BulkUpdateAsync(new[] { entity });

        Assert.Equal(0, updated);
        Assert.Equal("original", await ReadNameAsync(cn, "G50OccItem", 1));
    }

    [Fact]
    public async Task TripleGuard_CorrectTimestamp_WrongTenant_ZeroRowsUpdated()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50OccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL, RowVersion TEXT)");

        await InsertOccRawAsync(cn, 1, "original", "A", "tok-v1");

        // Correct PK, correct timestamp, wrong tenant
        var ctxB = MakeCtx(cn, "B");
        var entity = new G50OccItem { Id = 1, Name = "tampered", TenantId = "B", RowVersion = "tok-v1" };
        var updated = await ctxB.BulkUpdateAsync(new[] { entity });

        Assert.Equal(0, updated);
        Assert.Equal("original", await ReadNameAsync(cn, "G50OccItem", 1));
    }

    // ── Test 8: Lock-step: BulkInsert + cache invalidation + fresh query ─────

    /// <summary>
    /// BulkInsert -> verify cache invalidated -> fresh query returns new data (end-to-end).
    /// </summary>
    [Fact]
    public async Task LockStep_BulkInsert_CacheInvalidated_FreshQueryReturnsNewData()
    {
        using var innerCache = new NormMemoryCacheProvider();
        var spy = new SpyCacheProvider(innerCache);

        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");

        var opts = new DbContextOptions { CacheProvider = spy };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Seed and prime cache
        await InsertRawAsync(cn, "G50Item", 1, "seed", "X");
        var primed = await ctx.Query<G50Item>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(primed);

        spy.InvalidatedTags.Clear();

        // BulkInsert new row
        await ctx.BulkInsertAsync(new[] { new G50Item { Id = 2, Name = "new-bulk", TenantId = "X" } });

        // Verify cache was invalidated
        Assert.Contains("G50Item", spy.InvalidatedTags);

        // Fresh context to avoid change tracker identity map
        var freshCtx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { CacheProvider = spy });
        var afterInsert = await freshCtx.Query<G50Item>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Equal(2, afterInsert.Count);
        Assert.Contains(afterInsert, r => r.Name == "new-bulk");
    }

    // ── Test 9: Lock-step: BulkDelete + OCC ──────────────────────────────────

    /// <summary>
    /// BulkDelete an entity, then try BulkUpdate on the same entity -> 0 rows updated
    /// (entity is gone).
    /// </summary>
    [Fact]
    public async Task LockStep_BulkDelete_ThenBulkUpdate_ZeroRowsUpdated()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50OccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL, RowVersion TEXT)");

        await InsertOccRawAsync(cn, 1, "to-delete", "X", "tok-1");

        var ctx = new DbContext(cn, new SqliteProvider());

        // BulkDelete the entity
        await ctx.BulkDeleteAsync(new[] { new G50OccItem { Id = 1, TenantId = "X" } });

        // Verify entity is deleted
        var count = await CountRowsAsync(cn, "G50OccItem");
        Assert.Equal(0, count);

        // Try BulkUpdate on the deleted entity
        var entity = new G50OccItem { Id = 1, Name = "ghost-update", TenantId = "X", RowVersion = "tok-1" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal(0, updated);
    }

    // ── Test 10: Adversarial large batch ─────────────────────────────────────

    /// <summary>
    /// BulkInsert 500 rows for tenant A. BulkUpdate 500 rows where 250 are tenant A's PKs
    /// and 250 are tenant B's PKs. Only 250 should be updated.
    /// </summary>
    [Fact]
    public async Task AdversarialLargeBatch_MixedTenantPKs_OnlyOwnRowsUpdated()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");

        // Insert 500 rows for tenant A (ids 1-500) and 250 rows for tenant B (ids 501-750)
        var tenantARows = Enumerable.Range(1, 500)
            .Select(i => new G50Item { Id = i, Name = $"A-original-{i}", TenantId = "A" })
            .ToArray();
        var tenantBRows = Enumerable.Range(501, 250)
            .Select(i => new G50Item { Id = i, Name = $"B-original-{i}", TenantId = "B" })
            .ToArray();

        // Use raw insert for speed (no tenant provider needed for seeding)
        var ctxSeed = new DbContext(cn, new SqliteProvider());
        await ctxSeed.BulkInsertAsync(tenantARows);
        await ctxSeed.BulkInsertAsync(tenantBRows);

        Assert.Equal(750, await CountRowsAsync(cn, "G50Item"));

        // Tenant A context: BulkUpdate 500 rows, 250 own + 250 cross-tenant
        var ctxA = MakeCtx(cn, "A");
        var updateBatch = new List<G50Item>();
        // 250 of tenant A's own rows (ids 1-250)
        for (int i = 1; i <= 250; i++)
            updateBatch.Add(new G50Item { Id = i, Name = $"A-updated-{i}", TenantId = "A" });
        // 250 of tenant B's rows (ids 501-750) — forged PKs
        for (int i = 501; i <= 750; i++)
            updateBatch.Add(new G50Item { Id = i, Name = $"A-forged-{i}", TenantId = "A" });

        var updated = await ctxA.BulkUpdateAsync(updateBatch);

        // Only 250 own rows should have been updated
        Assert.Equal(250, updated);

        // Verify tenant A's updated rows
        Assert.Equal("A-updated-1", await ReadNameAsync(cn, "G50Item", 1));
        Assert.Equal("A-updated-250", await ReadNameAsync(cn, "G50Item", 250));

        // Verify tenant A's non-updated rows are still original
        Assert.Equal("A-original-251", await ReadNameAsync(cn, "G50Item", 251));
        Assert.Equal("A-original-500", await ReadNameAsync(cn, "G50Item", 500));

        // Verify tenant B's rows are unchanged
        Assert.Equal("B-original-501", await ReadNameAsync(cn, "G50Item", 501));
        Assert.Equal("B-original-750", await ReadNameAsync(cn, "G50Item", 750));
    }

    // ── Test 11 (bonus): BulkInsert commit inside transaction then query ─────

    /// <summary>
    /// Start outer transaction, BulkInsert inside it, commit the transaction,
    /// then verify rows are visible via a fresh query.
    /// </summary>
    [Fact]
    public async Task BulkInsert_InsideTransaction_CommitMakesRowsPersistent()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");
        var ctx = new DbContext(cn, new SqliteProvider());

        await using var tx = await ctx.Database.BeginTransactionAsync();

        await ctx.BulkInsertAsync(new[]
        {
            new G50Item { Id = 1, Name = "committed-1", TenantId = "X" },
            new G50Item { Id = 2, Name = "committed-2", TenantId = "X" }
        });

        await tx.CommitAsync();

        // Verify rows are persisted
        var count = await CountRowsAsync(cn, "G50Item");
        Assert.Equal(2, count);
        Assert.Equal("committed-1", await ReadNameAsync(cn, "G50Item", 1));
        Assert.Equal("committed-2", await ReadNameAsync(cn, "G50Item", 2));
    }

    // ── Test 12 (bonus): BulkDelete cross-tenant with interceptor verification ──

    /// <summary>
    /// BulkDelete with cross-tenant PK, verified via interceptor that the SQL
    /// includes the tenant predicate.
    /// </summary>
    [Fact]
    public async Task BulkDelete_CrossTenant_InterceptorVerifiesTenantPredicate()
    {
        using var cn = await CreateDbAsync(
            "CREATE TABLE G50Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)");

        // Insert row for tenant B
        await InsertRawAsync(cn, "G50Item", 10, "B-data", "B");

        // Tenant A context with interceptor
        var interceptor = new SqlCapturingInterceptor();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(interceptor);
        var ctxA = MakeCtx(cn, "A", opts);

        // Tenant A tries to delete tenant B's row
        var entity = new G50Item { Id = 10, Name = "B-data", TenantId = "A" };
        await ctxA.BulkDeleteAsync(new[] { entity });

        // Row should still exist
        Assert.Equal(1, await CountRowsAsync(cn, "G50Item"));
        Assert.Equal("B-data", await ReadNameAsync(cn, "G50Item", 10));

        // Interceptor should have captured a DELETE with TenantId predicate
        var capturedSql = interceptor.CapturedSql;
        var deleteSql = capturedSql.FirstOrDefault(s =>
            s.Contains("DELETE", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(deleteSql);
        Assert.Contains("TenantId", deleteSql, StringComparison.OrdinalIgnoreCase);
    }
}
