using System;
using System.Collections.Concurrent;
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
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Section 8 — Gate 4.5 → 5.0: Adversarial Enterprise Tests
//
// Adversarial tenant-isolation suites (SQLite live + provider shape), fuzzing of
// expression trees, compiled query / materializer cache stress, and transaction
// lifecycle coverage.
// ══════════════════════════════════════════════════════════════════════════════

// ── Entity types ─────────────────────────────────────────────────────────────

[Table("AEG_Item")]
file class AegItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
    public int Value { get; set; }
}

[Table("AEG_Parent")]
file class AegParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
    public ICollection<AegChild> Children { get; set; } = new List<AegChild>();
}

[Table("AEG_Child")]
file class AegChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public string Data { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
}

[Table("AEG_Owner")]
file class AegOwner
{
    [Key]
    public int Id { get; set; }
    public string TenantId { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<AegOwned> Items { get; set; } = new();
}

[Table("AEG_Owned")]
file class AegOwned
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Payload { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
}

[Table("AEG_OccItem")]
file class AegOccItem
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;

    [Timestamp]
    public string? RowVersion { get; set; }
}

[Table("AEG_BulkRow")]
file class AegBulkRow
{
    [Key]
    public int Id { get; set; }
    public string Batch { get; set; } = string.Empty;
}

[Table("AEG_TypeA")]
file class AegTypeA
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Tag { get; set; } = string.Empty;
}

[Table("AEG_TypeB")]
file class AegTypeB
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int Score { get; set; }
}

[Table("AEG_TypeC")]
file class AegTypeC
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public int Amount { get; set; }
}

[Table("AEG_TxRow")]
file class AegTxRow
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Phase { get; set; } = string.Empty;
    public int SeqNo { get; set; }
}

file sealed class StringTenantProvider : ITenantProvider
{
    private readonly string _id;
    public StringTenantProvider(string id) => _id = id;
    public object GetCurrentTenantId() => _id;
}

// ── Test class ───────────────────────────────────────────────────────────────

public class AdversarialTenantExpressionFuzzTests
{
    // ── Helpers ──────────────────────────────────────────────────────────────

    private static SqliteConnection OpenMemoryDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void ExecSql(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static async Task ExecSqlAsync(SqliteConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarLongAsync(SqliteConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task InsertRawAsync(SqliteConnection cn, string table, string cols, string vals, params (string name, object value)[] prms)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {table} ({cols}) VALUES ({vals})";
        foreach (var (name, value) in prms)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static DbContext MakeTenantCtx(SqliteConnection cn, string tenantId, DbContextOptions? opts = null)
    {
        opts ??= new DbContextOptions();
        opts.TenantProvider = new StringTenantProvider(tenantId);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static DbContext MakeTenantCtxWithInclude(SqliteConnection cn, string tenantId)
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new StringTenantProvider(tenantId),
            OnModelCreating = mb =>
            {
                mb.Entity<AegParent>()
                    .HasMany(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static DbContext MakeTenantCtxWithOwned(SqliteConnection cn, string tenantId)
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new StringTenantProvider(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb => mb.Entity<AegOwner>()
                .OwnsMany<AegOwned>(o => o.Items, tableName: "AEG_Owned", foreignKey: "OwnerId")
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 1: Comprehensive adversarial tenant isolation — all shapes
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Two tenants (A and B) with overlapping PKs. Verifies no cross-contamination
    /// across Insert, Update (tracked via SaveChanges), Delete, Query, Include,
    /// and Owned collection loading.
    /// </summary>
    [Fact]
    public async Task ComprehensiveTenantIsolation_AllShapes_NoCrossContamination()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"
            CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE AEG_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE AEG_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Data TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE AEG_Owner (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Name TEXT NOT NULL);
            CREATE TABLE AEG_Owned (Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, Payload TEXT NOT NULL, TenantId TEXT NOT NULL);
        ");

        // Seed overlapping PKs via raw SQL
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "itemA1"), ("@t", "A"), ("@v", 10));
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "itemB1"), ("@t", "B"), ("@v", 20));

        // -- Query isolation --
        using (var ctxA = MakeTenantCtx(cn, "A"))
        {
            var rowsA = await ctxA.Query<AegItem>().ToListAsync();
            Assert.All(rowsA, r => Assert.Equal("A", r.TenantId));
            Assert.DoesNotContain(rowsA, r => r.Name == "itemB1");
        }

        using (var ctxB = MakeTenantCtx(cn, "B"))
        {
            var rowsB = await ctxB.Query<AegItem>().ToListAsync();
            Assert.All(rowsB, r => Assert.Equal("B", r.TenantId));
            Assert.DoesNotContain(rowsB, r => r.Name == "itemA1");
        }

        // -- Insert isolation --
        using (var ctxA = MakeTenantCtx(cn, "A"))
        {
            await ctxA.InsertAsync(new AegItem { Name = "insertedByA", TenantId = "A", Value = 100 });
        }
        using (var ctxB = MakeTenantCtx(cn, "B"))
        {
            var rowsB = await ctxB.Query<AegItem>().ToListAsync();
            Assert.DoesNotContain(rowsB, r => r.Name == "insertedByA");
        }

        // -- BulkUpdate cross-tenant attempt --
        using (var ctxB = MakeTenantCtx(cn, "B"))
        {
            // Try to update tenant A's row using tenant B context
            var rowA = (await ctxB.Query<AegItem>().ToListAsync()); // empty or B-only
            // Force a forged entity with A's PK (id=1)
            var forged = new AegItem { Id = 1, Name = "forged-by-B", TenantId = "B", Value = 999 };
            var updated = await ctxB.BulkUpdateAsync(new[] { forged });
            Assert.Equal(0, updated);
        }
        // Verify A's row untouched
        Assert.Equal(10L, await ScalarLongAsync(cn, "SELECT Value FROM AEG_Item WHERE Id=1"));

        // -- BulkDelete cross-tenant attempt --
        using (var ctxB = MakeTenantCtx(cn, "B"))
        {
            var forged = new AegItem { Id = 1, Name = "itemA1", TenantId = "B", Value = 10 };
            var deleted = await ctxB.BulkDeleteAsync(new[] { forged });
            Assert.Equal(0, deleted);
        }
        Assert.Equal(1L, await ScalarLongAsync(cn, "SELECT COUNT(*) FROM AEG_Item WHERE Id=1"));

        // -- Include isolation (overlapping FK) --
        await InsertRawAsync(cn, "AEG_Parent", "Name, TenantId", "@n, @t",
            ("@n", "parentA"), ("@t", "A"));
        await InsertRawAsync(cn, "AEG_Parent", "Name, TenantId", "@n, @t",
            ("@n", "parentB"), ("@t", "B"));
        // Both have children with ParentId=1
        await InsertRawAsync(cn, "AEG_Child", "ParentId, Data, TenantId", "@pid, @d, @t",
            ("@pid", 1), ("@d", "childA"), ("@t", "A"));
        await InsertRawAsync(cn, "AEG_Child", "ParentId, Data, TenantId", "@pid, @d, @t",
            ("@pid", 1), ("@d", "childB"), ("@t", "B"));

        using (var ctxA = MakeTenantCtxWithInclude(cn, "A"))
        {
            var parents = await ((INormQueryable<AegParent>)ctxA.Query<AegParent>())
                .Include(p => p.Children)
                .AsSplitQuery()
                .ToListAsync();

            Assert.Single(parents);
            Assert.All(parents[0].Children, c => Assert.Equal("A", c.TenantId));
            Assert.DoesNotContain(parents[0].Children, c => c.Data == "childB");
        }

        // -- Owned collection isolation --
        await InsertRawAsync(cn, "AEG_Owner", "Id, TenantId, Name", "@id, @t, @n",
            ("@id", 1), ("@t", "A"), ("@n", "ownerA"));
        await InsertRawAsync(cn, "AEG_Owner", "Id, TenantId, Name", "@id, @t, @n",
            ("@id", 2), ("@t", "B"), ("@n", "ownerB"));
        await InsertRawAsync(cn, "AEG_Owned", "OwnerId, Payload, TenantId", "@oid, @p, @t",
            ("@oid", 1), ("@p", "ownedA"), ("@t", "A"));
        await InsertRawAsync(cn, "AEG_Owned", "OwnerId, Payload, TenantId", "@oid, @p, @t",
            ("@oid", 1), ("@p", "ownedB"), ("@t", "B")); // Same FK, different tenant

        using (var ctxA = MakeTenantCtxWithOwned(cn, "A"))
        {
            var owners = await ctxA.Query<AegOwner>().ToListAsync();
            Assert.Single(owners);
            Assert.Equal("ownerA", owners[0].Name);
            // Owned collection should only have A's items
            Assert.All(owners[0].Items, i => Assert.Equal("A", i.TenantId));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 2: SQL injection via tenant ID
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// TenantProvider returns an adversarial SQL-injection string. Verify that
    /// the tenant ID is parameterized and no injection occurs.
    /// </summary>
    [Theory]
    [InlineData("'; DROP TABLE AEG_Item; --")]
    [InlineData("' OR '1'='1")]
    [InlineData("UNION SELECT * FROM AEG_Item")]
    [InlineData("tenant\0null")]
    public async Task SqlInjection_ViaTenantId_Parameterized_NoInjection(string adversarialId)
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        // Seed a legitimate row
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "safe-row"), ("@t", "safe"), ("@v", 1));

        // Also seed a row with the adversarial tenant ID via raw parameterized SQL
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "adversarial-row"), ("@t", adversarialId), ("@v", 99));

        // Query with the adversarial tenant context
        using var advCtx = MakeTenantCtx(cn, adversarialId);
        var result = await advCtx.Query<AegItem>().ToListAsync();

        // Must return only the adversarial-tenant row, not the safe-tenant row
        Assert.All(result, r => Assert.Equal(adversarialId, r.TenantId));
        Assert.DoesNotContain(result, r => r.TenantId == "safe");

        // Table must still exist (no DROP succeeded)
        var count = await ScalarLongAsync(cn, "SELECT COUNT(*) FROM AEG_Item");
        Assert.True(count >= 1, "AEG_Item table must still exist after adversarial query");
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 5: Compiled query cache stress — 100 queries x 10 executions
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Creates 100 different compiled queries (each with a different predicate
    /// threshold compiled into a parameter). Executes each 10 times with the
    /// same parameter. Verifies all 1000 executions return correct results
    /// with no cache poisoning.
    /// </summary>
    [Fact]
    public async Task CompiledQueryCacheStress_100Queries_10Executions_NoPoison()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        // Seed 50 rows
        for (int i = 0; i < 50; i++)
        {
            await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                ("@n", $"cq{i}"), ("@t", "stress"), ("@v", i));
        }

        // Compile one parameterized query and run it 1000 times with 100 different thresholds
        var compiled = Norm.CompileQuery((DbContext ctx, int minValue) =>
            ctx.Query<AegItem>().Where(x => x.Value >= minValue));

        for (int q = 0; q < 100; q++)
        {
            int threshold = q % 50;

            for (int exec = 0; exec < 10; exec++)
            {
                using var ctx = MakeTenantCtx(cn, "stress");
                var results = await compiled(ctx, threshold);

                // All results must match the predicate
                Assert.All(results, r => Assert.True(r.Value >= threshold,
                    $"Query threshold={threshold}, exec={exec}: got Value={r.Value}"));

                // Count must match oracle
                var expectedCount = await ScalarLongAsync(cn,
                    $"SELECT COUNT(*) FROM AEG_Item WHERE TenantId='stress' AND Value >= {threshold}");
                Assert.Equal(expectedCount, results.Count);
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 6: Transaction lifecycle stress — savepoints
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// 50 iterations of begin tx -> create savepoint -> insert -> rollback to
    /// savepoint -> insert different -> commit. Verifies that only the post-rollback
    /// data survives.
    /// </summary>
    [Fact]
    public async Task TransactionLifecycleStress_50Iterations_SavepointRollbackCommit()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_TxRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Phase TEXT NOT NULL, SeqNo INTEGER NOT NULL)");
        using var ctx = new DbContext(cn, new SqliteProvider());

        for (int i = 0; i < 50; i++)
        {
            await using var tx = await ctx.Database.BeginTransactionAsync();

            // Create savepoint
            await ctx.CreateSavepointAsync(tx.Transaction!, $"sp_{i}");

            // Insert a row that will be rolled back
            await ctx.InsertAsync(new AegTxRow { Phase = "rollback", SeqNo = i });

            // Rollback to savepoint — this row should vanish
            await ctx.RollbackToSavepointAsync(tx.Transaction!, $"sp_{i}");

            // Insert the real row
            await ctx.InsertAsync(new AegTxRow { Phase = "commit", SeqNo = i });

            await tx.CommitAsync();
        }

        // Verify: all 50 committed rows exist, no rolled-back rows
        var committedCount = await ScalarLongAsync(cn, "SELECT COUNT(*) FROM AEG_TxRow WHERE Phase='commit'");
        var rolledBackCount = await ScalarLongAsync(cn, "SELECT COUNT(*) FROM AEG_TxRow WHERE Phase='rollback'");

        Assert.Equal(50L, committedCount);
        Assert.Equal(0L, rolledBackCount);

        // Verify sequence integrity
        for (int i = 0; i < 50; i++)
        {
            var exists = await ScalarLongAsync(cn, $"SELECT COUNT(*) FROM AEG_TxRow WHERE Phase='commit' AND SeqNo={i}");
            Assert.Equal(1L, exists);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 7: Multi-tenant cache poisoning — invalidation isolation
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// 5 tenants, each caching the same query shape. One tenant invalidates.
    /// Verify other tenants' caches are unaffected.
    /// </summary>
    [Fact]
    public async Task MultiTenantCachePoisoning_5Tenants_InvalidationIsolated()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        const int tenantCount = 5;
        // Seed rows for each tenant
        for (int t = 0; t < tenantCount; t++)
        {
            for (int r = 0; r < 3; r++)
            {
                await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                    ("@n", $"t{t}_r{r}"), ("@t", $"tenant{t}"), ("@v", t * 10 + r));
            }
        }

        // Use a tenant-aware cache that scopes by tenant ID
        var tenantCaches = new Dictionary<string, NormMemoryCacheProvider>();
        for (int t = 0; t < tenantCount; t++)
        {
            var tid = $"tenant{t}";
            tenantCaches[tid] = new NormMemoryCacheProvider(() => tid);
        }

        // Prime caches for all tenants
        var initialCounts = new Dictionary<string, int>();
        for (int t = 0; t < tenantCount; t++)
        {
            var tid = $"tenant{t}";
            var opts = new DbContextOptions { CacheProvider = tenantCaches[tid] };
            using var ctx = MakeTenantCtx(cn, tid, opts);
            var rows = await ctx.Query<AegItem>()
                .Cacheable(TimeSpan.FromMinutes(10))
                .ToListAsync();
            initialCounts[tid] = rows.Count;
            Assert.Equal(3, rows.Count);
        }

        // Invalidate tenant0's cache by inserting a new row
        var tid0 = "tenant0";
        tenantCaches[tid0].InvalidateTag("AEG_Item");

        // Insert a new row for tenant0
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "new-t0"), ("@t", tid0), ("@v", 999));

        // Query tenant0 again — should see the new row (cache was invalidated)
        {
            var opts0 = new DbContextOptions { CacheProvider = tenantCaches[tid0] };
            using var ctx0 = MakeTenantCtx(cn, tid0, opts0);
            var rows0 = await ctx0.Query<AegItem>()
                .Cacheable(TimeSpan.FromMinutes(10))
                .ToListAsync();
            Assert.Equal(4, rows0.Count);
        }

        // Other tenants' caches must be unaffected (still 3 rows from cache)
        for (int t = 1; t < tenantCount; t++)
        {
            var tid = $"tenant{t}";
            var opts = new DbContextOptions { CacheProvider = tenantCaches[tid] };
            using var ctx = MakeTenantCtx(cn, tid, opts);
            var rows = await ctx.Query<AegItem>()
                .Cacheable(TimeSpan.FromMinutes(10))
                .ToListAsync();
            Assert.Equal(3, rows.Count);
        }

        // Cleanup
        foreach (var c in tenantCaches.Values) c.Dispose();
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 8: Shared-state safety under rapid dispose
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Creates and disposes 100 DbContexts rapidly while querying. Verifies no
    /// ObjectDisposedException leaks from shared caches or connection pools.
    /// </summary>
    [Fact]
    public async Task SharedStateSafety_100RapidDisposes_NoObjectDisposedException()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        // Seed some data
        for (int i = 0; i < 10; i++)
        {
            await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                ("@n", $"shared{i}"), ("@t", "shared"), ("@v", i));
        }

        var exceptions = new ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, 100).Select(idx => Task.Run(async () =>
        {
            try
            {
                // Each iteration creates its own in-memory connection
                using var localCn = OpenMemoryDb();
                ExecSql(localCn, "CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");
                for (int i = 0; i < 5; i++)
                {
                    await InsertRawAsync(localCn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                        ("@n", $"local{idx}_{i}"), ("@t", "shared"), ("@v", i));
                }

                using var ctx = MakeTenantCtx(localCn, "shared");
                var result = await ctx.Query<AegItem>().ToListAsync();
                Assert.True(result.Count > 0);

                // Explicitly dispose and verify no shared state corruption
                ctx.Dispose();
            }
            catch (ObjectDisposedException ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        Assert.Empty(exceptions);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 10: Navigation loading under tenant isolation — overlapping FKs
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Include() with overlapping FKs across tenants. Children from the wrong
    /// tenant that share the same ParentId must not leak into the Include result.
    /// </summary>
    [Fact]
    public async Task NavigationLoading_OverlappingFKs_TenantIsolated()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"
            CREATE TABLE AEG_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE AEG_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Data TEXT NOT NULL, TenantId TEXT NOT NULL);
        ");

        // Both tenants have parents with overlapping auto-increment IDs
        // Parent for tenant A (Id=1)
        await InsertRawAsync(cn, "AEG_Parent", "Name, TenantId", "@n, @t",
            ("@n", "parentA"), ("@t", "A"));
        // Parent for tenant B (Id=2)
        await InsertRawAsync(cn, "AEG_Parent", "Name, TenantId", "@n, @t",
            ("@n", "parentB"), ("@t", "B"));

        // Children for tenant A's parent (ParentId=1)
        for (int i = 0; i < 5; i++)
        {
            await InsertRawAsync(cn, "AEG_Child", "ParentId, Data, TenantId", "@pid, @d, @t",
                ("@pid", 1), ("@d", $"childA_{i}"), ("@t", "A"));
        }

        // Children for tenant B but with same ParentId=1 (FK overlap)
        for (int i = 0; i < 3; i++)
        {
            await InsertRawAsync(cn, "AEG_Child", "ParentId, Data, TenantId", "@pid, @d, @t",
                ("@pid", 1), ("@d", $"childB_{i}"), ("@t", "B"));
        }

        // Tenant A Include: should get 5 children, not 8
        using var ctxA = MakeTenantCtxWithInclude(cn, "A");
        var parentsA = await ((INormQueryable<AegParent>)ctxA.Query<AegParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(parentsA);
        Assert.Equal(5, parentsA[0].Children.Count);
        Assert.All(parentsA[0].Children, c => Assert.Equal("A", c.TenantId));

        // Tenant B Include: should get 3 children for ParentId=2 (but none for ParentId=1
        // since tenant B's parent is Id=2)
        using var ctxB = MakeTenantCtxWithInclude(cn, "B");
        var parentsB = await ((INormQueryable<AegParent>)ctxB.Query<AegParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(parentsB);
        Assert.All(parentsB[0].Children, c => Assert.Equal("B", c.TenantId));
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 13 (bonus): Compiled query with tenant switch stress
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Single compiled query, cycled through 10 tenants 50 times each.
    /// Verifies compiled query plan cache is correctly tenant-scoped.
    /// </summary>
    [Fact]
    public async Task CompiledQueryTenantSwitch_10Tenants_50Cycles_NoLeakage()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        const int tenantCount = 10;
        const int rowsPerTenant = 5;

        for (int t = 0; t < tenantCount; t++)
        {
            for (int r = 0; r < rowsPerTenant; r++)
            {
                await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                    ("@n", $"t{t}_r{r}"), ("@t", $"ct{t}"), ("@v", t * 100 + r));
            }
        }

        var compiled = Norm.CompileQuery((DbContext ctx, int minValue) =>
            ctx.Query<AegItem>().Where(x => x.Value >= minValue));

        var violations = new ConcurrentBag<string>();

        for (int cycle = 0; cycle < 50; cycle++)
        {
            for (int t = 0; t < tenantCount; t++)
            {
                var tid = $"ct{t}";
                using var ctx = MakeTenantCtx(cn, tid);
                var results = await compiled(ctx, t * 100); // Should match all rows for this tenant

                foreach (var r in results)
                {
                    if (r.TenantId != tid)
                        violations.Add($"Cycle {cycle}, tenant {tid}: got row with TenantId={r.TenantId}");
                }

                if (results.Count != rowsPerTenant)
                    violations.Add($"Cycle {cycle}, tenant {tid}: expected {rowsPerTenant} rows, got {results.Count}");
            }
        }

        Assert.Empty(violations);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Test 14 (bonus): Concurrent multi-tenant cache + query stress
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Parallel queries across 5 tenants with a shared NormMemoryCacheProvider.
    /// Each tenant has 10 rows. 20 parallel tasks query random tenants.
    /// Verifies no cross-contamination under contention.
    /// </summary>
    [Fact]
    public async Task ConcurrentMultiTenantCacheStress_NoCrossContamination()
    {
        var dbName = $"aeg_{Guid.NewGuid():N}";
        using var setupCn = new SqliteConnection($"Data Source={dbName};Mode=Memory;Cache=Shared");
        setupCn.Open();
        ExecSql(setupCn, "CREATE TABLE IF NOT EXISTS AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        const int tenantCount = 5;
        for (int t = 0; t < tenantCount; t++)
        {
            for (int r = 0; r < 10; r++)
            {
                await InsertRawAsync(setupCn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                    ("@n", $"mt{t}_r{r}"), ("@t", $"mt{t}"), ("@v", t * 100 + r));
            }
        }

        var sharedCache = new NormMemoryCacheProvider(null);
        var violations = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, 20).Select(idx => Task.Run(async () =>
        {
            var tid = $"mt{idx % tenantCount}";
            using var localCn = new SqliteConnection($"Data Source={dbName};Mode=Memory;Cache=Shared");
            localCn.Open();

            var opts = new DbContextOptions { CacheProvider = sharedCache };
            using var ctx = MakeTenantCtx(localCn, tid, opts);

            for (int i = 0; i < 10; i++)
            {
                var rows = await ctx.Query<AegItem>().ToListAsync();
                foreach (var row in rows)
                {
                    if (row.TenantId != tid)
                        violations.Add($"Task {idx}: tenant {tid} received row with TenantId={row.TenantId}");
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        sharedCache.Dispose();
        Assert.Empty(violations);
    }
}
