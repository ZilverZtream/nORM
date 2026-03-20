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
// Section 8 — Gate 4.5 -> 5.0: Adversarial Enterprise Tests
//
// Adversarial tenant-isolation suites (SQLite live + provider shape), fuzzing of
// expression trees, compiled query / materializer cache stress, and transaction
// lifecycle coverage.
// ══════════════════════════════════════════════════════════════════════════════

// -- Entity types (file-scoped to avoid name collision) -----------------------

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

file sealed class AegTenantProvider : ITenantProvider
{
    private readonly string _id;
    public AegTenantProvider(string id) => _id = id;
    public object GetCurrentTenantId() => _id;
}

// -- Test class ---------------------------------------------------------------

public class AdversarialMultiShapeStressTests
{
    // -- Helpers ---------------------------------------------------------------

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

    private static async Task<long> ScalarLongAsync(SqliteConnection cn, string sql)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task InsertRawAsync(
        SqliteConnection cn, string table, string cols, string vals,
        params (string name, object value)[] prms)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {table} ({cols}) VALUES ({vals})";
        foreach (var (name, value) in prms)
            cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static DbContext MakeTenantCtx(
        SqliteConnection cn, string tenantId, DbContextOptions? opts = null)
    {
        opts ??= new DbContextOptions();
        opts.TenantProvider = new AegTenantProvider(tenantId);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static DbContext MakeCtx(SqliteConnection cn, DbContextOptions? opts = null)
    {
        return new DbContext(cn, new SqliteProvider(), opts ?? new DbContextOptions(),
            ownsConnection: false);
    }

    private static DbContext MakeTenantCtxWithInclude(SqliteConnection cn, string tenantId)
    {
        var opts = new DbContextOptions
        {
            TenantProvider = new AegTenantProvider(tenantId),
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
            TenantProvider = new AegTenantProvider(tenantId),
            TenantColumnName = "TenantId",
            OnModelCreating = mb => mb.Entity<AegOwner>()
                .OwnsMany<AegOwned>(o => o.Items, tableName: "AEG_Owned", foreignKey: "OwnerId")
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // =========================================================================
    // Test 1: Comprehensive adversarial tenant isolation -- all shapes
    // =========================================================================

    /// <summary>
    /// Two tenants (A and B) with overlapping PKs. Verifies no cross-contamination
    /// across Insert, Update (BulkUpdate), Delete (BulkDelete), Query, Include,
    /// and Owned collection loading.
    /// </summary>
    [Fact]
    public async Task ComprehensiveTenantIsolation_AllShapes_NoCrossContamination()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"
            CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE AEG_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE AEG_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ParentId INTEGER NOT NULL, Data TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE AEG_Owner (Id INTEGER PRIMARY KEY,
                TenantId TEXT NOT NULL, Name TEXT NOT NULL);
            CREATE TABLE AEG_Owned (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                OwnerId INTEGER NOT NULL, Payload TEXT NOT NULL, TenantId TEXT NOT NULL);
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
            var forged = new AegItem { Id = 1, Name = "forged-by-B", TenantId = "B", Value = 999 };
            var updated = await ctxB.BulkUpdateAsync(new[] { forged });
            Assert.Equal(0, updated);
        }
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
            ("@oid", 1), ("@p", "ownedB"), ("@t", "B"));

        using (var ctxA = MakeTenantCtxWithOwned(cn, "A"))
        {
            var owners = await ctxA.Query<AegOwner>().ToListAsync();
            Assert.Single(owners);
            Assert.Equal("ownerA", owners[0].Name);
            Assert.All(owners[0].Items, i => Assert.Equal("A", i.TenantId));
        }
    }

    // =========================================================================
    // Test 2: SQL injection via tenant ID
    // =========================================================================

    [Theory]
    [InlineData("'; DROP TABLE AEG_Item; --")]
    [InlineData("' OR '1'='1")]
    [InlineData("UNION SELECT * FROM AEG_Item")]
    [InlineData("tenant\0null")]
    public async Task SqlInjection_ViaTenantId_Parameterized_NoInjection(string adversarialId)
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "safe-row"), ("@t", "safe"), ("@v", 1));

        // Seed row for adversarial tenant via parameterized insert (bypassing nORM)
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "adversarial-row"), ("@t", adversarialId), ("@v", 99));

        using var advCtx = MakeTenantCtx(cn, adversarialId);
        var result = await advCtx.Query<AegItem>().ToListAsync();

        Assert.All(result, r => Assert.Equal(adversarialId, r.TenantId));
        Assert.DoesNotContain(result, r => r.TenantId == "safe");

        // Table must still exist
        var count = await ScalarLongAsync(cn, "SELECT COUNT(*) FROM AEG_Item");
        Assert.True(count >= 1);
    }

    // =========================================================================
    // Test 3: Expression tree fuzzing -- 50 random Where predicates
    // =========================================================================

    /// <summary>
    /// Generates 50 random Where predicates with different operators. Executes
    /// each through nORM. Verifies no crash, tenant isolation, and that results
    /// satisfy the predicate used.
    /// </summary>
    [Fact]
    public async Task ExpressionTreeFuzzing_50RandomPredicates_NoCrashAndCorrectResults()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        for (int i = 0; i < 30; i++)
        {
            await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                ("@n", $"item{i}"), ("@t", "fuzz"), ("@v", i));
        }

        var rng = new Random(42);

        for (int trial = 0; trial < 50; trial++)
        {
            using var ctx = MakeTenantCtx(cn, "fuzz");
            int predicateType = rng.Next(8);
            int threshold = rng.Next(30);
            List<AegItem> normResult;

            switch (predicateType)
            {
                case 0:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value > threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.True(r.Value > threshold));
                    break;
                case 1:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value < threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.True(r.Value < threshold));
                    break;
                case 2:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value == threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.Equal(threshold, r.Value));
                    break;
                case 3:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value >= threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.True(r.Value >= threshold));
                    break;
                case 4:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value <= threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.True(r.Value <= threshold));
                    break;
                case 5:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value != threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.NotEqual(threshold, r.Value));
                    break;
                case 6:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value > 0 && x.Value < threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.True(r.Value > 0 && r.Value < threshold));
                    break;
                default:
                    normResult = await ctx.Query<AegItem>()
                        .Where(x => x.Value == 0 || x.Value == threshold).ToListAsync();
                    Assert.All(normResult, r => Assert.True(r.Value == 0 || r.Value == threshold));
                    break;
            }

            Assert.NotNull(normResult);
            Assert.All(normResult, r => Assert.Equal("fuzz", r.TenantId));
        }
    }

    // =========================================================================
    // Test 4: Source-gen materializer fuzzing -- 20 table names, same type
    // =========================================================================

    /// <summary>
    /// Creates 20 tables with the same schema, queries each via raw SQL using
    /// QueryUnchangedAsync. Verifies correct data with no stale materializer.
    /// </summary>
    [Fact]
    public async Task MaterializerFuzzing_20Tables_SameShape_CorrectData()
    {
        using var cn = OpenMemoryDb();
        const int tableCount = 20;
        const int rowsPerTable = 5;

        for (int t = 0; t < tableCount; t++)
        {
            ExecSql(cn, $"CREATE TABLE AEG_Dyn{t} (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)");
            for (int r = 0; r < rowsPerTable; r++)
            {
                await InsertRawAsync(cn, $"AEG_Dyn{t}", "Name", "@n",
                    ("@n", $"table{t}_row{r}"));
            }
        }

        using var ctx = MakeCtx(cn);

        for (int t = 0; t < tableCount; t++)
        {
            var results = await ctx.QueryUnchangedAsync<AegTypeA>(
                $"SELECT Id, Name AS Tag FROM AEG_Dyn{t} ORDER BY Id");

            Assert.Equal(rowsPerTable, results.Count);
            for (int r = 0; r < rowsPerTable; r++)
            {
                Assert.Equal($"table{t}_row{r}", results[r].Tag);
            }
        }
    }

    // =========================================================================
    // Test 5: Compiled query cache stress -- 100 queries x 10 executions
    // =========================================================================

    /// <summary>
    /// Single compiled query executed 1000 times (100 threshold values x 10
    /// repetitions). Verifies all executions return correct results with no
    /// cache poisoning.
    /// </summary>
    [Fact]
    public async Task CompiledQueryCacheStress_100Thresholds_10Reps_NoPoison()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        for (int i = 0; i < 50; i++)
        {
            await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                ("@n", $"cq{i}"), ("@t", "stress"), ("@v", i));
        }

        var compiled = Norm.CompileQuery((DbContext ctx, int minValue) =>
            ctx.Query<AegItem>().Where(x => x.Value >= minValue));

        for (int q = 0; q < 100; q++)
        {
            int threshold = q % 50;
            for (int exec = 0; exec < 10; exec++)
            {
                using var ctx = MakeTenantCtx(cn, "stress");
                var results = await compiled(ctx, threshold);

                Assert.All(results, r => Assert.True(r.Value >= threshold,
                    $"Query threshold={threshold}, exec={exec}: got Value={r.Value}"));

                var expectedCount = 50 - threshold;
                Assert.Equal(expectedCount, results.Count);
            }
        }
    }

    // =========================================================================
    // Test 6: Transaction lifecycle stress -- savepoints
    // =========================================================================

    /// <summary>
    /// 50 iterations of begin -> savepoint -> insert -> rollback-to-savepoint
    /// -> insert -> commit. Only the post-rollback row survives each iteration.
    /// </summary>
    [Fact]
    public async Task TransactionLifecycleStress_50Iterations_SavepointRollbackCommit()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_TxRow (Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Phase TEXT NOT NULL, SeqNo INTEGER NOT NULL)");
        using var ctx = MakeCtx(cn);

        for (int i = 0; i < 50; i++)
        {
            await using var tx = await ctx.Database.BeginTransactionAsync();

            await ctx.CreateSavepointAsync(tx.Transaction!, $"sp_{i}");
            await ctx.InsertAsync(new AegTxRow { Phase = "rollback", SeqNo = i });

            await ctx.RollbackToSavepointAsync(tx.Transaction!, $"sp_{i}");
            await ctx.InsertAsync(new AegTxRow { Phase = "commit", SeqNo = i });

            await tx.CommitAsync();
        }

        var committedCount = await ScalarLongAsync(cn,
            "SELECT COUNT(*) FROM AEG_TxRow WHERE Phase='commit'");
        var rolledBackCount = await ScalarLongAsync(cn,
            "SELECT COUNT(*) FROM AEG_TxRow WHERE Phase='rollback'");

        Assert.Equal(50L, committedCount);
        Assert.Equal(0L, rolledBackCount);

        for (int i = 0; i < 50; i++)
        {
            var exists = await ScalarLongAsync(cn,
                $"SELECT COUNT(*) FROM AEG_TxRow WHERE Phase='commit' AND SeqNo={i}");
            Assert.Equal(1L, exists);
        }
    }

    // =========================================================================
    // Test 7: Multi-tenant cache poisoning -- invalidation isolation
    // =========================================================================

    /// <summary>
    /// 5 tenants each prime a cache. One tenant invalidates its tag. Other
    /// tenants' caches remain intact.
    /// </summary>
    [Fact]
    public async Task MultiTenantCachePoisoning_5Tenants_InvalidationIsolated()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        const int tenantCount = 5;
        for (int t = 0; t < tenantCount; t++)
            for (int r = 0; r < 3; r++)
                await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                    ("@n", $"t{t}_r{r}"), ("@t", $"tenant{t}"), ("@v", t * 10 + r));

        // Per-tenant cache providers
        var caches = new Dictionary<string, NormMemoryCacheProvider>();
        for (int t = 0; t < tenantCount; t++)
        {
            var tid = $"tenant{t}";
            caches[tid] = new NormMemoryCacheProvider(() => tid);
        }

        // Prime caches
        for (int t = 0; t < tenantCount; t++)
        {
            var tid = $"tenant{t}";
            var opts = new DbContextOptions { CacheProvider = caches[tid] };
            using var ctx = MakeTenantCtx(cn, tid, opts);
            var rows = await ctx.Query<AegItem>()
                .Cacheable(TimeSpan.FromMinutes(10))
                .ToListAsync();
            Assert.Equal(3, rows.Count);
        }

        // Invalidate tenant0's cache
        caches["tenant0"].InvalidateTag("AEG_Item");

        // Insert new row for tenant0
        await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
            ("@n", "new-t0"), ("@t", "tenant0"), ("@v", 999));

        // Tenant0 re-query: should see 4 rows (cache was invalidated)
        {
            var opts0 = new DbContextOptions { CacheProvider = caches["tenant0"] };
            using var ctx0 = MakeTenantCtx(cn, "tenant0", opts0);
            var rows0 = await ctx0.Query<AegItem>()
                .Cacheable(TimeSpan.FromMinutes(10))
                .ToListAsync();
            Assert.Equal(4, rows0.Count);
        }

        // Other tenants still see 3 rows (cache not invalidated)
        for (int t = 1; t < tenantCount; t++)
        {
            var tid = $"tenant{t}";
            var opts = new DbContextOptions { CacheProvider = caches[tid] };
            using var ctx = MakeTenantCtx(cn, tid, opts);
            var rows = await ctx.Query<AegItem>()
                .Cacheable(TimeSpan.FromMinutes(10))
                .ToListAsync();
            Assert.Equal(3, rows.Count);
        }

        foreach (var c in caches.Values) c.Dispose();
    }

    // =========================================================================
    // Test 8: Shared-state safety under rapid dispose
    // =========================================================================

    /// <summary>
    /// Creates and disposes 100 DbContexts rapidly with queries on separate
    /// in-memory databases. Verifies no ObjectDisposedException from shared caches.
    /// </summary>
    [Fact]
    public async Task SharedStateSafety_100RapidDisposes_NoObjectDisposedException()
    {
        var exceptions = new ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, 100).Select(idx => Task.Run(async () =>
        {
            try
            {
                using var localCn = OpenMemoryDb();
                ExecSql(localCn, @"CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");
                for (int i = 0; i < 5; i++)
                    await InsertRawAsync(localCn, "AEG_Item", "Name, TenantId, Value",
                        "@n, @t, @v",
                        ("@n", $"local{idx}_{i}"), ("@t", "shared"), ("@v", i));

                using var ctx = MakeTenantCtx(localCn, "shared");
                var result = await ctx.Query<AegItem>().ToListAsync();
                Assert.True(result.Count > 0);
            }
            catch (ObjectDisposedException ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        Assert.Empty(exceptions);
    }

    // =========================================================================
    // Test 9: Concurrent tenant writes with OCC tokens
    // =========================================================================

    /// <summary>
    /// 10 tenants each doing sequential BulkUpdate with OCC tokens on their own
    /// rows. Verifies no cross-tenant OCC interference -- each tenant updates
    /// its own row and fails to update a foreign row.
    /// </summary>
    [Fact]
    public async Task ConcurrentTenantOCC_10Tenants_NoCrossTenantInterference()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_OccItem (Id INTEGER PRIMARY KEY,
            Name TEXT NOT NULL, TenantId TEXT NOT NULL, RowVersion TEXT)");

        const int tenantCount = 10;
        for (int t = 0; t < tenantCount; t++)
        {
            await InsertRawAsync(cn, "AEG_OccItem",
                "Id, Name, TenantId, RowVersion", "@id, @n, @t, @rv",
                ("@id", t + 1), ("@n", $"original-t{t}"),
                ("@t", $"occ-t{t}"), ("@rv", "tok-v1"));
        }

        var violations = new ConcurrentBag<string>();

        // Sequential to avoid SQLite concurrent-transaction issues on single connection
        for (int t = 0; t < tenantCount; t++)
        {
            var tid = $"occ-t{t}";

            // Own update: should succeed (1 row)
            using (var ownCtx = MakeTenantCtx(cn, tid))
            {
                var ownEntity = new AegOccItem
                {
                    Id = t + 1, Name = $"updated-t{t}",
                    TenantId = tid, RowVersion = "tok-v1"
                };
                var ownUpdated = await ownCtx.BulkUpdateAsync(new[] { ownEntity });
                if (ownUpdated != 1)
                    violations.Add(
                        $"Tenant {tid} failed own update (expected 1, got {ownUpdated})");
            }

            // Cross-tenant attempt: target tenant 0's row
            if (t != 0)
            {
                using var crossCtx = MakeTenantCtx(cn, tid);
                var crossEntity = new AegOccItem
                {
                    Id = 1, Name = "stolen",
                    TenantId = tid, RowVersion = "tok-v1"
                };
                var crossUpdated = await crossCtx.BulkUpdateAsync(new[] { crossEntity });
                if (crossUpdated != 0)
                    violations.Add(
                        $"Tenant {tid} modified t0's row (expected 0, got {crossUpdated})");
            }
        }

        Assert.Empty(violations);

        // Verify each row was updated by its own tenant
        for (int t = 0; t < tenantCount; t++)
        {
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT Name FROM AEG_OccItem WHERE Id=@id";
            cmd.Parameters.AddWithValue("@id", t + 1);
            var name = (string?)await cmd.ExecuteScalarAsync();
            Assert.Equal($"updated-t{t}", name);
        }
    }

    // =========================================================================
    // Test 10: Navigation loading under tenant isolation
    // =========================================================================

    /// <summary>
    /// Include() with overlapping FKs across tenants. Children from the wrong
    /// tenant must not leak into the Include result.
    /// </summary>
    [Fact]
    public async Task NavigationLoading_OverlappingFKs_TenantIsolated()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"
            CREATE TABLE AEG_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL, TenantId TEXT NOT NULL);
            CREATE TABLE AEG_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ParentId INTEGER NOT NULL, Data TEXT NOT NULL, TenantId TEXT NOT NULL);
        ");

        await InsertRawAsync(cn, "AEG_Parent", "Name, TenantId", "@n, @t",
            ("@n", "parentA"), ("@t", "A"));
        await InsertRawAsync(cn, "AEG_Parent", "Name, TenantId", "@n, @t",
            ("@n", "parentB"), ("@t", "B"));

        for (int i = 0; i < 5; i++)
            await InsertRawAsync(cn, "AEG_Child", "ParentId, Data, TenantId", "@pid, @d, @t",
                ("@pid", 1), ("@d", $"childA_{i}"), ("@t", "A"));

        // Children with same FK but different tenant
        for (int i = 0; i < 3; i++)
            await InsertRawAsync(cn, "AEG_Child", "ParentId, Data, TenantId", "@pid, @d, @t",
                ("@pid", 1), ("@d", $"childB_{i}"), ("@t", "B"));

        using var ctxA = MakeTenantCtxWithInclude(cn, "A");
        var parentsA = await ((INormQueryable<AegParent>)ctxA.Query<AegParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(parentsA);
        Assert.Equal(5, parentsA[0].Children.Count);
        Assert.All(parentsA[0].Children, c => Assert.Equal("A", c.TenantId));

        using var ctxB = MakeTenantCtxWithInclude(cn, "B");
        var parentsB = await ((INormQueryable<AegParent>)ctxB.Query<AegParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Single(parentsB);
        Assert.All(parentsB[0].Children, c => Assert.Equal("B", c.TenantId));
    }

    // =========================================================================
    // Test 11: BulkInsert + BulkDelete atomicity stress
    // =========================================================================

    /// <summary>
    /// 50 iterations of BulkInsert(100) then BulkDelete(100). After each
    /// pair, the batch rows must be gone.
    /// </summary>
    [Fact]
    public async Task BulkInsertDeleteAtomicity_50Iterations_AlwaysConsistent()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, "CREATE TABLE AEG_BulkRow (Id INTEGER PRIMARY KEY, Batch TEXT NOT NULL)");
        using var ctx = MakeCtx(cn);

        for (int iteration = 0; iteration < 50; iteration++)
        {
            var entities = Enumerable.Range(0, 100)
                .Select(r => new AegBulkRow
                {
                    Id = iteration * 100 + r,
                    Batch = $"batch{iteration}"
                })
                .ToArray();

            var inserted = await ctx.BulkInsertAsync(entities);
            Assert.Equal(100, inserted);

            var afterInsert = await ScalarLongAsync(cn,
                $"SELECT COUNT(*) FROM AEG_BulkRow WHERE Batch='batch{iteration}'");
            Assert.Equal(100L, afterInsert);

            var deleted = await ctx.BulkDeleteAsync(entities);
            Assert.Equal(100, deleted);

            var afterDelete = await ScalarLongAsync(cn,
                $"SELECT COUNT(*) FROM AEG_BulkRow WHERE Batch='batch{iteration}'");
            Assert.Equal(0L, afterDelete);
        }

        Assert.Equal(0L, await ScalarLongAsync(cn, "SELECT COUNT(*) FROM AEG_BulkRow"));
    }

    // =========================================================================
    // Test 12: Long-horizon materializer stability
    // =========================================================================

    /// <summary>
    /// Creates entities across 3 types, queries each type 100 times. Verifies
    /// all materializations are correct with no stale delegate reuse.
    /// </summary>
    [Fact]
    public async Task LongHorizonMaterializerStability_MultiType_NoStaleDelegate()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"
            CREATE TABLE AEG_TypeA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Tag TEXT NOT NULL);
            CREATE TABLE AEG_TypeB (Id INTEGER PRIMARY KEY AUTOINCREMENT, Score INTEGER NOT NULL);
            CREATE TABLE AEG_TypeC (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Label TEXT NOT NULL, Amount INTEGER NOT NULL);
        ");

        for (int i = 0; i < 100; i++)
        {
            await InsertRawAsync(cn, "AEG_TypeA", "Tag", "@t", ("@t", $"tagA_{i}"));
            await InsertRawAsync(cn, "AEG_TypeB", "Score", "@s", ("@s", i * 7));
            await InsertRawAsync(cn, "AEG_TypeC", "Label, Amount", "@l, @a",
                ("@l", $"labelC_{i}"), ("@a", i * 3));
        }

        for (int round = 0; round < 100; round++)
        {
            using var ctx = MakeCtx(cn);

            var typeAResults = await ctx.Query<AegTypeA>().ToListAsync();
            Assert.Equal(100, typeAResults.Count);
            Assert.Contains(typeAResults, r => r.Tag == $"tagA_{round}");

            var typeBResults = await ctx.Query<AegTypeB>().ToListAsync();
            Assert.Equal(100, typeBResults.Count);
            Assert.Contains(typeBResults, r => r.Score == round * 7);

            var typeCResults = await ctx.Query<AegTypeC>().ToListAsync();
            Assert.Equal(100, typeCResults.Count);
            Assert.Contains(typeCResults, r =>
                r.Label == $"labelC_{round}" && r.Amount == round * 3);
        }
    }

    // =========================================================================
    // Test 13 (bonus): Compiled query with tenant switch stress
    // =========================================================================

    /// <summary>
    /// Single compiled query cycled through 10 tenants 50 times each.
    /// Compiled query plan cache must be correctly tenant-scoped.
    /// </summary>
    [Fact]
    public async Task CompiledQueryTenantSwitch_10Tenants_50Cycles_NoLeakage()
    {
        using var cn = OpenMemoryDb();
        ExecSql(cn, @"CREATE TABLE AEG_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL, TenantId TEXT NOT NULL, Value INTEGER NOT NULL DEFAULT 0)");

        const int tenantCount = 10;
        const int rowsPerTenant = 5;

        for (int t = 0; t < tenantCount; t++)
            for (int r = 0; r < rowsPerTenant; r++)
                await InsertRawAsync(cn, "AEG_Item", "Name, TenantId, Value", "@n, @t, @v",
                    ("@n", $"t{t}_r{r}"), ("@t", $"ct{t}"), ("@v", t * 100 + r));

        var compiled = Norm.CompileQuery((DbContext ctx, int minValue) =>
            ctx.Query<AegItem>().Where(x => x.Value >= minValue));

        var violations = new ConcurrentBag<string>();

        for (int cycle = 0; cycle < 50; cycle++)
        {
            for (int t = 0; t < tenantCount; t++)
            {
                var tid = $"ct{t}";
                using var ctx = MakeTenantCtx(cn, tid);
                var results = await compiled(ctx, t * 100);

                foreach (var r in results)
                    if (r.TenantId != tid)
                        violations.Add(
                            $"Cycle {cycle}, tenant {tid}: " +
                            $"got row with TenantId={r.TenantId}");

                if (results.Count != rowsPerTenant)
                    violations.Add(
                        $"Cycle {cycle}, tenant {tid}: " +
                        $"expected {rowsPerTenant}, got {results.Count}");
            }
        }

        Assert.Empty(violations);
    }

    // =========================================================================
    // Test 14 (bonus): Concurrent multi-tenant cache + query stress
    // =========================================================================

    /// <summary>
    /// Parallel queries across 5 tenants with a shared NormMemoryCacheProvider.
    /// Each tenant has 10 rows. 20 parallel tasks query random tenants.
    /// Verifies no cross-contamination under contention.
    /// </summary>
    [Fact]
    public async Task ConcurrentMultiTenantCacheStress_NoCrossContamination()
    {
        var dbName = $"aeg_{Guid.NewGuid():N}";
        using var setupCn = new SqliteConnection(
            $"Data Source={dbName};Mode=Memory;Cache=Shared");
        setupCn.Open();
        ExecSql(setupCn, @"CREATE TABLE IF NOT EXISTS AEG_Item
            (Id INTEGER PRIMARY KEY AUTOINCREMENT,
             Name TEXT NOT NULL, TenantId TEXT NOT NULL,
             Value INTEGER NOT NULL DEFAULT 0)");

        const int tenantCount = 5;
        for (int t = 0; t < tenantCount; t++)
            for (int r = 0; r < 10; r++)
                await InsertRawAsync(setupCn, "AEG_Item", "Name, TenantId, Value",
                    "@n, @t, @v",
                    ("@n", $"mt{t}_r{r}"), ("@t", $"mt{t}"), ("@v", t * 100 + r));

        using var sharedCache = new NormMemoryCacheProvider(null);
        var violations = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, 20).Select(idx => Task.Run(async () =>
        {
            var tid = $"mt{idx % tenantCount}";
            using var localCn = new SqliteConnection(
                $"Data Source={dbName};Mode=Memory;Cache=Shared");
            localCn.Open();

            var opts = new DbContextOptions { CacheProvider = sharedCache };
            using var ctx = MakeTenantCtx(localCn, tid, opts);

            for (int i = 0; i < 10; i++)
            {
                var rows = await ctx.Query<AegItem>().ToListAsync();
                foreach (var row in rows)
                    if (row.TenantId != tid)
                        violations.Add(
                            $"Task {idx}: tenant {tid} got TenantId={row.TenantId}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        Assert.Empty(violations);
    }
}
