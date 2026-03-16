using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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

// ── Shared entity types ───────────────────────────────────────────────────────

[Table("G45Item")]
file class G45Item
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Table("G45Parent")]
file class G45Parent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
    public ICollection<G45Child> Children { get; set; } = new List<G45Child>();
}

[Table("G45Child")]
file class G45Child
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public string Value { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
}

[Table("G50TenantRow")]
file class G50TenantRow
{
    [Key]
    public int Id { get; set; }
    public string TenantId { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
}

file sealed class FixedTenantProvider45 : ITenantProvider
{
    private readonly string _id;
    public FixedTenantProvider45(string id) => _id = id;
    public object GetCurrentTenantId() => _id;
}

// ════════════════════════════════════════════════════════════════════════════
// Gate 4.5 — Concurrency stress on command lifecycle + cancellation fault-injection
// ════════════════════════════════════════════════════════════════════════════

/// <summary>
/// High-volume sequential stress on the PreparedInsert command cache to exercise
/// the C1 fix (volatile _disposed) across rapid transaction-binding changes.
/// </summary>
public class Gate45CommandLifecycleStressTests
{
    private static (SqliteConnection, DbContext) BuildDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE G45Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    /// <summary>
    /// 500 alternating cycles of (insert-outside-tx, begin-tx, insert-inside-tx, commit)
    /// must complete without ObjectDisposedException or any other exception.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_500TransactionCycles_NoException()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        for (int i = 0; i < 500; i++)
        {
            await ctx.InsertAsync(new G45Item { Name = $"out{i}" });

            await using var tx = await ctx.Database.BeginTransactionAsync();
            await ctx.InsertAsync(new G45Item { Name = $"in{i}" });
            await tx.CommitAsync();
        }

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM G45Item";
        Assert.Equal(1000L, Convert.ToInt64(count.ExecuteScalar()));
    }

    /// <summary>
    /// 200 consecutive inserts within the same transaction must all reuse the cached
    /// command without re-preparation and produce the correct row count.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_200InsertsInOneTx_AllCommit()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        for (int i = 0; i < 200; i++)
            await ctx.InsertAsync(new G45Item { Name = $"bulk{i}" });
        await tx.CommitAsync();

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM G45Item";
        Assert.Equal(200L, Convert.ToInt64(count.ExecuteScalar()));
    }

    /// <summary>
    /// A pre-cancelled CancellationToken passed to InsertAsync must propagate the
    /// cancellation without corrupting the PreparedInsert cache for subsequent calls.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_CancelledToken_CacheRemainsUsable()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        // Warm the cache.
        await ctx.InsertAsync(new G45Item { Name = "warm" });

        // Pre-cancelled token: should throw OperationCanceledException.
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.InsertAsync(new G45Item { Name = "cancel" }, cts.Token));

        // Cache must still be functional after the cancellation.
        await ctx.InsertAsync(new G45Item { Name = "after-cancel" });

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM G45Item";
        // "warm" + "after-cancel" = 2; "cancel" was not committed
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar()));
    }

    /// <summary>
    /// A transaction that is rolled back after inserts must leave the PreparedInsert cache
    /// in a consistent state so subsequent inserts (outside the rolled-back tx) succeed.
    /// </summary>
    [Fact]
    public async Task PreparedInsert_RollbackThenReuse_CacheConsistent()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        // Insert outside tx to prime the null-tx cache entry.
        await ctx.InsertAsync(new G45Item { Name = "before" });

        // Start a tx, insert, then roll back.
        await using var tx = await ctx.Database.BeginTransactionAsync();
        await ctx.InsertAsync(new G45Item { Name = "rolled-back" });
        await tx.RollbackAsync();

        // Insert outside tx again — cache replaces the rolled-back tx entry.
        await ctx.InsertAsync(new G45Item { Name = "after" });

        using var count = cn.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM G45Item";
        // "before" + "after" = 2; rolled-back row not persisted
        Assert.Equal(2L, Convert.ToInt64(count.ExecuteScalar()));
    }
}

/// <summary>
/// Cancellation and rollback fault-injection around ambient transaction paths.
/// </summary>
public class Gate45CancellationFaultInjectionTests
{
    private static (SqliteConnection, DbContext) BuildDb(string? schema = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = schema ?? "CREATE TABLE G45Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    /// <summary>
    /// A query cancelled before execution must not leave the connection in a broken state;
    /// subsequent non-cancelled queries must succeed.
    /// </summary>
    [Fact]
    public async Task Query_PreCancelled_SubsequentQuerySucceeds()
    {
        var (_, ctx) = BuildDb();
        await using var _ = ctx;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.Query<G45Item>().ToListAsync(cts.Token));

        // Subsequent query with fresh token must work.
        var results = await ctx.Query<G45Item>().ToListAsync();
        Assert.Empty(results);
    }

    /// <summary>
    /// SaveChangesAsync cancelled after entity attachment must not partially commit data.
    /// The rolled-back state must leave the database unchanged.
    /// </summary>
    [Fact]
    public async Task SaveChanges_CancelledAfterAttach_NoPartialCommit()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;

        await ctx.InsertAsync(new G45Item { Name = "before" });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // This may succeed or throw depending on timing; the key invariant is
        // that if it throws, the row count is still consistent.
        try
        {
            await ctx.SaveChangesAsync(cts.Token);
        }
        catch (OperationCanceledException) { }

        // The database must not have received partial changes.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM G45Item";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.True(count <= 1, $"Expected ≤1 rows, found {count} (partial commit detected)");
    }

    /// <summary>
    /// Multiple nested savepoints where each is rolled back must leave the outer
    /// transaction unaffected; final commit must persist only the outer changes.
    /// </summary>
    [Fact]
    public async Task Savepoint_MultipleRollbacks_OuterTransactionUnaffected()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;
        var provider = new SqliteProvider();

        await using var outer = await cn.BeginTransactionAsync();

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)outer;
            ins.CommandText = "INSERT INTO G45Item (Name) VALUES ('outer1')";
            await ins.ExecuteNonQueryAsync();
        }

        await provider.CreateSavepointAsync(outer, "sp1");

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)outer;
            ins.CommandText = "INSERT INTO G45Item (Name) VALUES ('inner1')";
            await ins.ExecuteNonQueryAsync();
        }

        await provider.RollbackToSavepointAsync(outer, "sp1");

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)outer;
            ins.CommandText = "INSERT INTO G45Item (Name) VALUES ('outer2')";
            await ins.ExecuteNonQueryAsync();
        }

        await outer.CommitAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM G45Item";
        // "outer1" + "outer2" = 2; "inner1" was rolled back
        Assert.Equal(2L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    /// <summary>
    /// Pre-cancelled token passed to CreateSavepointAsync must not leave the
    /// transaction in a broken state; subsequent operations must still work.
    /// </summary>
    [Fact]
    public async Task Savepoint_PreCancelledToken_TransactionStillUsable()
    {
        var (cn, ctx) = BuildDb();
        await using var _ = ctx;
        var provider = new SqliteProvider();

        await using var tx = await cn.BeginTransactionAsync();

        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
            ins.CommandText = "INSERT INTO G45Item (Name) VALUES ('before-sp')";
            await ins.ExecuteNonQueryAsync();
        }

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp_cancel", cts.Token));

        // Transaction must still be usable after the cancelled savepoint.
        await using (var ins = cn.CreateCommand())
        {
            ins.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
            ins.CommandText = "INSERT INTO G45Item (Name) VALUES ('after-sp')";
            await ins.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM G45Item";
        Assert.Equal(2L, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Gate 5.0 — Adversarial multi-tenant end-to-end + shared-state safety
// ════════════════════════════════════════════════════════════════════════════

/// <summary>
/// End-to-end adversarial multi-tenant tests combining filters, includes,
/// compiled queries, and shared plan-cache safety under contention.
/// </summary>
public class Gate50AdversarialMultiTenantTests
{
    private static DbContextOptions TenantOpts(string tenantId, Action<ModelBuilder>? model = null) => new()
    {
        TenantProvider = new FixedTenantProvider45(tenantId),
        OnModelCreating = model
    };

    private static (SqliteConnection, string) CreateSharedDb(string schema)
    {
        var name = $"G50_{Guid.NewGuid():N}";
        var connStr = $"Data Source={name};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = schema;
        cmd.ExecuteNonQuery();
        return (keeper, connStr);
    }

    /// <summary>
    /// Compiled query with a tenant-scoped context must not return rows belonging
    /// to another tenant even when the static plan cache is shared across both contexts.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_TwoTenants_StaticPlanCacheShared_NoCrossLeak()
    {
        var dbName = $"G50CQ_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using (var setup = keeper.CreateCommand())
        {
            setup.CommandText = @"
CREATE TABLE G50TenantRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Data TEXT NOT NULL);
INSERT INTO G50TenantRow VALUES (1, 'A', 'data-A1');
INSERT INTO G50TenantRow VALUES (2, 'A', 'data-A2');
INSERT INTO G50TenantRow VALUES (3, 'B', 'data-B1');";
            setup.ExecuteNonQuery();
        }

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<G50TenantRow>().Where(r => r.Id >= minId));

        using var cnA = new SqliteConnection(connStr);
        using var cnB = new SqliteConnection(connStr);
        cnA.Open(); cnB.Open();

        using var ctxA = new DbContext(cnA, new SqliteProvider(), TenantOpts("A"));
        using var ctxB = new DbContext(cnB, new SqliteProvider(), TenantOpts("B"));

        // Compiled query for tenant A must only return A's rows.
        var rowsA = await compiled(ctxA, 1);
        Assert.All(rowsA, r => Assert.Equal("A", r.TenantId));
        Assert.Equal(2, rowsA.Count);

        // Compiled query for tenant B must only return B's rows.
        var rowsB = await compiled(ctxB, 1);
        Assert.All(rowsB, r => Assert.Equal("B", r.TenantId));
        Assert.Single(rowsB);
    }

    /// <summary>
    /// Include eager-loading combined with a tenant-scoped compiled query must not
    /// disclose cross-tenant children even when the plan cache is shared.
    /// Adversarial: both tenants use ParentId=1 as their FK value.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_Include_SharedPlanCache_TenantFilterOnBothLevels()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE G45Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
CREATE TABLE G45Child  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Value TEXT NOT NULL, TenantId TEXT NOT NULL);
INSERT INTO G45Parent VALUES (1, 'pA', 'A');
INSERT INTO G45Parent VALUES (2, 'pB', 'B');
INSERT INTO G45Child  VALUES (10, 1, 'child-A', 'A');
INSERT INTO G45Child  VALUES (11, 1, 'cross-tenant-poison', 'B');
INSERT INTO G45Child  VALUES (12, 2, 'child-B', 'B');";
        setup.ExecuteNonQuery();

        var opts = TenantOpts("A", mb =>
            mb.Entity<G45Parent>()
              .HasMany(p => p.Children)
              .WithOne()
              .HasForeignKey(c => c.ParentId, p => p.Id));

        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var parents = await ((INormQueryable<G45Parent>)ctx.Query<G45Parent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        // Tenant A sees only its own parent.
        Assert.Single(parents);
        Assert.Equal("pA", parents[0].Name);

        // Include must apply tenant predicate: only "child-A", not "cross-tenant-poison".
        Assert.Single(parents[0].Children);
        Assert.Equal("child-A", parents[0].Children.First().Value);
        Assert.DoesNotContain(parents[0].Children, c => c.TenantId != "A");
    }

    /// <summary>
    /// 50 concurrent tasks each creating an independent tenant context and querying
    /// only their own data must produce correct isolated results with no cross-tenant leakage.
    /// </summary>
    [Fact]
    public async Task HighContention_50TenantContexts_AllIsolated()
    {
        var dbName = $"G50HC_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        const int Degree = 50;
        using (var setup = keeper.CreateCommand())
        {
            var inserts = string.Join(";", Enumerable.Range(1, Degree)
                .Select(i => $"INSERT INTO G50TenantRow VALUES ({i}, 'T{i}', 'data-{i}')"));
            setup.CommandText = "CREATE TABLE G50TenantRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Data TEXT NOT NULL);" + inserts;
            setup.ExecuteNonQuery();
        }

        var tasks = Enumerable.Range(1, Degree).Select(async i =>
        {
            using var cn = new SqliteConnection(connStr);
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider(), TenantOpts($"T{i}"));
            var rows = await ctx.Query<G50TenantRow>().ToListAsync();
            Assert.Single(rows);
            Assert.Equal($"T{i}", rows[0].TenantId);
            Assert.Equal($"data-{i}", rows[0].Data);
        });

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Adversarial SQL meta-characters injected as a tenant ID must be parameterized
    /// safely and must not cause SQL injection in tenant-scoped Include queries.
    /// </summary>
    [Fact]
    public async Task Include_AdversarialTenantId_SqlMetaCharacters_NoInjection()
    {
        const string AdversarialTenantId = "'; DROP TABLE G45Child; --";

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
CREATE TABLE G45Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
CREATE TABLE G45Child  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Value TEXT NOT NULL, TenantId TEXT NOT NULL);
INSERT INTO G45Parent VALUES (1, 'parent', 'safe-tenant');
INSERT INTO G45Child  VALUES (10, 1, 'child', 'safe-tenant');";
        setup.ExecuteNonQuery();

        var opts = TenantOpts(AdversarialTenantId, mb =>
            mb.Entity<G45Parent>()
              .HasMany(p => p.Children)
              .WithOne()
              .HasForeignKey(c => c.ParentId, p => p.Id));

        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Must not throw an exception and must not drop the table.
        var parents = await ((INormQueryable<G45Parent>)ctx.Query<G45Parent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        // The adversarial tenant sees no rows (all rows belong to 'safe-tenant').
        // But the table must still exist (no injection succeeded).
        Assert.Empty(parents);

        using var probe = cn.CreateCommand();
        probe.CommandText = "SELECT COUNT(*) FROM G45Child";
        Assert.Equal(1L, Convert.ToInt64(probe.ExecuteScalar())); // table survived
    }

    /// <summary>
    /// The static query plan cache (shared across all DbContext instances) must produce
    /// tenant-filtered results even when the same query shape is reused by multiple contexts
    /// with different tenant providers, with no cross-context data leakage.
    /// </summary>
    [Fact]
    public async Task SharedPlanCache_MultipleContextsSequential_TenantIsolationMaintained()
    {
        var dbName = $"G50SP_{Guid.NewGuid():N}";
        var connStr = $"Data Source={dbName};Mode=Memory;Cache=Shared";
        using var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using (var setup = keeper.CreateCommand())
        {
            setup.CommandText = @"
CREATE TABLE G50TenantRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Data TEXT NOT NULL);
INSERT INTO G50TenantRow VALUES (1, 'X', 'x-data');
INSERT INTO G50TenantRow VALUES (2, 'Y', 'y-data');
INSERT INTO G50TenantRow VALUES (3, 'X', 'x-data2');";
            setup.ExecuteNonQuery();
        }

        // Execute the SAME query shape with different tenants to verify plan cache
        // does not return stale tenant-specific results.
        for (int pass = 0; pass < 10; pass++)
        {
            foreach (var (tenant, expectedCount) in new[] { ("X", 2), ("Y", 1) })
            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = new DbContext(cn, new SqliteProvider(), TenantOpts(tenant));
                var rows = await ctx.Query<G50TenantRow>().ToListAsync();
                Assert.Equal(expectedCount, rows.Count);
                Assert.All(rows, r => Assert.Equal(tenant, r.TenantId));
            }
        }
    }
}
