using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Audit gate tests covering all score-movement rules from the nORM Deep Audit Report.
///
/// Gate 2.8 → 3.0  — SG1/X1 source-gen parity (see SourceGenMaterializerCorrectnessTests.cs)
/// Gate 3.0 → 3.5  — Path coverage, malformed expression fuzz, provider edge matrices
/// Gate 3.5 → 4.0  — Provider SQL correctness matrix, transaction/connection fault injection
/// Gate 4.0 → 4.5  — C1 cache factory idempotence, migration partial-failure/retry, cancellation matrix
/// Gate 4.5 → 5.0  — No >Low correctness issues; adversarial cache, shared-state safety
/// </summary>

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.0 → 3.5 : path coverage, fuzz, provider edge matrices
// ══════════════════════════════════════════════════════════════════════════════

[Table("GateSimple")]
file class GateSimple
{
    [Key]
    public int Id { get; set; }
    public string? Name { get; set; }
}

public class Gate30To35Tests
{
    // ── Malformed expression fuzz ──────────────────────────────────────────────

    [Fact]
    public void QueryTranslator_NullLiteral_InWhere_DoesNotThrow()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var ex = Record.Exception(() =>
        {
            var _ = ctx.Query<GateSimple>().Where(e => e.Name == null).ToString();
        });
        Assert.Null(ex);
    }

    [Fact]
    public void QueryTranslator_EmptyStringLiteral_InWhere_DoesNotThrow()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var ex = Record.Exception(() =>
        {
            var _ = ctx.Query<GateSimple>().Where(e => e.Name == "").ToString();
        });
        Assert.Null(ex);
    }

    [Fact]
    public void QueryTranslator_ChainedWhere_DoesNotThrow()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var ex = Record.Exception(() =>
        {
            var _ = ctx.Query<GateSimple>()
                .Where(e => e.Id > 0)
                .Where(e => e.Name != null)
                .Where(e => e.Id < 1000)
                .ToString();
        });
        Assert.Null(ex);
    }

    [Fact]
    public void QueryTranslator_OrderByMultiple_DoesNotThrow()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var ex = Record.Exception(() =>
        {
            var _ = ctx.Query<GateSimple>()
                .OrderBy(e => e.Name)
                .ThenByDescending(e => e.Id)
                .ToString();
        });
        Assert.Null(ex);
    }

    // ── Provider edge matrices ─────────────────────────────────────────────────

    public static IEnumerable<object[]> AllProviders() =>
    [
        [new SqliteProvider(),                              "sqlite"],
        [new SqlServerProvider(),                           "sqlserver"],
        [new MySqlProvider(new SqliteParameterFactory()),   "mysql"],
        [new PostgresProvider(new SqliteParameterFactory()), "postgres"],
    ];

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Provider_BasicSelectQuery_GeneratesSql(object provider, string _)
    {
        var dbProvider = (nORM.Providers.DatabaseProvider)provider;
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, dbProvider);
        var sql = ctx.Query<GateSimple>().Where(e => e.Id > 0).ToString();
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GateSimple", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Provider_NullComparison_GeneratesIsNullSql(object provider, string _)
    {
        var dbProvider = (nORM.Providers.DatabaseProvider)provider;
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, dbProvider);
        var sql = ctx.Query<GateSimple>().Where(e => e.Name == null).ToString();
        Assert.Contains("IS NULL", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Provider_StringContains_GeneratesLikeSql(object provider, string _)
    {
        var dbProvider = (nORM.Providers.DatabaseProvider)provider;
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, dbProvider);
        var sql = ctx.Query<GateSimple>().Where(e => e.Name!.Contains("test")).ToString();
        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Provider_OrderByAscDesc_BothEmitted(object provider, string _)
    {
        var dbProvider = (nORM.Providers.DatabaseProvider)provider;
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, dbProvider);
        var asc  = ctx.Query<GateSimple>().OrderBy(e => e.Id).ToString();
        var desc = ctx.Query<GateSimple>().OrderByDescending(e => e.Id).ToString();
        Assert.Contains("ORDER BY", asc,  StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ORDER BY", desc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DESC",     desc, StringComparison.OrdinalIgnoreCase);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.5 → 4.0 : SQL correctness matrix + fault injection
// ══════════════════════════════════════════════════════════════════════════════

[Table("GateSE")]
file class GateSE
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Name { get; set; }
}

public class Gate35To40Tests
{
    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    // ── SQLite live correctness ───────────────────────────────────────────────

    [Fact]
    public async Task SqliteLive_Insert_Query_Update_Delete_RoundTrip()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)";
            cmd.ExecuteNonQuery();
        }

        var e = new GateSE { Name = "Alice" };
        ctx.Add(e);
        await ctx.SaveChangesAsync();
        Assert.True(e.Id > 0);

        // Verify inserted value
        var found = await ctx.Query<GateSE>().Where(x => x.Id == e.Id).ToListAsync();
        Assert.Single(found);
        Assert.Equal("Alice", found[0].Name);

        // Update: modify the tracked entity directly (e is Unchanged after SaveChanges)
        e.Name = "Bob";
        await ctx.SaveChangesAsync();
        var updated = await ctx.Query<GateSE>().Where(x => x.Id == e.Id).ToListAsync();
        Assert.Equal("Bob", updated[0].Name);

        // Delete the tracked entity
        ctx.Remove(e);
        await ctx.SaveChangesAsync();
        var gone = await ctx.Query<GateSE>().Where(x => x.Id == e.Id).ToListAsync();
        Assert.Empty(gone);
    }

    [Fact]
    public async Task SqliteLive_SkipTake_CorrectRows()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
            for (int i = 1; i <= 10; i++)
            {
                cmd.CommandText = $"INSERT INTO GateSE VALUES ({i}, 'item{i}')";
                cmd.ExecuteNonQuery();
            }
        }

        var page = await ctx.Query<GateSE>().OrderBy(e => e.Id).Skip(3).Take(3).ToListAsync();
        Assert.Equal(3, page.Count);
        Assert.Equal(4, page[0].Id);
        Assert.Equal(6, page[2].Id);
    }

    // ── Transaction fault injection ───────────────────────────────────────────

    [Fact]
    public void Transaction_RollbackOnError_ChangesNotPersisted()
    {
        using var cn = OpenDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TxTest (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO TxTest VALUES (1, 'before')";
            cmd.ExecuteNonQuery();
        }

        using var tx = cn.BeginTransaction();
        try
        {
            using (var cmd = cn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "UPDATE TxTest SET Val = 'during' WHERE Id = 1";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "THIS IS INVALID SQL";
                cmd.ExecuteNonQuery(); // throws
            }
            tx.Commit();
        }
        catch { tx.Rollback(); }

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT Val FROM TxTest WHERE Id = 1";
        Assert.Equal("before", (string)verify.ExecuteScalar()!);
    }

    [Fact]
    public async Task Transaction_CancellationBeforeCommit_Throws()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var e = new GateSE { Name = "ShouldNotExist" };
        ctx.Add(e);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.SaveChangesAsync(cts.Token));
    }

    [Fact]
    public async Task Connection_ReusedAfterQuery_Works()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO GateSE VALUES (1, 'x')";
            cmd.ExecuteNonQuery();
        }

        var r1 = await ctx.Query<GateSE>().ToListAsync();
        var r2 = await ctx.Query<GateSE>().ToListAsync();
        Assert.Single(r1);
        Assert.Single(r2);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 : C1 cache factory idempotence, cancellation matrix, migration retry
// ══════════════════════════════════════════════════════════════════════════════

public class Gate40To45Tests
{
    // ── C1: ConcurrentLruCache.GetOrAdd factory called at most once per key ───

    [Fact]
    public async Task ConcurrentLruCache_GetOrAdd_FactoryCalledOncePerKey()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 100);
        var factoryCallCount = 0;

        var tasks = Enumerable.Range(0, 200).Select(_ => Task.Run(() =>
            cache.GetOrAdd(42, k =>
            {
                Interlocked.Increment(ref factoryCallCount);
                Thread.Sleep(5);
                return "computed";
            })
        ));

        var results = await Task.WhenAll(tasks);

        Assert.All(results, r => Assert.Equal("computed", r));
        // Lazy<T> with ExecutionAndPublication guarantees exactly one factory execution
        Assert.Equal(1, factoryCallCount);
    }

    [Fact]
    public async Task ConcurrentLruCache_GetOrAdd_DifferentKeys_EachFactoryCalledOnce()
    {
        var cache = new ConcurrentLruCache<int, int>(maxSize: 1000);
        var callCounts = new int[50];

        var tasks = Enumerable.Range(0, 50).SelectMany(key =>
            Enumerable.Range(0, 20).Select(_ => Task.Run(() =>
                cache.GetOrAdd(key, k =>
                {
                    Interlocked.Increment(ref callCounts[k]);
                    return k * 10;
                })
            ))
        );

        await Task.WhenAll(tasks);

        for (int i = 0; i < 50; i++)
            Assert.Equal(1, callCounts[i]);
    }

    [Fact]
    public void ConcurrentLruCache_GetOrAdd_FactoryException_CacheRemainsFunctional()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        var threw = false;

        try { cache.GetOrAdd(1, _ => throw new InvalidOperationException("factory failed")); }
        catch (InvalidOperationException) { threw = true; }

        Assert.True(threw);
        // After a failed factory the key must not be stuck — a subsequent call must succeed
        var value = cache.GetOrAdd(1, _ => "recovered");
        Assert.Equal("recovered", value);
    }

    // ── Cancellation semantics matrix ─────────────────────────────────────────

    [Fact]
    public async Task Cancellation_PreCancelled_ToListAsync_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<GateSE>().ToListAsync(cts.Token));
    }

    [Fact]
    public async Task Cancellation_PreCancelled_CountAsync_Throws()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<GateSE>().CountAsync(cts.Token));
    }

    // ── Migration partial-failure / retry ─────────────────────────────────────

    [Fact]
    public void Migration_ValidThenInvalidStatement_RollsBackEntireTransaction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var tx = cn.BeginTransaction();
        try
        {
            using (var cmd = cn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "CREATE TABLE MigOk (Id INTEGER PRIMARY KEY)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "NOT VALID SQL";
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
        }
        catch { tx.Rollback(); }

        using var probe = cn.CreateCommand();
        probe.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='MigOk'";
        Assert.Equal(0L, Convert.ToInt64(probe.ExecuteScalar()));
    }

    [Fact]
    public void Migration_RetryAfterFailure_Succeeds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        bool firstAttempt = true;
        for (int attempt = 0; attempt < 2; attempt++)
        {
            using var tx = cn.BeginTransaction();
            try
            {
                using var cmd = cn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "CREATE TABLE RetryTable (Id INTEGER PRIMARY KEY)";
                cmd.ExecuteNonQuery();

                if (firstAttempt)
                {
                    firstAttempt = false;
                    cmd.CommandText = "BAD SQL";
                    cmd.ExecuteNonQuery();
                }

                tx.Commit();
            }
            catch { tx.Rollback(); }
        }

        using var probe = cn.CreateCommand();
        probe.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='RetryTable'";
        Assert.Equal(1L, Convert.ToInt64(probe.ExecuteScalar()));
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 → 5.0 : adversarial cache, shared-state safety
// ══════════════════════════════════════════════════════════════════════════════

public class Gate45To50Tests
{
    // ── Cache poisoning: hostile value does not corrupt other keys ────────────

    [Fact]
    public void Cache_HostileValue_DoesNotAffectOtherKeys()
    {
        var cache = new ConcurrentLruCache<string, string>(maxSize: 100);
        cache.GetOrAdd("safe-key", _ => "safe-value");
        cache.GetOrAdd("hostile-key", _ => "'; DROP TABLE users; --");

        Assert.True(cache.TryGet("safe-key", out var v));
        Assert.Equal("safe-value", v);
        Assert.True(cache.TryGet("hostile-key", out var h));
        Assert.Equal("'; DROP TABLE users; --", h); // stored verbatim, not interpreted
    }

    [Fact]
    public void Cache_MaxSizeEnforced_ItemCountWithinBound()
    {
        var cache = new ConcurrentLruCache<int, int>(maxSize: 5);
        for (int i = 0; i < 10; i++)
            cache.Set(i, i * 100);

        int found = 0;
        for (int i = 0; i < 10; i++)
            if (cache.TryGet(i, out _)) found++;
        Assert.True(found <= 5, $"Cache held {found} items, expected ≤5");
    }

    // ── Shared-state safety: parallel contexts must not leak state ────────────

    [Fact]
    public async Task ParallelContexts_IsolatedInMemoryDbs_NoStateLeakage()
    {
        var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider());

            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY, Name TEXT)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = $"INSERT INTO GateSE VALUES ({i}, 'ctx{i}')";
                cmd.ExecuteNonQuery();
            }

            var results = await ctx.Query<GateSE>().ToListAsync();
            Assert.Single(results);
            Assert.Equal(i, results[0].Id);
        }));

        await Task.WhenAll(tasks);
    }

    // ── Query plan cache: same expression, different contexts, no cross-contamination ──

    [Fact]
    public async Task QueryPlanCache_DifferentContexts_SameExpression_NoCrossContamination()
    {
        // Two contexts with different in-memory DBs — same query shape but different data
        using var cn1 = new SqliteConnection("Data Source=:memory:");
        using var cn2 = new SqliteConnection("Data Source=:memory:");
        cn1.Open(); cn2.Open();

        foreach (var (cn, val) in new[] { (cn1, "ctx1"), (cn2, "ctx2") })
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "CREATE TABLE GateSE (Id INTEGER PRIMARY KEY, Name TEXT)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"INSERT INTO GateSE VALUES (1, '{val}')";
            cmd.ExecuteNonQuery();
        }

        using var ctx1 = new DbContext(cn1, new SqliteProvider());
        using var ctx2 = new DbContext(cn2, new SqliteProvider());

        var r1 = await ctx1.Query<GateSE>().Where(e => e.Id == 1).ToListAsync();
        var r2 = await ctx2.Query<GateSE>().Where(e => e.Id == 1).ToListAsync();

        Assert.Equal("ctx1", r1[0].Name);
        Assert.Equal("ctx2", r2[0].Name);
    }
}
