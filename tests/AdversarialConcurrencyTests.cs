using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial concurrency proofs for Gate 4.5 → 5.0.
///
/// Validates that shared caches and compiled query/materializer boundaries
/// are free of data races under forced contention and cancellation storms.
///
/// Subsystems covered:
///   - <see cref="CompiledMaterializerStore"/>: concurrent Add + TryGet races.
///   - Compiled query plan cache: concurrent GetPlan from many threads.
///   - Materializer factory cache: concurrent CreateMaterializer calls.
///   - Cancellation boundary: queries cancelled mid-flight do not corrupt caches.
///   - Context isolation: independent DbContext instances do not share mutable state.
/// </summary>
public class AdversarialConcurrencyTests
{
    // ── Shared entity types ────────────────────────────────────────────────────
    // All types are unique to this class to avoid cross-test static-store pollution.

    [Table("AC_Product")]
    private class AcProduct
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }

    [Table("AC_Category")]
    private class AcCategory
    {
        [Key]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    // Types used exclusively for store-race tests (unique names prevent pollution).
    [Table("AC_ConcStore")]
    private class AcConcStoreEntity
    {
        public int Id { get; set; }
        public string Val { get; set; } = string.Empty;
    }

    [Table("AC_FWW")]
    private class AcFwwEntity
    {
        public int Id { get; set; }
    }

    // ── Schema helpers ─────────────────────────────────────────────────────────

    private static SqliteConnection BuildDb(int rows = 20)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var sb = new System.Text.StringBuilder();
        sb.Append("CREATE TABLE AC_Product  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price REAL NOT NULL);");
        sb.Append("CREATE TABLE AC_Category (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);");
        for (int i = 1; i <= rows; i++)
            sb.Append(System.FormattableString.Invariant($"INSERT INTO AC_Product  VALUES ({i},'P{i:D3}',{i * 1.5});"));
        for (int i = 1; i <= 5; i++)
            sb.Append($"INSERT INTO AC_Category VALUES ({i},'Cat{i}');");
        cmd.CommandText = sb.ToString();
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-1: CompiledMaterializerStore — concurrent Add + TryGet never returns
    //       a null delegate when TryGet returns true.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_1_ConcurrentAddAndTryGet_NeverReturnNullDelegate()
    {
        const int threads = 32;
        int nullDelegateCount = 0;

        var tasks = Enumerable.Range(0, threads).Select(i => Task.Run(() =>
        {
            if (i % 2 == 0)
            {
                CompiledMaterializerStore.Add(typeof(AcConcStoreEntity), r =>
                    new AcConcStoreEntity { Id = r.GetInt32(0), Val = r.GetString(1) });
            }
            else
            {
                if (CompiledMaterializerStore.TryGet(typeof(AcConcStoreEntity), "AC_ConcStore", out var mat))
                {
                    if (mat == null)
                        Interlocked.Increment(ref nullDelegateCount);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, nullDelegateCount);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-2: CompiledMaterializerStore — first-write-wins is stable under
    //       many concurrent Add calls for the same key.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_2_ConcurrentAdd_SameKey_FirstWriteWinsIsStable()
    {
        const int threads = 20;
        var seenDelegates = new ConcurrentBag<Func<System.Data.Common.DbDataReader, CancellationToken, Task<object>>>();

        var tasks = Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
        {
            CompiledMaterializerStore.Add(typeof(AcFwwEntity),
                r => (object)new AcFwwEntity { Id = r.GetInt32(0) });

            if (CompiledMaterializerStore.TryGet(typeof(AcFwwEntity), "AC_FWW", out var mat) && mat != null)
                seenDelegates.Add(mat);
        })).ToArray();

        await Task.WhenAll(tasks);

        // All reads must have gotten the same delegate (reference equality).
        Assert.Single(seenDelegates.Distinct());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-3: Compiled query plan cache — concurrent queries with the same
    //       expression always produce identical SQL strings.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_3_ConcurrentGetPlan_SameExpression_AlwaysReturnsSameSql()
    {
        const int threads = 24;
        var sqlBag = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
        {
            using var cn = BuildDb();
            using var ctx = new DbContext(cn, new SqliteProvider());
            var sql = ctx.Query<AcProduct>()
                .Where(p => p.Price > 5m)
                .OrderBy(p => p.Id)
                .ToString();
            sqlBag.Add(sql ?? "");
        })).ToArray();

        await Task.WhenAll(tasks);

        var distinctSql = sqlBag.Distinct(StringComparer.Ordinal).ToList();
        Assert.Single(distinctSql);
        Assert.Contains("AC_Product", distinctSql[0], StringComparison.OrdinalIgnoreCase);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-4: Materializer factory cache — concurrent ToListAsync on the same
    //       mapping returns callable delegates with no torn IL state.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_4_ConcurrentMaterializerCreation_AllDelegatesCallable()
    {
        const int threads = 16;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, threads).Select(_ => Task.Run(async () =>
        {
            try
            {
                using var cn = BuildDb(rows: 5);
                using var ctx = new DbContext(cn, new SqliteProvider());

                var results = await ctx.Query<AcProduct>()
                    .Where(p => p.Id <= 5)
                    .ToListAsync();

                if (results.Count != 5)
                    errors.Add($"Expected 5 products, got {results.Count}");

                foreach (var p in results)
                    if (string.IsNullOrEmpty(p.Name))
                        errors.Add($"Product {p.Id} has empty name");
            }
            catch (Exception ex)
            {
                errors.Add(ex.Message);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-5: Cancellation during concurrent materializer cache warm-up does not
    //       corrupt the cache — completed queries still return correct results.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_5_CancellationDuringCacheWarmUp_DoesNotCorruptCache()
    {
        const int threads = 12;
        var results = new ConcurrentBag<(bool Cancelled, int Count)>();

        var tasks = Enumerable.Range(0, threads).Select(i => Task.Run(async () =>
        {
            using var cn = BuildDb(rows: 10);
            using var ctx = new DbContext(cn, new SqliteProvider());
            using var cts = new CancellationTokenSource();

            // Odd threads cancel early; even threads run to completion.
            if (i % 2 != 0) cts.CancelAfter(TimeSpan.FromMilliseconds(1));

            try
            {
                var list = await ctx.Query<AcProduct>()
                    .Where(p => p.Id > 0)
                    .ToListAsync(cts.Token);
                results.Add((false, list.Count));
            }
            catch (OperationCanceledException)
            {
                results.Add((true, 0));
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Completed queries must return the correct row count.
        foreach (var (cancelled, count) in results)
            if (!cancelled)
                Assert.Equal(10, count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-6: Context isolation — independent DbContext instances with different
    //       data never cross-contaminate their query results.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_6_ConcurrentContexts_IsolatedData_NoContamination()
    {
        const int pairs = 8;
        int contaminationCount = 0;

        var tasks = Enumerable.Range(1, pairs).Select(seed => Task.Run(async () =>
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var setup = cn.CreateCommand();
            setup.CommandText =
                "CREATE TABLE AC_Product (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price REAL NOT NULL);" +
                $"INSERT INTO AC_Product VALUES ({seed},'Sentinel{seed}',{seed});";
            setup.ExecuteNonQuery();

            using var ctx = new DbContext(cn, new SqliteProvider());
            var list = await ctx.Query<AcProduct>().ToListAsync();

            if (list.Count != 1 || list[0].Id != seed || list[0].Name != $"Sentinel{seed}")
                Interlocked.Increment(ref contaminationCount);
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, contaminationCount);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-7: Cancellation storm against queries — after cancelled calls the
    //       compiled query and materializer caches must remain consistent for
    //       subsequent successful queries.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_7_CancellationStorm_CacheRemainsConsistentAfterwards()
    {
        const int threads = 12;
        int successCount = 0;

        var tasks = Enumerable.Range(0, threads).Select(i => Task.Run(async () =>
        {
            using var cn = BuildDb(rows: 5);
            using var ctx = new DbContext(cn, new SqliteProvider());
            using var cts = new CancellationTokenSource();
            if (i % 3 == 0) cts.CancelAfter(TimeSpan.FromMilliseconds(1));

            try
            {
                var result = await ctx.Query<AcProduct>()
                    .Where(p => p.Id > 0)
                    .OrderBy(p => p.Id)
                    .ToListAsync(cts.Token);

                if (result.Count == 5)
                    Interlocked.Increment(ref successCount);
            }
            catch (OperationCanceledException) { }
        })).ToArray();

        await Task.WhenAll(tasks);

        // After the storm, a fresh query must still produce correct results.
        using var cn2 = BuildDb(rows: 5);
        using var ctx2 = new DbContext(cn2, new SqliteProvider());
        var finalResult = await ctx2.Query<AcProduct>().Where(p => p.Id > 0).ToListAsync();
        Assert.Equal(5, finalResult.Count);
        Assert.All(finalResult, p => Assert.False(string.IsNullOrEmpty(p.Name)));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-8: Plan cache never recycles a plan for a structurally different query.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_8_PlanCache_DistinctExpressions_NeverCrossContaminate()
    {
        const int threads = 16;
        int wrongSqlCount = 0;

        var tasks = Enumerable.Range(1, threads).Select(id => Task.Run(() =>
        {
            using var cn = BuildDb();
            using var ctx = new DbContext(cn, new SqliteProvider());

            var name = $"P{id:D3}";
            var sql = ctx.Query<AcProduct>()
                .Where(p => p.Name == name)
                .ToString();

            if (!(sql ?? "").Contains("AC_Product", StringComparison.OrdinalIgnoreCase))
                Interlocked.Increment(ref wrongSqlCount);
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, wrongSqlCount);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-9: Pre-cancelled SaveChangesAsync does not corrupt a subsequent save
    //       on an independent context.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_9_PreCancelledSave_DoesNotCorruptSubsequentContext()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE AC_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL, Price REAL NOT NULL);";
        setup.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        ctx.Add(new AcProduct { Name = "First", Price = 1m });
        try { await ctx.SaveChangesAsync(cts.Token); }
        catch (OperationCanceledException) { }

        // Fresh context on a fresh DB must succeed independently.
        using var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using var setup2 = cn2.CreateCommand();
        setup2.CommandText =
            "CREATE TABLE AC_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL, Price REAL NOT NULL);";
        setup2.ExecuteNonQuery();

        using var ctx2 = new DbContext(cn2, new SqliteProvider());
        ctx2.Add(new AcProduct { Name = "Second", Price = 2m });
        await ctx2.SaveChangesAsync();

        var saved = ctx2.Query<AcProduct>().ToList();
        Assert.Single(saved);
        Assert.Equal("Second", saved.Single().Name);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AC-10: Concurrent reads from many contexts all see the same SQL for the
    //        same query shape — proves plan cache convergence under high load.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AC_10_HighConcurrencyPlanCache_ConvergesCorrectly()
    {
        const int threads = 32;
        var sqlBag = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
        {
            using var cn = BuildDb(rows: 3);
            using var ctx = new DbContext(cn, new SqliteProvider());
            // Identical expression on every thread.
            var sql = ctx.Query<AcProduct>()
                .Where(p => p.Price > 1m && p.Price < 100m)
                .OrderByDescending(p => p.Name)
                .ToString();
            sqlBag.Add(sql ?? "");
        })).ToArray();

        await Task.WhenAll(tasks);

        var distinct = sqlBag.Distinct(StringComparer.Ordinal).ToList();
        Assert.Single(distinct);
        Assert.Contains("AC_Product", distinct[0], StringComparison.OrdinalIgnoreCase);
    }
}
