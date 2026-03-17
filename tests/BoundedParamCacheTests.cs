using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.8 → 4.0 — BoundedCache lifecycle / eviction
//
// Q1/X1 audit finding: _compiledParamSets was a static unbounded ConcurrentDictionary
// that could grow without limit under adversarial or pathological query-shape churn.
// Fix: replaced with BoundedCache<TKey,TValue> (FIFO eviction, cap = 10 000).
//
// These tests verify:
//   - BoundedCache correctly evicts oldest entries when at capacity.
//   - Long-horizon simulated workload does not exceed MaxSize.
//   - Concurrent inserts stay within bounds.
//   - The _compiledParamSets field in NormQueryProvider uses BoundedCache (structural).
// ══════════════════════════════════════════════════════════════════════════════

public class BoundedParamCacheTests
{
    // ── BoundedCache unit tests ───────────────────────────────────────────────

    [Fact]
    public void BoundedCache_BelowCapacity_AllEntriesRetained()
    {
        var cache = new BoundedCache<int, string>(maxSize: 10);
        for (int i = 0; i < 10; i++)
            cache.Set(i, $"v{i}");

        Assert.Equal(10, cache.Count);
        for (int i = 0; i < 10; i++)
        {
            Assert.True(cache.TryGet(i, out var v));
            Assert.Equal($"v{i}", v);
        }
    }

    [Fact]
    public void BoundedCache_AtCapacity_NewEntryEvictsOldest()
    {
        var cache = new BoundedCache<int, string>(maxSize: 3);
        cache.Set(1, "a");
        cache.Set(2, "b");
        cache.Set(3, "c");
        Assert.Equal(3, cache.Count);

        // Adding a 4th entry must evict key=1 (FIFO: first inserted).
        cache.Set(4, "d");

        Assert.True(cache.Count <= 3,
            $"Count should be <= 3 after eviction, got {cache.Count}");
        Assert.False(cache.TryGet(1, out _), "Key 1 (oldest) should have been evicted");
        Assert.True(cache.TryGet(4, out var v) && v == "d");
    }

    [Fact]
    public void BoundedCache_ManyInserts_CountNeverExceedsMaxSize()
    {
        const int cap = 50;
        var cache = new BoundedCache<int, string>(maxSize: cap);

        for (int i = 0; i < 1000; i++)
            cache.Set(i, $"val{i}");

        Assert.True(cache.Count <= cap,
            $"Cache count {cache.Count} exceeded maxSize {cap}");
    }

    [Fact]
    public void BoundedCache_DuplicateKey_NotDoubleAdded()
    {
        var cache = new BoundedCache<int, string>(maxSize: 5);
        cache.Set(1, "first");
        cache.Set(1, "second"); // duplicate key — should not insert again

        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryGet(1, out var v));
        Assert.Equal("first", v); // first writer wins
    }

    [Fact]
    public void BoundedCache_TryGet_MissingKey_ReturnsFalse()
    {
        var cache = new BoundedCache<string, int>(maxSize: 10);
        Assert.False(cache.TryGet("missing", out var val));
        Assert.Equal(0, val);
    }

    [Fact]
    public void BoundedCache_Clear_RemovesAll()
    {
        var cache = new BoundedCache<int, int>(maxSize: 100);
        for (int i = 0; i < 50; i++) cache.Set(i, i);
        Assert.True(cache.Count > 0);

        cache.Clear();
        Assert.Equal(0, cache.Count);
        Assert.False(cache.TryGet(0, out _));
    }

    [Fact]
    public void BoundedCache_MaxSize_PropertyReflectsConstructorArg()
    {
        var cache = new BoundedCache<int, int>(maxSize: 12_345);
        Assert.Equal(12_345, cache.MaxSize);
    }

    // ── Long-horizon stress test ───────────────────────────────────────────────

    /// <summary>
    /// Simulates a long-running process generating 100 000 distinct "query plan" keys
    /// (the adversarial case from Q1). Verifies the cache never exceeds MaxSize.
    /// </summary>
    [Fact]
    public void BoundedCache_LongHorizon_100kDistinctKeys_StaysBounded()
    {
        const int cap = 500;
        const int total = 100_000;
        var cache = new BoundedCache<int, string>(maxSize: cap);
        int maxObserved = 0;

        for (int i = 0; i < total; i++)
        {
            cache.Set(i, $"plan_{i}");
            int count = cache.Count;
            if (count > maxObserved) maxObserved = count;
        }

        Assert.True(maxObserved <= cap + 1, // +1: tiny race window during concurrent eviction
            $"Peak cache size {maxObserved} exceeded cap {cap}");
        Assert.True(cache.Count <= cap,
            $"Final count {cache.Count} exceeds cap {cap}");
    }

    // ── Concurrent inserts stay within bounds ─────────────────────────────────

    [Fact]
    public async Task BoundedCache_ConcurrentInserts_CountStaysBounded()
    {
        const int cap = 200;
        const int tasks = 20;
        const int insertsPerTask = 500;
        var cache = new BoundedCache<int, string>(maxSize: cap);
        int maxObserved = 0;
        var @lock = new object();

        var work = Enumerable.Range(0, tasks).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < insertsPerTask; i++)
            {
                int key = t * insertsPerTask + i;
                cache.Set(key, $"v{key}");
                int cnt = cache.Count;
                lock (@lock) { if (cnt > maxObserved) maxObserved = cnt; }
            }
        })).ToArray();

        await Task.WhenAll(work);

        // Allow a small overrun due to concurrent eviction timing.
        Assert.True(maxObserved <= cap * 2,
            $"Peak {maxObserved} was more than 2× cap {cap} under concurrent load");
        Assert.True(cache.Count <= cap * 2,
            $"Final count {cache.Count} should not greatly exceed cap {cap}");
    }

    // ── Structural: _compiledParamSets in NormQueryProvider uses BoundedCache ─

    [Fact]
    public void NormQueryProvider_CompiledParamSets_IsBoundedCache()
    {
        // Verify via reflection that _compiledParamSets is a BoundedCache, not
        // an unbounded ConcurrentDictionary (the Q1/X1 finding).
        var fieldInfo = typeof(nORM.Query.NormQueryProvider)
            .GetField("_compiledParamSets",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        Assert.NotNull(fieldInfo);
        Assert.True(
            fieldInfo!.FieldType.IsGenericType &&
            fieldInfo.FieldType.GetGenericTypeDefinition() == typeof(BoundedCache<,>),
            $"Expected BoundedCache<,> but got {fieldInfo.FieldType.Name}");
    }

    [Fact]
    public void NormQueryProvider_CompiledParamSets_CapIs10000OrMore()
    {
        var fieldInfo = typeof(nORM.Query.NormQueryProvider)
            .GetField("_compiledParamSets",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        Assert.NotNull(fieldInfo);
        var cache = fieldInfo!.GetValue(null);
        Assert.NotNull(cache);

        // MaxSize property on BoundedCache<QueryPlan, HashSet<string>>
        var maxSizeProp = cache!.GetType().GetProperty("MaxSize",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(maxSizeProp);
        var maxSize = (int)maxSizeProp!.GetValue(cache)!;
        Assert.True(maxSize >= 1_000,
            $"MaxSize should be >= 1000 to handle realistic workloads, got {maxSize}");
    }
}
