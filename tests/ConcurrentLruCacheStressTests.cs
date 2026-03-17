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
// C1 — ConcurrentLruCache high-contention stress tests (Gate 4.0→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Adversarial concurrency tests for <see cref="ConcurrentLruCache{TKey,TValue}"/>.
///
/// C1 root cause: The cache uses a complex concurrency surface — <c>ConcurrentDictionary</c>
/// + <c>LinkedList</c> + <c>ReaderWriterLockSlim</c> + interlocked timestamp reads — that
/// existing functional tests (TTL/hit-rate/clear) do not exercise under parallel pressure.
///
/// These tests drive the cache with many parallel threads performing concurrent reads,
/// writes, and TTL-triggered evictions, verifying:
/// <list type="bullet">
///   <item>No exceptions escape under any interleaving.</item>
///   <item>All retrieved values are correct (no partial/torn writes).</item>
///   <item>Count never exceeds <c>maxSize + 1</c> (eviction maintains the bound).</item>
///   <item>The cache remains usable after high-contention runs (Clear + re-use).</item>
///   <item>TTL expiry + eviction interleave safely with concurrent inserts.</item>
///   <item>Factory functions run at most once per key under concurrent misses.</item>
/// </list>
/// </summary>
public class ConcurrentLruCacheStressTests
{
    private const int Threads      = 16;
    private const int OpsPerThread = 2_000;

    // ── 1. Parallel GetOrAdd with overlapping key space ──────────────────────

    [Fact]
    public void ParallelGetOrAdd_OverlappingKeys_NoExceptionNoCorruption()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 50);
        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, Threads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < OpsPerThread; i++)
            {
                var key = rng.Next(0, 100); // heavy overlap → many racing writers
                try
                {
                    var val = cache.GetOrAdd(key, k => $"v{k}");
                    if (val != $"v{key}")
                        errors.Add(new InvalidOperationException(
                            $"Key {key}: expected 'v{key}', got '{val}'"));
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
    }

    // ── 2. Count never exceeds maxSize under parallel insertions ─────────────

    [Fact]
    public void ParallelInserts_CountNeverExceedsMaxSize()
    {
        const int maxSize = 30;
        using var cache = new ConcurrentLruCache<int, int>(maxSize: maxSize);
        var errors = new System.Collections.Concurrent.ConcurrentBag<string>();

        var threads = Enumerable.Range(0, Threads).Select(t => new Thread(() =>
        {
            var rng = new Random(t * 31);
            for (int i = 0; i < OpsPerThread; i++)
            {
                cache.GetOrAdd(rng.Next(0, 200), k => k);
                var c = cache.Count;
                // Allow one overshoot for the brief window between insert and eviction
                if (c > maxSize + 1)
                    errors.Add($"Count {c} exceeded maxSize {maxSize} at thread {t}");
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
    }

    // ── 3. Concurrent readers + writers: retrieved values are always correct ──

    [Fact]
    public void ConcurrentReadersAndWriters_RetrievedValuesCorrect()
    {
        using var cache = new ConcurrentLruCache<int, int>(maxSize: 20);
        // Pre-populate keys 0..19 so readers have something to hit.
        for (int k = 0; k < 20; k++)
            cache.GetOrAdd(k, x => x * 10);

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        // 8 reader threads, 8 writer threads competing over the same keyspace.
        var tasks = new List<Thread>();
        for (int t = 0; t < 8; t++)
        {
            int tLocal = t;
            tasks.Add(new Thread(() =>
            {
                var rng = new Random(tLocal);
                for (int i = 0; i < OpsPerThread; i++)
                {
                    int key = rng.Next(0, 20);
                    if (cache.TryGet(key, out var v))
                    {
                        if (v != key * 10)
                            errors.Add(new InvalidOperationException(
                                $"Key {key}: expected {key * 10}, got {v}"));
                    }
                }
            }));
            tasks.Add(new Thread(() =>
            {
                var rng = new Random(tLocal + 100);
                for (int i = 0; i < OpsPerThread; i++)
                {
                    int key = rng.Next(0, 20);
                    try { cache.GetOrAdd(key, k => k * 10); }
                    catch (Exception ex) { errors.Add(ex); }
                }
            }));
        }

        tasks.ForEach(t => t.Start());
        tasks.ForEach(t => t.Join());

        Assert.Empty(errors);
    }

    // ── 4. TTL expiry racing with concurrent inserts ──────────────────────────

    [Fact]
    public void ParallelInserts_WithShortTtl_NoExceptionUnderTtlChurn()
    {
        // 10 ms TTL causes frequent expiry while threads are inserting.
        using var cache = new ConcurrentLruCache<int, string>(
            maxSize: 20,
            timeToLive: TimeSpan.FromMilliseconds(10));

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, Threads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < 500; i++) // fewer ops — TTL flips create contention
            {
                var key = rng.Next(0, 30);
                try
                {
                    cache.GetOrAdd(key, k => $"val{k}");
                    // Mix in TryGet on expired entries to exercise the expiry-removal path.
                    cache.TryGet(key, out _);
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
    }

    // ── 5. SetMaxSize while threads are reading/writing ───────────────────────

    [Fact]
    public void SetMaxSize_UnderConcurrentAccess_NoException()
    {
        using var cache = new ConcurrentLruCache<int, int>(maxSize: 100);
        for (int k = 0; k < 80; k++) cache.GetOrAdd(k, x => x);

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, Threads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < 500; i++)
            {
                try
                {
                    if (i % 50 == 0)
                    {
                        // Occasionally shrink / grow the cache while others read/write.
                        cache.SetMaxSize(rng.Next(10, 60));
                    }
                    else
                    {
                        var key = rng.Next(0, 80);
                        cache.GetOrAdd(key, k => k);
                        cache.TryGet(key, out _);
                    }
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
    }

    // ── 6. Clear while threads are reading/writing ────────────────────────────

    [Fact]
    public void Clear_UnderConcurrentAccess_NoExceptionAndCacheReusable()
    {
        using var cache = new ConcurrentLruCache<int, int>(maxSize: 50);
        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, Threads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < 800; i++)
            {
                try
                {
                    if (i % 200 == 0) cache.Clear();
                    else               cache.GetOrAdd(rng.Next(0, 60), k => k);
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);

        // Cache must be fully usable after all that chaos.
        cache.GetOrAdd(999, k => k);
        Assert.True(cache.TryGet(999, out var v) && v == 999);
    }

    // ── 7. Factory runs at most once per key under concurrent misses ──────────

    [Fact]
    public void GetOrAdd_ConcurrentMissOnSameKey_FactoryRunsOnce()
    {
        using var cache = new ConcurrentLruCache<int, int>(maxSize: 200);
        var factoryCalls = new System.Collections.Concurrent.ConcurrentDictionary<int, int>();

        const int key = 42;
        const int numThreads = 32;

        // All threads race to add the same key simultaneously.
        var barrier = new Barrier(numThreads);
        var threads = Enumerable.Range(0, numThreads).Select(_ => new Thread(() =>
        {
            barrier.SignalAndWait();
            cache.GetOrAdd(key, k =>
            {
                factoryCalls.AddOrUpdate(k, 1, (_, n) => n + 1);
                Thread.Sleep(1); // small delay to maximise race window
                return k * 7;
            });
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        // Factory must have run exactly once for this key.
        var calls = factoryCalls.GetValueOrDefault(key, 0);
        Assert.Equal(1, calls);

        // And the cached value must be correct.
        Assert.True(cache.TryGet(key, out var v));
        Assert.Equal(key * 7, v);
    }

    // ── 8. Hit/miss stats remain non-negative under parallel access ───────────

    [Fact]
    public void HitMissStats_UnderParallelAccess_NeverNegative()
    {
        using var cache = new ConcurrentLruCache<int, int>(maxSize: 20);

        Parallel.For(0, Threads * OpsPerThread, i =>
        {
            var key = i % 50;
            cache.GetOrAdd(key, k => k);
            cache.TryGet(key, out _);
        });

        Assert.True(cache.Hits >= 0);
        Assert.True(cache.Misses >= 0);
        Assert.True(cache.HitRate is >= 0.0 and <= 1.0);
    }

    // ── 9. High-keyspace eviction: oldest entries are removed under pressure ──

    [Fact]
    public void Eviction_UnderHighKeySpace_CacheRemainsAtMaxSize()
    {
        const int maxSize = 10;
        using var cache = new ConcurrentLruCache<int, int>(maxSize: maxSize);

        // Insert far more unique keys than the cache can hold.
        for (int k = 0; k < 1_000; k++)
            cache.GetOrAdd(k, x => x);

        // After pressure, count must not exceed maxSize (+1 transient window).
        Assert.True(cache.Count <= maxSize + 1,
            $"Expected count ≤ {maxSize + 1}, got {cache.Count}");
    }

    // ── 10. Concurrent TryGet on expired + non-expired mix ───────────────────

    [Fact]
    public void TryGet_MixedExpiredAndFresh_NoExceptionAndCorrectValues()
    {
        // Very short TTL: some entries expire while being read.
        using var cache = new ConcurrentLruCache<int, string>(
            maxSize: 100,
            timeToLive: TimeSpan.FromMilliseconds(5));

        // Pre-populate.
        for (int k = 0; k < 50; k++)
            cache.GetOrAdd(k, x => $"item{x}");

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        Parallel.For(0, Threads * 500, i =>
        {
            try
            {
                int key = i % 50;
                if (cache.TryGet(key, out var v))
                {
                    if (v != $"item{key}")
                        errors.Add(new Exception($"Bad value for key {key}: '{v}'"));
                }
                // Re-insert fresh entry to maintain TTL churn.
                cache.GetOrAdd(key, k => $"item{k}");
            }
            catch (Exception ex) { errors.Add(ex); }
        });

        Assert.Empty(errors);
    }
}
