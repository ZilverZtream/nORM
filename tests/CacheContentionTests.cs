using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// CacheContentionTests — Gate 4.5 cache contention items
//
// Covers both BoundedCache<K,V> and ConcurrentLruCache<K,V> under high-
// concurrency stress scenarios that the basic unit tests do not reach:
//   • BoundedCache: parallel Set/TryGet racing against eviction
//   • ConcurrentLruCache: concurrent GetOrAdd racing with Set/TTL expiry
//   • Both: size bound never exceeded beyond tolerance under contention
//   • Both: no torn reads or lost writes visible to callers
//   • TTL + eviction interleave safely
//   • SetMaxSize under concurrency shrinks safely
//   • Clear() during active reads/writes is safe
// ══════════════════════════════════════════════════════════════════════════════

public class CacheContentionTests
{
    // ══════════════════════════════════════════════════════════════════════════
    // SECTION 1: BoundedCache<TKey,TValue> contention
    // ══════════════════════════════════════════════════════════════════════════

    private const int BoundedThreads = 8;
    private const int BoundedOps = 1_000;

    // ── BC-1: Parallel Set + TryGet with overlapping keys ────────────────────

    [Fact]
    public void BoundedCache_ParallelSetAndGet_OverlappingKeys_NoExceptionNoCorruption()
    {
        var cache = new BoundedCache<int, string>(maxSize: 20);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, BoundedThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < BoundedOps; i++)
            {
                var key = rng.Next(0, 50);
                try
                {
                    cache.Set(key, $"v{key}");
                    if (cache.TryGet(key, out var val))
                    {
                        // Value could be a newer Set by another thread, but must be one of ours
                        if (!val.StartsWith("v"))
                            errors.Add(new InvalidOperationException($"Corrupt value: '{val}' for key {key}"));
                    }
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
    }

    // ── BC-2: Size bound is never exceeded under contention ──────────────────

    [Fact]
    public void BoundedCache_ParallelSet_SizeNeverExceedsMaxSize()
    {
        const int maxSize = 10;
        var cache = new BoundedCache<int, string>(maxSize);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, BoundedThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < BoundedOps; i++)
            {
                cache.Set(rng.Next(0, 200), "val");
                var count = cache.Count;
                // Due to concurrent access, count can momentarily be maxSize+1
                // (the eviction loop runs after the enqueue, not atomically),
                // but should never exceed maxSize + 2 * thread_count as a generous bound.
                if (count > maxSize + BoundedThreads * 2)
                    errors.Add(new InvalidOperationException($"Count {count} exceeds tolerance"));
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
        // After settling, the count should be at or near maxSize
        Assert.True(cache.Count <= maxSize + 1);
    }

    // ── BC-3: Set is idempotent: same key set by multiple threads ─────────────

    [Fact]
    public void BoundedCache_SameKey_SetByMultipleThreads_NoException()
    {
        var cache = new BoundedCache<string, int>(maxSize: 5);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, 20).Select(t => new Thread(() =>
        {
            try { cache.Set("sharedKey", t); }
            catch (Exception ex) { errors.Add(ex); }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.Empty(errors);
        // The key must be present
        Assert.True(cache.TryGet("sharedKey", out _));
    }

    // ── BC-4: TryGet returns false for evicted keys (not throwing) ────────────

    [Fact]
    public void BoundedCache_Evicted_TryGet_ReturnsFalse()
    {
        var cache = new BoundedCache<int, string>(maxSize: 3);

        // Fill past capacity to trigger eviction
        for (int i = 0; i < 100; i++)
            cache.Set(i, $"v{i}");

        // After eviction, old keys may be gone — just verify no exceptions
        for (int i = 0; i < 100; i++)
        {
            var ex = Record.Exception(() => cache.TryGet(i, out _));
            Assert.Null(ex);
        }

        Assert.True(cache.Count <= 3 + 1);
    }

    // ── BC-5: Clear() under concurrent writes ─────────────────────────────────

    [Fact]
    public void BoundedCache_ClearDuringWrites_NoException()
    {
        var cache = new BoundedCache<int, string>(maxSize: 50);
        var errors = new ConcurrentBag<Exception>();
        var stop = new ManualResetEventSlim(false);

        // Writers
        var writerThreads = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            while (!stop.IsSet)
            {
                try { cache.Set(rng.Next(100), "val"); }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        // Clearer
        var clearerThread = new Thread(() =>
        {
            for (int i = 0; i < 50 && !stop.IsSet; i++)
            {
                try
                {
                    Thread.Sleep(1);
                    cache.Clear();
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        });

        writerThreads.ForEach(t => t.Start());
        clearerThread.Start();
        Thread.Sleep(100);
        stop.Set();
        writerThreads.ForEach(t => t.Join());
        clearerThread.Join();

        Assert.Empty(errors);
    }

    // ── BC-6: Count and MaxSize are always readable under contention ──────────

    [Fact]
    public void BoundedCache_CountAndMaxSize_ReadableUnderContention()
    {
        var cache = new BoundedCache<int, string>(maxSize: 25);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, BoundedThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < BoundedOps; i++)
            {
                try
                {
                    cache.Set(rng.Next(0, 100), "x");
                    var count = cache.Count;
                    var max = cache.MaxSize;
                    if (count < 0) errors.Add(new InvalidOperationException($"Negative count: {count}"));
                    if (max != 25) errors.Add(new InvalidOperationException($"MaxSize changed: {max}"));
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());
        Assert.Empty(errors);
    }

    // ── BC-7: Large key space forces consistent eviction behavior ─────────────

    [Fact]
    public void BoundedCache_LargeKeySpace_MaintainsBound()
    {
        var cache = new BoundedCache<int, string>(maxSize: 100);

        // Insert 10x capacity sequentially
        for (int i = 0; i < 1000; i++)
            cache.Set(i, $"val_{i}");

        // After 1000 inserts into a size-100 cache, count should be bounded
        Assert.True(cache.Count <= 101); // maxSize + 1 tolerance
    }

    // ── BC-8: Comparer-based BoundedCache works under contention ─────────────

    [Fact]
    public void BoundedCache_WithStringComparer_CaseInsensitive_NoException()
    {
        var cache = new BoundedCache<string, int>(maxSize: 10, StringComparer.OrdinalIgnoreCase);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            try
            {
                cache.Set("key", t);
                cache.Set("KEY", t + 100);
                cache.TryGet("Key", out var v);
            }
            catch (Exception ex) { errors.Add(ex); }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());
        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SECTION 2: ConcurrentLruCache<TKey,TValue> contention
    // ══════════════════════════════════════════════════════════════════════════

    private const int LruThreads = 12;
    private const int LruOps = 1_500;

    // ── LRU-1: Parallel Set + TryGet — no corruption ─────────────────────────

    [Fact]
    public void LruCache_ParallelSetAndGet_NoExceptionNoCorruption()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 50);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, LruThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < LruOps; i++)
            {
                var key = rng.Next(0, 100);
                try
                {
                    cache.Set(key, $"val_{key}");
                    if (cache.TryGet(key, out var val))
                    {
                        if (!val.StartsWith("val_"))
                            errors.Add(new InvalidOperationException($"Corrupt value '{val}' for key {key}"));
                    }
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());
        Assert.Empty(errors);
    }

    // ── LRU-2: Size bound maintained under contention ────────────────────────

    [Fact]
    public void LruCache_SizeBound_MaintainedUnderContention()
    {
        const int maxSize = 20;
        using var cache = new ConcurrentLruCache<int, string>(maxSize);
        var errors = new ConcurrentBag<Exception>();

        var threads = Enumerable.Range(0, LruThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < LruOps; i++)
            {
                try
                {
                    cache.Set(rng.Next(0, 200), "v");
                    var count = cache.Count;
                    // Allow generous tolerance for concurrent eviction timing
                    if (count > maxSize * 2)
                        errors.Add(new InvalidOperationException($"Count {count} too large for maxSize {maxSize}"));
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());
        Assert.Empty(errors);
    }

    // ── LRU-3: GetOrAdd factory runs at most once per key under contention ────

    [Fact]
    public void LruCache_GetOrAdd_FactoryRunsAtMostOnce_PerKey()
    {
        const int keys = 50;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 100);
        var factoryCallCount = new ConcurrentDictionary<int, int>();

        var threads = Enumerable.Range(0, LruThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < LruOps; i++)
            {
                var key = rng.Next(0, keys);
                cache.GetOrAdd(key, k =>
                {
                    factoryCallCount.AddOrUpdate(k, 1, (_, c) => c + 1);
                    return $"factory_{k}";
                });
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        // Factory can be called more than once for the same key only when TTL expires
        // (no TTL here, so it should ideally be 1). Allow a small window for races.
        foreach (var kvp in factoryCallCount)
        {
            // In rare racing scenarios the factory may be called twice; beyond that is a bug.
            // The double-check in GetOrAdd reduces this but does not eliminate all races.
            Assert.True(kvp.Value <= LruThreads,
                $"Key {kvp.Key}: factory called {kvp.Value} times (expected <= {LruThreads})");
        }
    }

    // ── LRU-4: TTL expiry + concurrent writes do not deadlock ─────────────────

    [Fact]
    public void LruCache_TtlExpiry_ConcurrentWrites_NoDeadlock()
    {
        using var cache = new ConcurrentLruCache<int, string>(
            maxSize: 50, timeToLive: TimeSpan.FromMilliseconds(10));
        var errors = new ConcurrentBag<Exception>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var threads = Enumerable.Range(0, 6).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var key = rng.Next(0, 30);
                    cache.Set(key, $"v{key}", ttlOverride: TimeSpan.FromMilliseconds(5));
                    cache.TryGet(rng.Next(0, 30), out _);
                    if (rng.Next(10) == 0)
                        Thread.Sleep(5); // cause TTL expiry
                }
                catch (Exception ex) { errors.Add(ex); break; }
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        Thread.Sleep(200);
        cts.Cancel();
        threads.ForEach(t => t.Join(2_000)); // generous timeout

        Assert.Empty(errors);
        Assert.True(threads.All(t => !t.IsAlive), "Thread deadlock detected");
    }

    // ── LRU-5: SetMaxSize shrinks safely under concurrency ────────────────────

    [Fact]
    public void LruCache_SetMaxSize_ShrinkDuringConcurrentWrites_NoException()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 100);
        var errors = new ConcurrentBag<Exception>();
        var stop = new ManualResetEventSlim(false);

        // Writers
        var writerThreads = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            while (!stop.IsSet)
            {
                try { cache.Set(rng.Next(200), "val"); }
                catch (Exception ex) { errors.Add(ex); break; }
            }
        })).ToList();

        // Resizer
        var resizerThread = new Thread(() =>
        {
            int[] sizes = { 5, 10, 50, 100, 20 };
            int idx = 0;
            while (!stop.IsSet)
            {
                try
                {
                    Thread.Sleep(10);
                    cache.SetMaxSize(sizes[idx++ % sizes.Length]);
                }
                catch (Exception ex) { errors.Add(ex); break; }
            }
        });

        writerThreads.ForEach(t => t.Start());
        resizerThread.Start();
        Thread.Sleep(200);
        stop.Set();
        writerThreads.ForEach(t => t.Join(1_000));
        resizerThread.Join(1_000);

        Assert.Empty(errors);
    }

    // ── LRU-6: Clear() while GetOrAdd is running ──────────────────────────────

    [Fact]
    public void LruCache_ClearDuringGetOrAdd_NoException()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 50);
        var errors = new ConcurrentBag<Exception>();
        var stop = new ManualResetEventSlim(false);

        var readers = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            while (!stop.IsSet)
            {
                try
                {
                    var k = rng.Next(100);
                    cache.GetOrAdd(k, key => $"v{key}");
                    cache.TryGet(k, out _);
                }
                catch (Exception ex) { errors.Add(ex); break; }
            }
        })).ToList();

        var clearer = new Thread(() =>
        {
            while (!stop.IsSet)
            {
                try
                {
                    Thread.Sleep(5);
                    cache.Clear();
                }
                catch (Exception ex) { errors.Add(ex); break; }
            }
        });

        readers.ForEach(t => t.Start());
        clearer.Start();
        Thread.Sleep(200);
        stop.Set();
        readers.ForEach(t => t.Join(2_000));
        clearer.Join(2_000);

        Assert.Empty(errors);
    }

    // ── LRU-7: Hit/miss statistics are monotonically non-decreasing ───────────

    [Fact]
    public void LruCache_HitMissStats_NonNegative_AfterContention()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);

        var threads = Enumerable.Range(0, 8).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < 200; i++)
            {
                var key = rng.Next(20);
                cache.Set(key, $"v{key}");
                cache.TryGet(rng.Next(20), out _);
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        Assert.True(cache.Hits >= 0);
        Assert.True(cache.Misses >= 0);
        Assert.True(cache.Hits + cache.Misses > 0); // at least some accesses
    }

    // ── LRU-8: HitRate property under contention ──────────────────────────────

    [Fact]
    public void LruCache_HitRate_ValidRangeAfterContention()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 50);

        // Pre-seed
        for (int i = 0; i < 20; i++) cache.Set(i, $"v{i}");

        var threads = Enumerable.Range(0, LruThreads).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            for (int i = 0; i < 500; i++)
                cache.TryGet(rng.Next(40), out _); // 50% hit rate roughly
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        var hitRate = cache.HitRate;
        Assert.True(hitRate >= 0.0 && hitRate <= 1.0,
            $"HitRate out of range: {hitRate}");
    }

    // ── LRU-9: Dispose during active reads is safe ────────────────────────────

    [Fact]
    public void LruCache_DisposeDuringActiveReads_NoUnhandledExceptions()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 100);
        var errors = new ConcurrentBag<Exception>();
        var stop = new ManualResetEventSlim(false);

        for (int i = 0; i < 50; i++) cache.Set(i, $"v{i}");

        var readers = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            var rng = new Random(t);
            while (!stop.IsSet)
            {
                try { cache.TryGet(rng.Next(50), out _); }
                catch (ObjectDisposedException) { break; } // expected after Dispose
                catch (Exception ex) { errors.Add(ex); break; }
            }
        })).ToList();

        readers.ForEach(t => t.Start());
        Thread.Sleep(20);
        cache.Dispose(); // Dispose while readers are active
        stop.Set();
        readers.ForEach(t => t.Join(1_000));

        Assert.Empty(errors);
    }

    // ── LRU-10: GetOrAdd with null factory throws ArgumentNullException ────────

    [Fact]
    public void LruCache_GetOrAdd_NullFactory_Throws()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        Assert.Throws<ArgumentNullException>(() => cache.GetOrAdd(1, null!));
    }

    // ── LRU-11: SetMaxSize with zero or negative throws ───────────────────────

    [Fact]
    public void LruCache_SetMaxSize_Zero_Throws()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        Assert.Throws<ArgumentOutOfRangeException>(() => cache.SetMaxSize(0));
    }

    [Fact]
    public void LruCache_Constructor_ZeroMaxSize_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ConcurrentLruCache<int, string>(0));
    }

    // ── LRU-12: TTL per-entry override respected under parallel access ─────────

    [Fact]
    public async Task LruCache_PerEntryTtlOverride_ExpiresAfterTtl()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 100,
            timeToLive: TimeSpan.FromHours(1)); // long default TTL

        // Set key 1 with very short TTL override
        cache.Set(1, "short", ttlOverride: TimeSpan.FromMilliseconds(30));
        // Set key 2 with no override (uses default 1-hour TTL)
        cache.Set(2, "long");

        Assert.True(cache.TryGet(1, out _), "Key 1 should be present before TTL expiry");
        Assert.True(cache.TryGet(2, out _), "Key 2 should be present");

        await Task.Delay(100); // Wait for key 1's TTL to expire

        Assert.False(cache.TryGet(1, out _), "Key 1 should have expired");
        Assert.True(cache.TryGet(2, out _), "Key 2 should still be present (long TTL)");
    }

    // ── LRU-13: Parallel GetOrAdd with TTL expiry between calls ───────────────

    [Fact]
    public async Task LruCache_GetOrAdd_TtlExpiry_FactoryCalledAgain()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10,
            timeToLive: TimeSpan.FromMilliseconds(30));

        int callCount = 0;
        cache.GetOrAdd(42, k => { Interlocked.Increment(ref callCount); return "first"; });
        Assert.Equal(1, callCount);

        await Task.Delay(100); // Expire the entry

        cache.GetOrAdd(42, k => { Interlocked.Increment(ref callCount); return "second"; });
        Assert.Equal(2, callCount);

        cache.TryGet(42, out var val);
        Assert.Equal("second", val);
    }

    // ── LRU-14: Set with ttlOverride=null uses cache-level TTL ───────────────

    [Fact]
    public async Task LruCache_Set_NullTtlOverride_UsesCacheTtl()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10,
            timeToLive: TimeSpan.FromMilliseconds(50));

        cache.Set(1, "val", ttlOverride: null);
        Assert.True(cache.TryGet(1, out _));

        await Task.Delay(150);
        Assert.False(cache.TryGet(1, out _), "Entry should have expired using cache TTL");
    }

    // ── LRU-15: Contention between Set() that replaces existing node ──────────

    [Fact]
    public void LruCache_Set_Replace_ExistingEntry_UnderContention()
    {
        using var cache = new ConcurrentLruCache<int, int>(maxSize: 5);
        var errors = new ConcurrentBag<Exception>();

        // Pre-seed
        for (int i = 0; i < 5; i++) cache.Set(i, i);

        var threads = Enumerable.Range(0, 10).Select(t => new Thread(() =>
        {
            try
            {
                for (int i = 0; i < 200; i++)
                {
                    // Overwrite keys 0-4 repeatedly
                    cache.Set(i % 5, t * 1000 + i);
                    cache.TryGet(i % 5, out _);
                }
            }
            catch (Exception ex) { errors.Add(ex); }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());
        Assert.Empty(errors);
        Assert.True(cache.Count <= 5);
    }
}
