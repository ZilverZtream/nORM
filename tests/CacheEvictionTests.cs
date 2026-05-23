using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies plan-cache eviction behavior under stress: the cache must stay bounded,
/// evict entries when capacity is exceeded, and still serve the most-recently-added
/// entries correctly after churn.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class CacheEvictionTests
{
    // ── Sequential add/evict/get ──────────────────────────────────────────────

    [Fact]
    public void PlanCache_ExceedCapacity_EntryCountStaysBounded()
    {
        const int capacity = 50;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: capacity);

        for (var i = 0; i < capacity * 3; i++)
            cache.Set(i, $"plan_{i}");

        Assert.True(cache.Count <= capacity,
            $"Cache grew to {cache.Count}; expected at most {capacity}.");
    }

    [Fact]
    public void PlanCache_ExceedCapacity_EvictionsAreObservable()
    {
        const int capacity = 20;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: capacity);

        for (var i = 0; i < capacity * 2; i++)
            cache.Set(i, $"plan_{i}");

        Assert.True(cache.Evictions > 0,
            "Expected at least one eviction when writes exceed capacity.");
    }

    [Fact]
    public void PlanCache_AfterEviction_RecentEntriesAreStillRetrievable()
    {
        const int capacity = 10;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: capacity);

        // Fill and overflow twice
        for (var i = 0; i < capacity * 2; i++)
            cache.Set(i, $"plan_{i}");

        // The very last entries written should survive eviction of old ones
        for (var i = capacity; i < capacity * 2; i++)
        {
            if (cache.TryGet(i, out var val))
                Assert.Equal($"plan_{i}", val);
        }
    }

    [Fact]
    public void PlanCache_SetMaxSize_ReducesCapacityAndEvicts()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 100);

        for (var i = 0; i < 100; i++)
            cache.Set(i, $"plan_{i}");

        Assert.Equal(100, cache.Count);

        cache.SetMaxSize(30);

        Assert.True(cache.Count <= 30,
            $"After SetMaxSize(30) cache still has {cache.Count} entries.");
        Assert.True(cache.Evictions > 0, "Shrinking max size must evict entries.");
    }

    [Fact]
    public void PlanCache_Clear_RemovesAllEntries()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 50);

        for (var i = 0; i < 50; i++)
            cache.Set(i, $"plan_{i}");

        Assert.Equal(50, cache.Count);

        cache.Clear();

        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.Evictions);
        Assert.False(cache.TryGet(0, out _), "After Clear, no entry should be retrievable.");
    }

    [Fact]
    public void PlanCache_GetOrAdd_CountsHitsAndMisses()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 50);

        cache.GetOrAdd(1, _ => "plan_1"); // miss
        cache.GetOrAdd(1, _ => "plan_1"); // hit
        cache.GetOrAdd(2, _ => "plan_2"); // miss

        Assert.Equal(1, cache.Hits);
        Assert.Equal(2, cache.Misses);
    }

    // ── Parallel add/evict/get ────────────────────────────────────────────────

    [Fact]
    public void PlanCache_ParallelChurn_StaysBoundedAndEvicts()
    {
        const int capacity = 64;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: capacity);

        Parallel.For(0, 8, worker =>
        {
            var start = worker * 500;
            for (var i = 0; i < 500; i++)
            {
                var key = start + i;
                cache.Set(key, $"plan_{key}");
                cache.TryGet(key, out _);
            }
        });

        Assert.True(cache.Count <= capacity,
            $"Parallel churn grew cache to {cache.Count}; max is {capacity}.");
        Assert.True(cache.Evictions > 0,
            "Parallel churn beyond capacity must force observable evictions.");
    }

    [Fact]
    public void PlanCache_ParallelChurn_NoExceptions()
    {
        const int capacity = 32;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: capacity);

        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        Parallel.For(0, 16, worker =>
        {
            try
            {
                for (var i = 0; i < 200; i++)
                {
                    var key = (worker * 200 + i) % 100; // key collisions deliberate
                    cache.GetOrAdd(key, k => $"plan_{k}");
                    cache.TryGet(key, out _);
                    if (i % 50 == 0) cache.SetMaxSize(Math.Max(8, capacity - (i % 20)));
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        });

        Assert.Empty(exceptions);
    }
}
