using System;
using System.Threading;
using nORM.Internal;
using Xunit;

namespace nORM.Tests;

public class ConcurrentLruCacheTests
{
    [Fact]
    public void Expired_entries_are_evicted()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10, timeToLive: TimeSpan.FromMilliseconds(50));
        cache.GetOrAdd(1, _ => "value");
        Thread.Sleep(60);
        Assert.False(cache.TryGet(1, out _));
    }

    [Fact]
    public void Hit_rate_is_tracked()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10, timeToLive: TimeSpan.FromMinutes(1));
        cache.GetOrAdd(1, _ => "value");
        cache.GetOrAdd(1, _ => "other");
        Assert.Equal(1, cache.Hits);
        Assert.Equal(1, cache.Misses);
        Assert.Equal(0.5, cache.HitRate);
    }

    [Fact]
    public void Cache_can_be_cleared()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10, timeToLive: TimeSpan.FromMinutes(1));
        cache.GetOrAdd(1, _ => "value");
        cache.Clear();
        Assert.False(cache.TryGet(1, out _));
    }
}

