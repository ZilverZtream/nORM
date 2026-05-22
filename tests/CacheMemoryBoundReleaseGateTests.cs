using System.Threading.Tasks;
using nORM.Internal;
using nORM.SourceGeneration;
using Xunit;

namespace nORM.Tests;

public class CacheMemoryBoundReleaseGateTests
{
    [Fact]
    public void ConcurrentLruCache_AdversarialChurn_StaysBoundedAndEvicts()
    {
        using var cache = new ConcurrentLruCache<int, byte[]>(maxSize: 128);

        Parallel.For(0, 8, worker =>
        {
            var start = worker * 1_000;
            for (var i = 0; i < 1_000; i++)
            {
                var key = start + i;
                cache.Set(key, new byte[256]);
                cache.TryGet(key, out _);
            }
        });

        Assert.True(cache.Count <= 128, $"LRU cache grew to {cache.Count} entries.");
        Assert.True(cache.Evictions > 0, "Adversarial churn should force observable eviction.");
        Assert.True(cache.Hits > 0, "Stress gate should exercise cache hits, not only writes.");
    }

    [Fact]
    public void BoundedCache_AdversarialChurn_StaysBelowMaxSize()
    {
        var cache = new BoundedCache<int, int>(maxSize: 100);

        Parallel.For(0, 16, worker =>
        {
            var start = worker * 1_000;
            for (var i = 0; i < 1_000; i++)
                cache.Set(start + i, i);
        });

        Assert.True(cache.Count <= cache.MaxSize, $"Bounded cache grew to {cache.Count}; max is {cache.MaxSize}.");
    }

    [Fact]
    public void CompiledMaterializerStore_TableNameChurn_StaysBoundedAndObservable()
    {
        CompiledMaterializerStore.Clear();
        try
        {
            for (var i = 0; i < 650; i++)
            {
                var tableName = "CacheBound_" + i.ToString("D4");
                CompiledMaterializerStore.Add<CacheBoundEntity>(tableName, static _ => new CacheBoundEntity());
            }

            Assert.True(CompiledMaterializerStore.Count <= 500,
                $"Compiled materializer store grew to {CompiledMaterializerStore.Count} entries.");
            Assert.True(CompiledMaterializerStore.Evictions > 0,
                "Compiled materializer store should evict when table/model churn exceeds its v1 bound.");
            Assert.False(CompiledMaterializerStore.TryGet(typeof(CacheBoundEntity), "CacheBound_0000", out _));
            Assert.True(CompiledMaterializerStore.TryGet(typeof(CacheBoundEntity), "CacheBound_0649", out _));
        }
        finally
        {
            CompiledMaterializerStore.Clear();
        }
    }

    private sealed class CacheBoundEntity
    {
        public int Id { get; set; }
    }
}
