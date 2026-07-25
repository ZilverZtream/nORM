using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression (memory-exhaustion DoS): the per-cache-key semaphore map must stay bounded under
/// adversarial <c>.Cacheable()</c> key churn. The previous cleanup removed only a fixed 100 entries per
/// timer tick, so distinct-parameter churn added keys faster than they drained, growing the map without
/// bound. Cleanup now drains unused locks down to the threshold in one pass — while never removing an
/// in-use lock out from under a populating thread.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CacheLockDrainBoundTests
{
    [Fact]
    public void Drain_bounds_the_map_to_the_threshold_in_one_pass()
    {
        var locks = new ConcurrentDictionary<string, SemaphoreSlim>();
        for (var i = 0; i < 5000; i++) locks[$"k{i}"] = new SemaphoreSlim(1, 1);

        NormQueryProvider.DrainUnusedCacheLocks(locks, maxToKeep: 1000);

        // The old fixed-batch cleanup would have left ~4900; a single drain must reach the threshold.
        Assert.Equal(1000, locks.Count);
    }

    [Fact]
    public void Drain_never_removes_in_use_locks()
    {
        var locks = new ConcurrentDictionary<string, SemaphoreSlim>();
        for (var i = 0; i < 1500; i++)
        {
            var held = new SemaphoreSlim(1, 1);
            held.Wait(); // CurrentCount == 0 → in use → must survive
            locks[$"held{i}"] = held;
        }
        for (var i = 0; i < 500; i++) locks[$"free{i}"] = new SemaphoreSlim(1, 1);

        NormQueryProvider.DrainUnusedCacheLocks(locks, maxToKeep: 1000);

        // overage = 2000 - 1000 = 1000, but only the 500 free locks are removable; the 1500 held stay.
        Assert.Equal(1500, locks.Count);
        Assert.All(locks.Keys, k => Assert.StartsWith("held", k));
    }

    [Fact]
    public void Drain_is_a_noop_below_the_threshold()
    {
        var locks = new ConcurrentDictionary<string, SemaphoreSlim>();
        for (var i = 0; i < 300; i++) locks[$"k{i}"] = new SemaphoreSlim(1, 1);

        NormQueryProvider.DrainUnusedCacheLocks(locks, maxToKeep: 1000);

        Assert.Equal(300, locks.Count);
    }
}
