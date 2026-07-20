using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A pooled context routes its Dispose through a pool-return hook. If two Dispose calls race, both must not
/// observe the hook as set and both return the context — that would enqueue one instance twice and hand it to
/// two leases at once, so two request scopes would share one ChangeTracker/connection and interleave writes.
/// The hook must be taken atomically so it fires at most once per lease no matter how many disposers race.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PoolReturnHookConcurrencyTests
{
    private static DbContext MakeCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Concurrent_dispose_returns_the_context_to_the_pool_at_most_once()
    {
        const int iterations = 80;
        const int disposers = 4;

        for (var iter = 0; iter < iterations; iter++)
        {
            var ctx = MakeCtx();
            var fires = 0;
            // The hook stands in for NormDbContextPool.Return; count how many disposers fire it. Returning true
            // marks the context "pooled" so the winner skips teardown (mirroring a real pool return).
            ctx.SetPoolReturnHook(() => { Interlocked.Increment(ref fires); return true; });

            using var barrier = new Barrier(disposers);
            var tasks = new Task[disposers];
            for (var d = 0; d < disposers; d++)
            {
                tasks[d] = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    ctx.Dispose();
                });
            }
            await Task.WhenAll(tasks);

            Assert.True(fires <= 1, $"pool-return hook fired {fires} times on iteration {iter}; a pooled context was returned more than once.");
        }
    }
}
