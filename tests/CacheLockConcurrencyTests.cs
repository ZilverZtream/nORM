using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that the cache-lock cleanup timer never disposes a semaphore that another
/// thread obtained via GetOrAdd but has not yet called Wait on — previously the cleanup code
/// called semaphore.Dispose() after TryRemove, causing ObjectDisposedException on the waiting thread.
/// </summary>
public class CacheLockConcurrencyTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static DbContext CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    /// <summary>
    /// Obtain the static _cacheLocks field from NormQueryProvider via reflection.
    /// </summary>
    private static ConcurrentDictionary<string, SemaphoreSlim> GetCacheLocks()
    {
        var field = typeof(NormQueryProvider)
            .GetField("_cacheLocks", BindingFlags.NonPublic | BindingFlags.Static)!;
        return (ConcurrentDictionary<string, SemaphoreSlim>)field.GetValue(null)!;
    }

    /// <summary>
    /// Invoke the private static CleanupCacheLocks method.
    /// </summary>
    private static void InvokeCleanup()
    {
        var method = typeof(NormQueryProvider)
            .GetMethod("CleanupCacheLocks", BindingFlags.NonPublic | BindingFlags.Static)!;
        method.Invoke(null, new object?[] { null });
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Concurrent cleanup + semaphore Wait must never produce ObjectDisposedException.
    /// Stress test: 50 tasks continuously GetOrAdd + Wait/Release while a tight loop calls
    /// CleanupCacheLocks. We fill the dict past MaxLocksToKeep (1000) to ensure cleanup runs.
    /// </summary>
    [Fact]
    public async Task CacheLock_ConcurrentAccessDuringCleanup_NoObjectDisposedException()
    {
        var cacheLocks = GetCacheLocks();

        // Pre-populate with 1100 keys so cleanup will always find candidates.
        // Use a unique prefix per test run to avoid cross-test interference.
        var prefix = Guid.NewGuid().ToString("N");
        for (int i = 0; i < 1100; i++)
            cacheLocks.GetOrAdd($"{prefix}_pre_{i}", _ => new SemaphoreSlim(1, 1));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var exceptions = new ConcurrentBag<Exception>();

        // 50 worker tasks: each repeatedly gets/releases a semaphore while cleanup fires.
        var workers = new List<Task>(50);
        for (int w = 0; w < 50; w++)
        {
            var workerId = w;
            workers.Add(Task.Run(async () =>
            {
                int iteration = 0;
                while (!cts.Token.IsCancellationRequested)
                {
                    var key = $"{prefix}_worker_{workerId}_{iteration % 20}";
                    try
                    {
                        var sem = cacheLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
                        await sem.WaitAsync(cts.Token).ConfigureAwait(false);
                        try { await Task.Yield(); }
                        finally { sem.Release(); }
                    }
                    catch (OperationCanceledException) { /* expected on shutdown */ }
                    catch (Exception ex) { exceptions.Add(ex); }
                    iteration++;
                }
            }));
        }

        // Cleanup loop: call CleanupCacheLocks as fast as possible while workers run.
        var cleanupTask = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try { InvokeCleanup(); }
                catch (Exception ex) { exceptions.Add(ex); }
            }
        });

        await Task.WhenAll(workers);
        await cleanupTask;

        // must not see any ObjectDisposedException (or any exception from workers).
        Assert.Empty(exceptions);
    }

    /// <summary>
    /// Value-matching TryRemove semantics — removing a stale (key,semaphore) pair
    /// must NOT remove a freshly re-inserted semaphore for the same key.
    /// This validates the core invariant of the fix without involving NormQueryProvider.
    /// </summary>
    [Fact]
    public void CacheLock_ValueMatchingRemove_DoesNotRemoveReplacedSemaphore()
    {
        var dict = new ConcurrentDictionary<string, SemaphoreSlim>();

        var stale = new SemaphoreSlim(1, 1);
        dict["key"] = stale;

        // Simulate: cleanup obtained reference to stale semaphore.
        // Now another thread replaces it with a fresh one BEFORE cleanup calls TryRemove.
        var fresh = new SemaphoreSlim(1, 1);
        dict["key"] = fresh;

        // Value-matching TryRemove: should NOT remove because value changed.
        bool removed = dict.TryRemove(new KeyValuePair<string, SemaphoreSlim>("key", stale));

        Assert.False(removed, "TryRemove with stale value should not remove the current entry.");
        Assert.True(dict.ContainsKey("key"), "Key should still exist after failed value-matching remove.");
        Assert.Same(fresh, dict["key"]);

        // Cleanup: stale is not disposed by the fix, so we can dispose it here for hygiene.
        stale.Dispose();
        fresh.Dispose();
    }
}
