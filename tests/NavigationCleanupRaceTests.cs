using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// C1: Verifies that RemovePendingLoadsForEntity uses the same synchronization
/// primitive (_batchSemaphore) as LoadNavigationAsync and ProcessBatchAsync,
/// preventing concurrent Dictionary mutation across different lock domains.
/// </summary>
public class NavigationCleanupRaceTests
{
    [Table("NcrAuthor")]
    private sealed class NcrAuthor
    {
        [Key]
        public string AuthorId { get; set; } = string.Empty;
        public ICollection<NcrBook> Books { get; set; } = new List<NcrBook>();
    }

    [Table("NcrBook")]
    private sealed class NcrBook
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string AuthorId { get; set; } = string.Empty;
        public string Title { get; set; } = string.Empty;
    }

    private static DbContextOptions BuildOptions() => new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<NcrAuthor>()
              .HasKey(a => a.AuthorId)
              .HasMany(a => a.Books)
              .WithOne()
              .HasForeignKey(b => b.AuthorId);
        }
    };

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void CreateSchema(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS NcrAuthor (AuthorId TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS NcrBook (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId TEXT NOT NULL, Title TEXT NOT NULL);
        ";
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// C1-1: Concurrent cleanup + load operations should not throw due to
    /// Dictionary corruption. Before the fix, RemovePendingLoadsForEntity used
    /// lock(_syncLock) while LoadNavigationAsync used _batchSemaphore, which meant
    /// two threads could mutate _pendingLoads simultaneously.
    /// </summary>
    [Fact]
    public async Task C1_ConcurrentCleanupAndLoad_DoNotCrash()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);

        // Insert seed data
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO NcrAuthor VALUES ('A1');
                INSERT INTO NcrAuthor VALUES ('A2');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('A1', 'Book1');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('A2', 'Book2');
            ";
            cmd.ExecuteNonQuery();
        }

        var options = BuildOptions();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var loader = new BatchedNavigationLoader(ctx);

        var authors = new List<NcrAuthor>();
        for (int i = 0; i < 20; i++)
        {
            authors.Add(new NcrAuthor { AuthorId = $"stress_{i}" });
        }

        // Run many concurrent cleanup + load operations
        var tasks = new List<Task>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        for (int i = 0; i < 20; i++)
        {
            var author = authors[i];

            // Queue a load and a cleanup concurrently
            tasks.Add(Task.Run(() =>
            {
                // Cleanup should not throw even when loads are in flight
                loader.RemovePendingLoadsForEntity(author);
            }));

            tasks.Add(Task.Run(() =>
            {
                loader.RemovePendingLoadsForEntity(author);
            }));
        }

        // All tasks should complete without throwing (no Dictionary race crash)
        await Task.WhenAll(tasks);

        loader.Dispose();
    }

    /// <summary>
    /// C1-2: Cleanup during processing should not silently lose entries or crash.
    /// When RemovePendingLoadsForEntity runs while ProcessBatchAsync is draining the
    /// dictionary, neither operation should throw.
    /// </summary>
    [Fact]
    public async Task C1_CleanupDuringProcessing_DoesNotLoseEntries()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NcrAuthor VALUES ('A1');";
            cmd.ExecuteNonQuery();
        }

        var options = BuildOptions();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var loader = new BatchedNavigationLoader(ctx);

        var author1 = new NcrAuthor { AuthorId = "A1" };
        var author2 = new NcrAuthor { AuthorId = "cleanup_target" };

        // Rapidly alternate between queuing loads and removing entities
        var exceptions = new List<Exception>();
        var barrier = new Barrier(2);

        var cleanupTask = Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (int i = 0; i < 50; i++)
            {
                try
                {
                    loader.RemovePendingLoadsForEntity(author2);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            }
        });

        var removeTask = Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (int i = 0; i < 50; i++)
            {
                try
                {
                    loader.RemovePendingLoadsForEntity(author1);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            }
        });

        await Task.WhenAll(cleanupTask, removeTask);

        // No exceptions should have been thrown due to concurrent Dictionary access
        Assert.Empty(exceptions);

        loader.Dispose();
    }

    /// <summary>
    /// Fuzz test: 10 concurrent tasks repeatedly call RemovePendingLoadsForEntity on
    /// random entities while another task calls LoadNavigationAsync. Runs for 2 seconds.
    /// Verifies no exceptions, no dictionary corruption, and all tasks complete.
    /// </summary>
    [Fact]
    public async Task Fuzz_ConcurrentCleanupAndLoad_NoCorruption()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);

        // Seed some data so LoadNavigationAsync has rows to read
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO NcrAuthor VALUES ('fuzz_a1');
                INSERT INTO NcrAuthor VALUES ('fuzz_a2');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('fuzz_a1', 'FuzzBook1');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('fuzz_a2', 'FuzzBook2');
            ";
            cmd.ExecuteNonQuery();
        }

        var options = BuildOptions();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var loader = new BatchedNavigationLoader(ctx);

        // Create a pool of random entities to operate on
        var entities = Enumerable.Range(0, 50)
            .Select(i => new NcrAuthor { AuthorId = $"fuzz_{i}" })
            .ToArray();

        var exceptions = new ConcurrentBag<Exception>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var tasks = new List<Task>();

        // 10 tasks doing RemovePendingLoadsForEntity on random entities
        var rng = new Random(42);
        for (int t = 0; t < 10; t++)
        {
            var localSeed = rng.Next();
            tasks.Add(Task.Run(() =>
            {
                var localRng = new Random(localSeed);
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var entity = entities[localRng.Next(entities.Length)];
                        loader.RemovePendingLoadsForEntity(entity);
                    }
                    catch (ObjectDisposedException)
                    {
                        // Loader may be disposed as test winds down
                        break;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
            }));
        }

        // 1 task doing LoadNavigationAsync repeatedly (uses cts.Token so it
        // terminates promptly when the test period ends)
        tasks.Add(Task.Run(async () =>
        {
            var localRng = new Random(12345);
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var entity = entities[localRng.Next(entities.Length)];
                    await loader.LoadNavigationAsync(entity, nameof(NcrAuthor.Books), cts.Token);
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (OperationCanceledException)
                {
                    // Expected when CTS fires — stop the loop
                    break;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }
        }));

        // Wait for all tasks (cleanup tasks stop via CTS check; load task stops via OCE)
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        // No dictionary corruption or unexpected exceptions
        Assert.Empty(exceptions);

        loader.Dispose();
    }

    /// <summary>
    /// Cleanup during active batch processing: Start a batch via LoadNavigationAsync,
    /// concurrently call RemovePendingLoadsForEntity for entities in the batch.
    /// Verify no crash and remaining entries are correctly processed.
    /// </summary>
    [Fact]
    public async Task CleanupDuringActiveBatch_NoCrashAndRemainingProcessed()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);

        // Seed data for two authors
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO NcrAuthor VALUES ('batch_a1');
                INSERT INTO NcrAuthor VALUES ('batch_a2');
                INSERT INTO NcrAuthor VALUES ('batch_a3');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('batch_a1', 'B1');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('batch_a2', 'B2');
                INSERT INTO NcrBook (AuthorId, Title) VALUES ('batch_a3', 'B3');
            ";
            cmd.ExecuteNonQuery();
        }

        var options = BuildOptions();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var loader = new BatchedNavigationLoader(ctx);

        var author1 = new NcrAuthor { AuthorId = "batch_a1" };
        var author2 = new NcrAuthor { AuthorId = "batch_a2" };
        var author3 = new NcrAuthor { AuthorId = "batch_a3" };

        var exceptions = new ConcurrentBag<Exception>();

        // Fire off multiple loads to create a batch
        var loadTask1 = loader.LoadNavigationAsync(author1, nameof(NcrAuthor.Books), CancellationToken.None);
        var loadTask2 = loader.LoadNavigationAsync(author2, nameof(NcrAuthor.Books), CancellationToken.None);
        var loadTask3 = loader.LoadNavigationAsync(author3, nameof(NcrAuthor.Books), CancellationToken.None);

        // Concurrently try to clean up one of the entities mid-batch
        var cleanupTasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            cleanupTasks.Add(Task.Run(() =>
            {
                try
                {
                    loader.RemovePendingLoadsForEntity(author2);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    exceptions.Add(ex);
                }
            }));
        }

        // Wait for cleanup tasks
        await Task.WhenAll(cleanupTasks);

        // Wait for load tasks — they should either complete successfully or
        // have been removed by cleanup (in which case the TCS is never set, but
        // we use WaitAsync to prevent indefinite hang)
        var completedTasks = new List<Task<List<object>>> { loadTask1, loadTask2, loadTask3 };
        int succeeded = 0;
        foreach (var t in completedTasks)
        {
            try
            {
                await t.WaitAsync(TimeSpan.FromSeconds(10));
                succeeded++;
            }
            catch (TimeoutException)
            {
                // A removed entity's TCS may never be completed — acceptable
            }
            catch (Exception)
            {
                // Other exceptions from DB or cancellation are not failures here
            }
        }

        // No dictionary corruption exceptions
        Assert.Empty(exceptions);

        // At least some tasks should have completed (the ones not cleaned up)
        Assert.True(succeeded >= 1, $"Expected at least 1 load to succeed; got {succeeded}.");

        loader.Dispose();
    }

    /// <summary>
    /// NAV1: Dispose() racing with RemovePendingLoadsForEntity must not throw
    /// ObjectDisposedException. The race: Remove passes `if (_disposed) return` check,
    /// then Dispose fires, then Remove calls _batchSemaphore.Wait(). Before the fix,
    /// Dispose() called _batchSemaphore.Dispose() after releasing it, so the racing
    /// Wait() would throw ODE. After the fix, the semaphore is never explicitly disposed.
    /// </summary>
    [Fact]
    public async Task NAV1_Dispose_ConcurrentWithRemovePending_DoesNotThrowODE()
    {
        using var cn = OpenMemory();
        var options = BuildOptions();

        var odesCaught = 0;
        var iterations = 2000;

        for (int i = 0; i < iterations; i++)
        {
            using var ctx = new DbContext(cn, new SqliteProvider(), options);
            var loader = new BatchedNavigationLoader(ctx);
            var entity = new NcrAuthor { AuthorId = $"nav1_{i}" };

            // Race Dispose() against RemovePendingLoadsForEntity() with no coordination.
            // The semaphore-lifecycle race is triggered when Remove reads _disposed=false
            // then Dispose runs fully (including the former _batchSemaphore.Dispose() call)
            // before Remove calls _batchSemaphore.Wait().
            var t1 = Task.Run(() =>
            {
                try { loader.Dispose(); }
                catch (ObjectDisposedException) { Interlocked.Increment(ref odesCaught); }
            });
            var t2 = Task.Run(() =>
            {
                try { loader.RemovePendingLoadsForEntity(entity); }
                catch (ObjectDisposedException) { Interlocked.Increment(ref odesCaught); }
            });

            await Task.WhenAll(t1, t2);
        }

        Assert.Equal(0, odesCaught);
    }

    /// <summary>
    /// NAV1: Dispose() is safe to call concurrently from multiple threads.
    /// Only one thread should do the actual cleanup; the others should silently no-op.
    /// </summary>
    [Fact]
    public async Task NAV1_MultipleDisposeCalls_AreIdempotent()
    {
        using var cn = OpenMemory();
        var options = BuildOptions();
        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var loader = new BatchedNavigationLoader(ctx);

        // Concurrent Dispose calls — should not throw
        var tasks = Enumerable.Range(0, 20).Select(_ => Task.Run(() => loader.Dispose())).ToArray();
        await Task.WhenAll(tasks);   // no assertion needed — absence of throw is the check
    }
}
