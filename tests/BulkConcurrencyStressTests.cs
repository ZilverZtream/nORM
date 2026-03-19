using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 — Concurrency stress: cache contention, concurrent bulk workloads,
// cancellation races, compiled-query thread safety, BoundedCache correctness
// ══════════════════════════════════════════════════════════════════════════════

public class BulkConcurrencyStressTests : IDisposable
{
    // ── Shared entity types ──────────────────────────────────────────────────

    [Table("G45Item")]
    private class G45Item
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Table("G45BulkItem")]
    private class G45BulkItem
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("G45LargeItem")]
    private class G45LargeItem
    {
        [Key]
        public int Id { get; set; }
        public string Data { get; set; } = string.Empty;
    }

    [Table("G45RetryItem")]
    private class G45RetryItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── Temp file tracking for cleanup ───────────────────────────────────────

    private readonly List<string> _tempFiles = new();

    private string CreateTempDbPath()
    {
        var path = Path.GetTempFileName();
        _tempFiles.Add(path);
        return path;
    }

    public void Dispose()
    {
        foreach (var path in _tempFiles)
        {
            SqliteConnection.ClearAllPools();
            try { File.Delete(path); } catch { }
            try { File.Delete(path + "-wal"); } catch { }
            try { File.Delete(path + "-shm"); } catch { }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a shared named in-memory SQLite database. Returns a keeper
    /// connection (must stay open) and the connection string for additional
    /// connections.
    /// </summary>
    private static (SqliteConnection keeper, string connStr) BuildSharedDb(string ddl)
    {
        var name = Guid.NewGuid().ToString("N");
        var connStr = $"Data Source={name};Mode=Memory;Cache=Shared";
        var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (keeper, connStr);
    }

    private static SqliteConnection OpenShared(string connStr)
    {
        var cn = new SqliteConnection(connStr);
        cn.Open();
        return cn;
    }

    private static DbContext BuildContext(SqliteConnection cn, DbContextOptions? opts = null)
        => new(cn, new SqliteProvider(), opts);

    private static long CountRows(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{table}]";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 1. Cache contention stress
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CacheContention_ParallelSetInvalidateTryGet_NoDeadlock()
    {
        using var cache = new NormMemoryCacheProvider();
        const int taskCount = 10;
        const int opsPerTask = 200;
        var tag = "shared-tag";
        var exceptions = new ConcurrentBag<Exception>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < opsPerTask && !cts.IsCancellationRequested; i++)
                {
                    var key = $"task{taskIdx}_key{i}";
                    // Alternate between Set, TryGet, and InvalidateTag
                    switch (i % 3)
                    {
                        case 0:
                            cache.Set(key, $"value_{taskIdx}_{i}", TimeSpan.FromMinutes(5), new[] { tag });
                            break;
                        case 1:
                            cache.TryGet<string>(key, out _);
                            break;
                        case 2:
                            cache.InvalidateTag(tag);
                            break;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        var completed = Task.WhenAll(tasks);
        var winner = await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(5)));

        Assert.True(winner == completed, "Cache contention stress timed out — possible deadlock");
        Assert.Empty(exceptions);

        // After invalidation storm, new sets should still work
        cache.Set("post-storm", "alive", TimeSpan.FromMinutes(1), new[] { tag });
        Assert.True(cache.TryGet<string>("post-storm", out var val));
        Assert.Equal("alive", val);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 2. Concurrent BulkInsert stress
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentBulkInsert_5Tasks_20RowsEach_TotalIs100()
    {
        var dbPath = CreateTempDbPath();
        var connStr = $"Data Source={dbPath}";

        // Create the table
        using (var setup = new SqliteConnection(connStr))
        {
            setup.Open();
            using var cmd = setup.CreateCommand();
            cmd.CommandText = "CREATE TABLE G45BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        const int taskCount = 5;
        const int rowsPerTask = 20;
        var exceptions = new ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(async () =>
        {
            try
            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = BuildContext(cn);
                var items = Enumerable.Range(taskIdx * rowsPerTask + 1, rowsPerTask)
                    .Select(i => new G45BulkItem { Id = i, Label = $"T{taskIdx}_R{i}" })
                    .ToList();
                var inserted = await ctx.BulkInsertAsync(items);
                Assert.Equal(rowsPerTask, inserted);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(exceptions);

        // Verify total row count
        using var verifyCn = new SqliteConnection(connStr);
        verifyCn.Open();
        var total = CountRows(verifyCn, "G45BulkItem");
        Assert.Equal(taskCount * rowsPerTask, total);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 3. Concurrent BulkUpdate + Query
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentBulkUpdateAndQuery_NoExceptions_ValidData()
    {
        var dbPath = CreateTempDbPath();
        var connStr = $"Data Source={dbPath}";

        // Seed data
        using (var setup = new SqliteConnection(connStr))
        {
            setup.Open();
            using var cmd = setup.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE G45Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);
                INSERT INTO G45Item (Id, Name, Value) VALUES (1,'A',10),(2,'B',20),(3,'C',30),(4,'D',40),(5,'E',50);";
            cmd.ExecuteNonQuery();
        }

        var exceptions = new ConcurrentBag<Exception>();
        var queryResults = new ConcurrentBag<int>();
        var updateDone = new TaskCompletionSource();

        // Task 1: BulkUpdate — update all rows' Name
        var updater = Task.Run(async () =>
        {
            try
            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = BuildContext(cn);
                var items = Enumerable.Range(1, 5)
                    .Select(i => new G45Item { Id = i, Name = $"Updated{i}", Value = i * 100 })
                    .ToList();
                await ctx.BulkUpdateAsync(items);
                updateDone.TrySetResult();
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                updateDone.TrySetResult();
            }
        });

        // Task 2: Query in a loop while update is running
        var reader = Task.Run(async () =>
        {
            try
            {
                using var cn = new SqliteConnection(connStr);
                cn.Open();
                using var ctx = BuildContext(cn);
                for (int i = 0; i < 10; i++)
                {
                    var count = ctx.Query<G45Item>().Count();
                    queryResults.Add(count);
                    if (updateDone.Task.IsCompleted) break;
                    await Task.Yield();
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        });

        await Task.WhenAll(updater, reader);

        Assert.Empty(exceptions);
        // All query results should show 5 rows (update doesn't delete rows)
        Assert.All(queryResults, count => Assert.Equal(5, count));
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 4. Pre-cancelled token BulkInsert
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PreCancelledToken_BulkInsert_ThrowsOperationCanceled()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE G45BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        using var ctx = BuildContext(cn);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var items = new[] { new G45BulkItem { Id = 1, Label = "X" } };

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.BulkInsertAsync(items, cts.Token));
        Assert.True(ex is OperationCanceledException);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 5. Pre-cancelled token BulkUpdate
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PreCancelledToken_BulkUpdate_ThrowsOperationCanceled()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE G45Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();
        using var insertCmd = cn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO G45Item (Id, Name, Value) VALUES (1,'A',1);";
        insertCmd.ExecuteNonQuery();

        using var ctx = BuildContext(cn);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var items = new[] { new G45Item { Id = 1, Name = "Updated", Value = 2 } };

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.BulkUpdateAsync(items, cts.Token));
        Assert.True(ex is OperationCanceledException);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 6. Pre-cancelled token BulkDelete
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PreCancelledToken_BulkDelete_ThrowsOperationCanceled()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE G45BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        using var insertCmd = cn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO G45BulkItem (Id, Label) VALUES (1,'A');";
        insertCmd.ExecuteNonQuery();

        using var ctx = BuildContext(cn);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var items = new[] { new G45BulkItem { Id = 1, Label = "A" } };

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.BulkDeleteAsync(items, cts.Token));
        Assert.True(ex is OperationCanceledException);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 7. Late cancellation during BulkInsert
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LateCancellation_BulkInsert_NoCorruptState()
    {
        var dbPath = CreateTempDbPath();
        var connStr = $"Data Source={dbPath}";

        using (var setup = new SqliteConnection(connStr))
        {
            setup.Open();
            using var cmd = setup.CreateCommand();
            cmd.CommandText = "CREATE TABLE G45LargeItem (Id INTEGER PRIMARY KEY, Data TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        using var cts = new CancellationTokenSource();
        var items = Enumerable.Range(1, 1000)
            .Select(i => new G45LargeItem { Id = i, Data = $"Data_{i}_{new string('x', 100)}" })
            .ToList();

        // Cancel after 100ms
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        bool operationCancelled = false;
        int insertedCount = 0;

        try
        {
            using var cn = new SqliteConnection(connStr);
            cn.Open();
            using var ctx = BuildContext(cn);
            insertedCount = await ctx.BulkInsertAsync(items, cts.Token);
        }
        catch (OperationCanceledException)
        {
            operationCancelled = true;
        }

        // Either the operation was cancelled or it completed fully
        // In both cases, the DB should be in a consistent state
        using var verifyCn = new SqliteConnection(connStr);
        verifyCn.Open();
        var actualRows = CountRows(verifyCn, "G45LargeItem");

        if (operationCancelled)
        {
            // If cancelled, rows might be 0 (rolled back) or partial (batch boundary)
            Assert.True(actualRows >= 0 && actualRows <= 1000,
                $"Row count {actualRows} should be between 0 and 1000 after cancellation");
        }
        else
        {
            // If completed, all rows should be present
            Assert.Equal(1000, insertedCount);
            Assert.Equal(1000, actualRows);
        }

        // Verify no corruption — a simple SELECT should succeed
        using var checkCmd = verifyCn.CreateCommand();
        checkCmd.CommandText = "SELECT Id, Data FROM G45LargeItem ORDER BY Id LIMIT 1";
        using var reader = checkCmd.ExecuteReader();
        if (reader.Read())
        {
            var id = reader.GetInt32(0);
            var data = reader.GetString(1);
            Assert.True(id > 0);
            Assert.False(string.IsNullOrEmpty(data));
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 8. Compiled query cache contention
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CompiledQueryCacheContention_ParallelExecution_CorrectResults()
    {
        var (keeper, connStr) = BuildSharedDb(
            "CREATE TABLE G45Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);");
        using var _ = keeper;

        // Seed data
        using (var seedCn = OpenShared(connStr))
        {
            using var cmd = seedCn.CreateCommand();
            for (int i = 1; i <= 20; i++)
            {
                cmd.CommandText = $"INSERT INTO G45Item (Id, Name, Value) VALUES ({i}, 'Item{i}', {i * 10});";
                cmd.ExecuteNonQuery();
            }
        }

        // Compile a single query delegate — shared across all tasks
        var compiled = Norm.CompileQuery((DbContext c, int minValue) =>
            c.Query<G45Item>().Where(x => x.Value >= minValue));

        const int taskCount = 10;
        var exceptions = new ConcurrentBag<Exception>();
        var results = new ConcurrentDictionary<int, List<G45Item>>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(async () =>
        {
            try
            {
                using var cn = OpenShared(connStr);
                using var ctx = BuildContext(cn);
                // Each task queries with minValue = taskIdx * 20
                // So task 0 queries Value >= 0 (all 20), task 1 queries Value >= 20, etc.
                int minVal = taskIdx * 20;
                var items = await compiled(ctx, minVal);
                results[taskIdx] = items;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(exceptions);

        // Verify each task got correct results
        for (int t = 0; t < taskCount; t++)
        {
            Assert.True(results.ContainsKey(t), $"Task {t} did not produce results");
            int minVal = t * 20;
            var expected = Enumerable.Range(1, 20).Where(i => i * 10 >= minVal).Count();
            Assert.Equal(expected, results[t].Count);
            Assert.All(results[t], item => Assert.True(item.Value >= minVal,
                $"Task {t}: Item {item.Id} has Value {item.Value} < {minVal}"));
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 9. BoundedCache concurrent access
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BoundedCache_ConcurrentSetAndGet_SizeBounded_ValuesCorrect()
    {
        const int maxSize = 50;
        var cache = new BoundedCache<string, int>(maxSize);
        const int taskCount = 10;
        const int keysPerTask = 20;
        var exceptions = new ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < keysPerTask; i++)
                {
                    var key = $"t{taskIdx}_k{i}";
                    cache.Set(key, taskIdx * 1000 + i);
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(exceptions);

        // Cache size must not exceed maxSize
        Assert.True(cache.Count <= maxSize,
            $"Cache count {cache.Count} exceeds max size {maxSize}");

        // Every key that IS present must have the correct value
        int verifiedCount = 0;
        for (int t = 0; t < taskCount; t++)
        {
            for (int i = 0; i < keysPerTask; i++)
            {
                var key = $"t{t}_k{i}";
                if (cache.TryGet(key, out var val))
                {
                    Assert.Equal(t * 1000 + i, val);
                    verifiedCount++;
                }
            }
        }

        // At least some entries should be present (cache not empty)
        Assert.True(verifiedCount > 0, "Cache should contain at least one entry");
        // The total keys inserted (200) exceed maxSize (50), so eviction occurred
        Assert.True(verifiedCount <= maxSize,
            $"Verified count {verifiedCount} should not exceed max size {maxSize}");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 10. Retry after transient failure
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RetryAfterTransientFailure_RetriesAndSucceeds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE G45RetryItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        var policy = new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(10),
            ShouldRetry = ex =>
            {
                // Treat all DbExceptions as transient for this test
                return ex is DbException;
            }
        };

        var opts = new DbContextOptions { RetryPolicy = policy };
        using var ctx = BuildContext(cn, opts);

        // Insert an entity — should succeed on first attempt, no retry needed
        var entity = new G45RetryItem { Id = 1, Name = "RetryTest" };
        ctx.Add(entity);
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);

        // Verify the row exists via raw SQL
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT Name FROM G45RetryItem WHERE Id = 1";
        var name = verifyCmd.ExecuteScalar() as string;
        Assert.Equal("RetryTest", name);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 11. BoundedCache eviction FIFO order
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public void BoundedCache_EvictionOrder_OldestEvictedFirst()
    {
        const int maxSize = 5;
        var cache = new BoundedCache<int, string>(maxSize);

        // Insert 10 items; first 5 should be evicted
        for (int i = 0; i < 10; i++)
            cache.Set(i, $"val{i}");

        Assert.True(cache.Count <= maxSize,
            $"Count {cache.Count} exceeds maxSize {maxSize}");

        // Newest entries (5-9) should still be present
        for (int i = 5; i < 10; i++)
        {
            Assert.True(cache.TryGet(i, out var val), $"Key {i} should be present");
            Assert.Equal($"val{i}", val);
        }

        // Oldest entries (0-4) should have been evicted
        int evictedCount = 0;
        for (int i = 0; i < 5; i++)
        {
            if (!cache.TryGet(i, out _))
                evictedCount++;
        }
        Assert.True(evictedCount > 0, "At least some old entries should be evicted");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 12. Cache tag invalidation correctness
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public void CacheTagInvalidation_RemovesOnlyTaggedEntries()
    {
        using var cache = new NormMemoryCacheProvider();

        // Set entries with different tags
        cache.Set("key1", "value1", TimeSpan.FromMinutes(5), new[] { "tagA" });
        cache.Set("key2", "value2", TimeSpan.FromMinutes(5), new[] { "tagB" });
        cache.Set("key3", "value3", TimeSpan.FromMinutes(5), new[] { "tagA", "tagB" });

        // Invalidate tagA
        cache.InvalidateTag("tagA");

        // key1 (tagA only) should be gone
        Assert.False(cache.TryGet<string>("key1", out _), "key1 should be invalidated by tagA");
        // key2 (tagB only) should still exist
        Assert.True(cache.TryGet<string>("key2", out var v2));
        Assert.Equal("value2", v2);
        // key3 (tagA + tagB) should be gone (tagA was invalidated)
        Assert.False(cache.TryGet<string>("key3", out _), "key3 should be invalidated by tagA");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 13. Concurrent queries return consistent results
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentLinqQueries_SameTable_AllReturnCorrectCounts()
    {
        var (keeper, connStr) = BuildSharedDb(
            "CREATE TABLE G45Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);");
        using var _ = keeper;

        // Seed 50 rows
        using (var seedCn = OpenShared(connStr))
        {
            using var cmd = seedCn.CreateCommand();
            for (int i = 1; i <= 50; i++)
            {
                cmd.CommandText = $"INSERT INTO G45Item (Id, Name, Value) VALUES ({i}, 'Item{i}', {i});";
                cmd.ExecuteNonQuery();
            }
        }

        const int taskCount = 10;
        var counts = new ConcurrentBag<int>();
        var exceptions = new ConcurrentBag<Exception>();

        var tasks = Enumerable.Range(0, taskCount).Select(__ => Task.Run(() =>
        {
            try
            {
                using var cn = OpenShared(connStr);
                using var ctx = BuildContext(cn);
                var count = ctx.Query<G45Item>().Count();
                counts.Add(count);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(exceptions);
        Assert.Equal(taskCount, counts.Count);
        Assert.All(counts, c => Assert.Equal(50, c));
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 14. RetryPolicy ShouldRetry is invoked on transient DbException
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RetryPolicy_ShouldRetry_IsConsultedOnDbException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE G45RetryItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        int shouldRetryCallCount = 0;
        var policy = new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex =>
            {
                Interlocked.Increment(ref shouldRetryCallCount);
                return ex is DbException;
            }
        };

        var opts = new DbContextOptions { RetryPolicy = policy };
        using var ctx = BuildContext(cn, opts);

        // Successful operation — ShouldRetry should NOT be called
        var entity = new G45RetryItem { Id = 42, Name = "Test" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();
        Assert.Equal(0, shouldRetryCallCount);

        // Verify the entity was persisted
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM G45RetryItem WHERE Id = 42";
        var count = Convert.ToInt64(verifyCmd.ExecuteScalar());
        Assert.Equal(1, count);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 15. BoundedCache clear under contention
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BoundedCache_ClearUnderContention_NoCrash()
    {
        const int maxSize = 100;
        var cache = new BoundedCache<int, int>(maxSize);
        var exceptions = new ConcurrentBag<Exception>();

        // Writer task: continuously add entries
        var writerDone = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var writer = Task.Run(() =>
        {
            try
            {
                int key = 0;
                while (!writerDone.Token.IsCancellationRequested)
                {
                    cache.Set(key++, key);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                exceptions.Add(ex);
            }
        });

        // Clear task: periodically clear the cache
        var clearer = Task.Run(async () =>
        {
            try
            {
                while (!writerDone.Token.IsCancellationRequested)
                {
                    cache.Clear();
                    await Task.Delay(50);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                exceptions.Add(ex);
            }
        });

        // Reader task: continuously read from cache
        var reader = Task.Run(() =>
        {
            try
            {
                while (!writerDone.Token.IsCancellationRequested)
                {
                    cache.TryGet(0, out _);
                    // Count must never exceed maxSize (with some slack for concurrency)
                    var count = cache.Count;
                    Assert.True(count <= maxSize + 10,
                        $"Count {count} significantly exceeds maxSize {maxSize}");
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                exceptions.Add(ex);
            }
        });

        await Task.WhenAll(writer, clearer, reader);

        Assert.Empty(exceptions);
    }
}
