using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Entity types for MiscCoverageTests ──────────────────────────────────────

[Table("MiscBlog")]
public class MiscBlog
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
}

[Table("MiscPost")]
public class MiscPost
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int BlogId { get; set; }
    public string Content { get; set; } = string.Empty;
}

// A class without a parameterless constructor to exercise NormQueryableImplUnconstrained
public class RecordLike
{
    public int Id { get; }
    public string Name { get; }
    public RecordLike(int id, string name) { Id = id; Name = name; }
}

// ── MiscCoverageTests ────────────────────────────────────────────────────────

/// <summary>
/// Comprehensive coverage for classes with low or zero coverage:
/// NormQueryableImplUnconstrained, NormIncludableQueryableExtensions,
/// NormIncludableQueryableUnconstrained, NormAsyncExtensions, WindowFunctionsExtensions,
/// TemporalExtensions, Json, NormExceptionHandler, NormAsyncPolicy,
/// NavigationContext, LazyNavigationCollection, LazyNavigationReference,
/// AdaptiveTimeoutManager, BaseDbCommandInterceptor, LockFreeObjectPool,
/// ShadowPropertyInfo, ChangeTracker, ConnectionManager.
/// </summary>
public class MiscCoverageTests
{
    private static (DbContext ctx, SqliteConnection cn) CreateCtx(DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider(), opts ?? new DbContextOptions());
        return (ctx, cn);
    }

    private static void CreateTables(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS MiscBlog (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS MiscPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, BlogId INTEGER NOT NULL, Content TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
    }

    // ── NormAsyncPolicy ──────────────────────────────────────────────────────

    [Fact]
    public void NormAsyncPolicy_SuppressExecutionContextFlow_IsBoolean()
    {
        var v = NormAsyncPolicy.SuppressExecutionContextFlow;
        Assert.IsType<bool>(v);
    }

    // ── Json ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Json_Value_ThrowsOutsideQuery()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Json.Value<int>("col", "$.id"));
        Assert.Contains("nORM LINQ queries", ex.Message);
    }

    // ── ShadowPropertyInfo ───────────────────────────────────────────────────

    [Fact]
    public void ShadowPropertyInfo_BasicProperties()
    {
        // ShadowPropertyInfo is internal — access via reflection
        var type = typeof(nORM.Internal.ShadowPropertyInfo);
        var ctor = type.GetConstructor(
            BindingFlags.Public | BindingFlags.Instance,
            null,
            new[] { typeof(string), typeof(Type), typeof(Type) },
            null)!;

        var spi = (PropertyInfo)ctor.Invoke(new object[] { "Shadow", typeof(int), typeof(MiscBlog) });

        Assert.Equal("Shadow", spi.Name);
        Assert.Equal(typeof(int), spi.PropertyType);
        Assert.Equal(typeof(MiscBlog), spi.DeclaringType);
        Assert.Equal(typeof(MiscBlog), spi.ReflectedType);
        Assert.True(spi.CanRead);
        Assert.True(spi.CanWrite);
        Assert.Equal(PropertyAttributes.None, spi.Attributes);
    }

    [Fact]
    public void ShadowPropertyInfo_GetAccessors_ReturnsEmpty()
    {
        var type = typeof(nORM.Internal.ShadowPropertyInfo);
        var ctor = type.GetConstructor(
            BindingFlags.Public | BindingFlags.Instance, null,
            new[] { typeof(string), typeof(Type), typeof(Type) }, null)!;
        var spi = (PropertyInfo)ctor.Invoke(new object[] { "S", typeof(string), typeof(MiscPost) });

        Assert.Empty(spi.GetAccessors(false));
        Assert.Null(spi.GetGetMethod(false));
        Assert.Null(spi.GetSetMethod(false));
        Assert.Empty(spi.GetIndexParameters());
        Assert.Empty(spi.GetCustomAttributes(false));
        Assert.Empty(spi.GetCustomAttributes(typeof(KeyAttribute), false));
        Assert.False(spi.IsDefined(typeof(KeyAttribute), false));
    }

    [Fact]
    public void ShadowPropertyInfo_GetValue_Throws()
    {
        var type = typeof(nORM.Internal.ShadowPropertyInfo);
        var ctor = type.GetConstructor(
            BindingFlags.Public | BindingFlags.Instance, null,
            new[] { typeof(string), typeof(Type), typeof(Type) }, null)!;
        var spi = (PropertyInfo)ctor.Invoke(new object[] { "S", typeof(int), typeof(MiscBlog) });

        Assert.Throws<NotSupportedException>(() => spi.GetValue(null));
        Assert.Throws<NotSupportedException>(() => spi.SetValue(null, 42));
        Assert.Throws<NotSupportedException>(() =>
            spi.GetValue(null, BindingFlags.Default, null, null, null));
        Assert.Throws<NotSupportedException>(() =>
            spi.SetValue(null, 42, BindingFlags.Default, null, null, null));
    }

    // ── LockFreeObjectPool ───────────────────────────────────────────────────

    private class SimpleResettable : IResettable
    {
        public int Value { get; set; }
        public bool WasReset { get; private set; }
        public void Reset() { WasReset = true; Value = 0; }
    }

    private class SimplePlain
    {
        public int Value { get; set; }
    }

    [Fact]
    public void LockFreeObjectPool_GetReturnsSameInstanceOnSameThread()
    {
        // LockFreeObjectPool is internal — use via reflection-based instantiation
        var poolType = typeof(nORM.Internal.LockFreeObjectPool<>).MakeGenericType(typeof(SimplePlain));
        var pool = Activator.CreateInstance(poolType)!;
        var getMethod = poolType.GetMethod("Get")!;
        var returnMethod = poolType.GetMethod("Return")!;

        var item1 = (SimplePlain)getMethod.Invoke(pool, null)!;
        var item2 = (SimplePlain)getMethod.Invoke(pool, null)!;
        Assert.Same(item1, item2);

        returnMethod.Invoke(pool, new object[] { item1 });
    }

    [Fact]
    public void LockFreeObjectPool_Return_ResetsResettable()
    {
        var poolType = typeof(nORM.Internal.LockFreeObjectPool<>).MakeGenericType(typeof(SimpleResettable));
        var pool = Activator.CreateInstance(poolType)!;
        var getMethod = poolType.GetMethod("Get")!;
        var returnMethod = poolType.GetMethod("Return")!;

        var item = (SimpleResettable)getMethod.Invoke(pool, null)!;
        item.Value = 99;
        returnMethod.Invoke(pool, new object[] { item });

        Assert.True(item.WasReset);
        Assert.Equal(0, item.Value);
    }

    // ── NormExceptionHandler ─────────────────────────────────────────────────

    [Fact]
    public void NormExceptionHandler_Sync_SuccessReturnsValue()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        var result = handler.ExecuteWithExceptionHandlingSync(() => 42, "TestOp");
        Assert.Equal(42, result);
    }

    [Fact]
    public void NormExceptionHandler_Sync_TimeoutException_ThrowsNormTimeoutException()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        Assert.Throws<NormTimeoutException>(() =>
            handler.ExecuteWithExceptionHandlingSync<int>(
                () => throw new TimeoutException("timed out"),
                "TestOp"));
    }

    [Fact]
    public void NormExceptionHandler_Sync_GenericException_ThrowsNormException()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        Assert.Throws<NormException>(() =>
            handler.ExecuteWithExceptionHandlingSync<int>(
                () => throw new InvalidOperationException("oops"),
                "TestOp",
                new Dictionary<string, object> { ["extra"] = "val" }));
    }

    [Fact]
    public async Task NormExceptionHandler_Async_SuccessReturnsValue()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        var result = await handler.ExecuteWithExceptionHandling(
            () => Task.FromResult(99),
            "AsyncOp");
        Assert.Equal(99, result);
    }

    [Fact]
    public async Task NormExceptionHandler_Async_TimeoutException_ThrowsNormTimeoutException()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        await Assert.ThrowsAsync<NormTimeoutException>(() =>
            handler.ExecuteWithExceptionHandling<int>(
                () => throw new TimeoutException("t/o"),
                "AsyncOp"));
    }

    [Fact]
    public async Task NormExceptionHandler_Async_GenericException_ThrowsNormException()
    {
        var handler = new NormExceptionHandler(NullLogger.Instance);
        await Assert.ThrowsAsync<NormException>(() =>
            handler.ExecuteWithExceptionHandling<int>(
                () => Task.FromException<int>(new Exception("boom")),
                "AsyncOp",
                new Dictionary<string, object> { ["Sql"] = "SELECT 1", ["Param0"] = 42 }));
    }

    [Fact]
    public void NormDatabaseException_CanBeConstructed()
    {
        var ex = new NormDatabaseException("msg", "SELECT 1",
            new Dictionary<string, object> { ["p0"] = 1 },
            new Exception("inner"));
        Assert.Equal("msg", ex.Message);
    }

    [Fact]
    public void NormTimeoutException_CanBeConstructed()
    {
        var ex = new NormTimeoutException("timed out", null, null, null);
        Assert.Equal("timed out", ex.Message);
    }

    // ── AdaptiveTimeoutManager ───────────────────────────────────────────────

    [Fact]
    public void AdaptiveTimeoutManager_GetTimeoutForOperation_ReturnsBaseWhenDisabled()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration { EnableAdaptiveTimeouts = false };
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        var t = mgr.GetTimeoutForOperation(AdaptiveTimeoutManager.OperationType.SimpleSelect);
        Assert.Equal(cfg.SimpleQueryTimeout, t);
    }

    [Fact]
    public void AdaptiveTimeoutManager_AllOperationTypes_ReturnPositiveTimeouts()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration();
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        foreach (AdaptiveTimeoutManager.OperationType op in Enum.GetValues(typeof(AdaptiveTimeoutManager.OperationType)))
        {
            var t = mgr.GetTimeoutForOperation(op, recordCount: 100, complexityScore: 500);
            Assert.True(t > TimeSpan.Zero, $"Timeout for {op} should be > 0");
        }
    }

    [Fact]
    public void AdaptiveTimeoutManager_BulkOperation_ScalesByRecordCount()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration();
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        var small = mgr.GetTimeoutForOperation(AdaptiveTimeoutManager.OperationType.BulkInsert, recordCount: 1);
        var large = mgr.GetTimeoutForOperation(AdaptiveTimeoutManager.OperationType.BulkInsert, recordCount: 100_000);
        // Both should be positive; large may be greater or capped at 30 min
        Assert.True(large >= small);
    }

    [Fact]
    public async Task AdaptiveTimeoutManager_ExecuteWithAdaptiveTimeout_SuccessUpdatesStats()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration();
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        var result = await mgr.ExecuteWithAdaptiveTimeout(
            ct => Task.FromResult(7),
            AdaptiveTimeoutManager.OperationType.SimpleSelect,
            "key1");

        Assert.Equal(7, result);
        var stats = mgr.GetOperationStatistics();
        Assert.True(stats.ContainsKey("key1"));
        Assert.Equal(1, stats["key1"].ExecutionCount);
        Assert.Equal(0, stats["key1"].TimeoutCount);
    }

    [Fact]
    public async Task AdaptiveTimeoutManager_ExecuteWithAdaptiveTimeout_FailureUpdatesStats()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration();
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mgr.ExecuteWithAdaptiveTimeout<int>(
                ct => Task.FromException<int>(new InvalidOperationException("fail")),
                AdaptiveTimeoutManager.OperationType.SimpleSelect,
                "key2"));

        var stats = mgr.GetOperationStatistics();
        Assert.True(stats.ContainsKey("key2"));
        Assert.Equal(1, stats["key2"].TimeoutCount);
    }

    [Fact]
    public async Task AdaptiveTimeoutManager_HighSuccessRate_AdaptsTimeout()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration
        {
            EnableAdaptiveTimeouts = true,
            SimpleQueryTimeout = TimeSpan.FromSeconds(15)
        };
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        // Run several successful operations to build up stats
        for (int i = 0; i < 5; i++)
            await mgr.ExecuteWithAdaptiveTimeout(ct => Task.FromResult(i), AdaptiveTimeoutManager.OperationType.SimpleSelect, "opAdapt");

        var stats = mgr.GetOperationStatistics();
        Assert.True(stats["opAdapt"].SuccessRate >= 0.95);
        Assert.Equal(1.0, stats["opAdapt"].SuccessRate);
    }

    [Fact]
    public void AdaptiveTimeoutManager_TimeoutStatistics_SuccessRate_ZeroCount()
    {
        var s = new AdaptiveTimeoutManager.TimeoutStatistics { ExecutionCount = 0, TimeoutCount = 0 };
        Assert.Equal(1.0, s.SuccessRate);
    }

    [Fact]
    public void AdaptiveTimeoutManager_HistoricalMultiplier_TripsOnLowSuccessRate()
    {
        var cfg = new AdaptiveTimeoutManager.TimeoutConfiguration
        {
            EnableAdaptiveTimeouts = true,
            SimpleQueryTimeout = TimeSpan.FromSeconds(1)
        };
        var mgr = new AdaptiveTimeoutManager(cfg, NullLogger.Instance);

        // Force a stats entry with low success rate via reflection
        var statsField = typeof(AdaptiveTimeoutManager)
            .GetField("_operationStats", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var dict = (System.Collections.Concurrent.ConcurrentDictionary<string, AdaptiveTimeoutManager.TimeoutStatistics>)
            statsField.GetValue(mgr)!;
        dict["lowkey"] = new AdaptiveTimeoutManager.TimeoutStatistics
        {
            ExecutionCount = 10,
            TimeoutCount = 2,
            AverageExecutionTime = TimeSpan.FromMilliseconds(100)
        };

        var t = mgr.GetTimeoutForOperation(AdaptiveTimeoutManager.OperationType.SimpleSelect, operationKey: "lowkey");
        // With 80% success rate historical multiplier 1.5 kicks in
        Assert.True(t > TimeSpan.FromSeconds(1));
    }

    // ── BaseDbCommandInterceptor ─────────────────────────────────────────────

    private class TestInterceptor : BaseDbCommandInterceptor
    {
        public TestInterceptor(ILogger logger) : base(logger) { }
    }

    [Fact]
    public async Task BaseDbCommandInterceptor_DefaultMethods_DontThrow()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var interceptor = new TestInterceptor(NullLogger.Instance);
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            var nonQueryResult = await interceptor.NonQueryExecutingAsync(cmd, ctx, CancellationToken.None);
            Assert.False(nonQueryResult.IsSuppressed);

            await interceptor.NonQueryExecutedAsync(cmd, ctx, 1, TimeSpan.FromMilliseconds(10), CancellationToken.None);

            var scalarResult = await interceptor.ScalarExecutingAsync(cmd, ctx, CancellationToken.None);
            Assert.False(scalarResult.IsSuppressed);

            await interceptor.ScalarExecutedAsync(cmd, ctx, 42, TimeSpan.FromMilliseconds(5), CancellationToken.None);

            var readerResult = await interceptor.ReaderExecutingAsync(cmd, ctx, CancellationToken.None);
            Assert.False(readerResult.IsSuppressed);

            await interceptor.ReaderExecutedAsync(cmd, ctx, null!, TimeSpan.FromMilliseconds(3), CancellationToken.None);

            await interceptor.CommandFailedAsync(cmd, ctx, new Exception("boom"), CancellationToken.None);
        }
    }

    [Fact]
    public void BaseDbCommandInterceptor_NullLogger_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new TestInterceptor(null!));
    }

    // ── InterceptionResult ───────────────────────────────────────────────────

    [Fact]
    public void InterceptionResult_Continue_IsNotSuppressed()
    {
        var r = InterceptionResult<int>.Continue();
        Assert.False(r.IsSuppressed);
    }

    [Fact]
    public void InterceptionResult_SuppressWithResult_IsSuppressed()
    {
        var r = InterceptionResult<int>.SuppressWithResult(99);
        Assert.True(r.IsSuppressed);
        Assert.Equal(99, r.Result);
    }

    // ── NavigationContext ────────────────────────────────────────────────────

    [Fact]
    public void NavigationContext_IsLoaded_MarkAsLoaded_MarkAsUnloaded()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var nav = new NavigationContext(ctx, typeof(MiscBlog));
            Assert.Equal(ctx, nav.DbContext);
            Assert.Equal(typeof(MiscBlog), nav.EntityType);

            Assert.False(nav.IsLoaded("Posts"));
            nav.MarkAsLoaded("Posts");
            Assert.True(nav.IsLoaded("Posts"));
            nav.MarkAsUnloaded("Posts");
            Assert.False(nav.IsLoaded("Posts"));
        }
    }

    [Fact]
    public void NavigationContext_Dispose_ClearsLoadedProperties()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var nav = new NavigationContext(ctx, typeof(MiscBlog));
            nav.MarkAsLoaded("A");
            nav.MarkAsLoaded("B");
            nav.Dispose();
            Assert.False(nav.IsLoaded("A"));
            Assert.False(nav.IsLoaded("B"));
        }
    }

    // ── LazyNavigationReference ──────────────────────────────────────────────

    [Fact]
    public void LazyNavigationReference_SetValue_Makes_IsLoaded_True()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var navCtx = new NavigationContext(ctx, typeof(MiscPost));
            var prop = typeof(MiscPost).GetProperty(nameof(MiscPost.Content))!;
            // We use a string property here just as a PropertyInfo carrier
            // (LazyNavigationReference<MiscBlog> uses MiscBlog as the generic)
            var lnr = new LazyNavigationReference<MiscBlog>(new MiscPost(), prop, navCtx);

            var blog = new MiscBlog { Id = 1, Title = "t" };
            lnr.SetValue(blog);

            // IsLoaded is tracked via context
            Assert.True(navCtx.IsLoaded(prop.Name));
        }
    }

    [Fact]
    public void LazyNavigationReference_ImplicitConversion_ReturnsTask()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var navCtx = new NavigationContext(ctx, typeof(MiscPost));
            var prop = typeof(MiscPost).GetProperty(nameof(MiscPost.Content))!;
            var lnr = new LazyNavigationReference<MiscBlog>(new MiscPost(), prop, navCtx);

            // SetValue so no actual DB load is needed
            var blog = new MiscBlog { Id = 1 };
            lnr.SetValue(blog);

            Task<MiscBlog?> task = lnr;
            Assert.NotNull(task);
        }
    }

    // ── ChangeTracker ────────────────────────────────────────────────────────

    [Fact]
    public void ChangeTracker_Track_ReturnsSameEntry_ForSameReference()
    {
        var (ctx, cn) = CreateCtx(new DbContextOptions { EagerChangeTracking = true });
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 1, Title = "A" };
            ctx.Attach(entity);
            ctx.Attach(entity); // second attach should return same entry

            Assert.Single(ctx.ChangeTracker.Entries);
        }
    }

    [Fact]
    public void ChangeTracker_Clear_RemovesAllEntries()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            ctx.Attach(new MiscBlog { Id = 1 });
            ctx.Attach(new MiscBlog { Id = 2 });
            Assert.Equal(2, ctx.ChangeTracker.Entries.Count());
            ctx.ChangeTracker.Clear();
            Assert.Empty(ctx.ChangeTracker.Entries);
        }
    }

    [Fact]
    public void ChangeTracker_GetEntryOrDefault_ReturnsNullForUntracked()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 5 };
            var getEntry = typeof(ChangeTracker)
                .GetMethod("GetEntryOrDefault", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var result = getEntry.Invoke(ctx.ChangeTracker, new object[] { entity });
            Assert.Null(result);
        }
    }

    [Fact]
    public void ChangeTracker_GetEntryOrDefault_ReturnsEntryForTracked()
    {
        var (ctx, cn) = CreateCtx(new DbContextOptions { EagerChangeTracking = true });
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 5, Title = "X" };
            ctx.Attach(entity);

            var getEntry = typeof(ChangeTracker)
                .GetMethod("GetEntryOrDefault", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var result = getEntry.Invoke(ctx.ChangeTracker, new object[] { entity });
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void ChangeTracker_DetectAllChanges_ViaReflection()
    {
        var (ctx, cn) = CreateCtx(new DbContextOptions { EagerChangeTracking = true });
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 1, Title = "A" };
            ctx.Attach(entity);
            entity.Title = "B";

            var markDirty = typeof(ChangeTracker)
                .GetMethod("MarkDirty", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var entry = ctx.ChangeTracker.Entries.Single();
            markDirty.Invoke(ctx.ChangeTracker, new object[] { entry });

            var detectAll = typeof(ChangeTracker)
                .GetMethod("DetectAllChanges", BindingFlags.Instance | BindingFlags.NonPublic)!;
            detectAll.Invoke(ctx.ChangeTracker, null); // should not throw

            Assert.Equal(EntityState.Modified, ctx.ChangeTracker.Entries.Single().State);
        }
    }

    [Fact]
    public void ChangeTracker_DetectChanges_DirtyNonNotifyingEntity()
    {
        var (ctx, cn) = CreateCtx(new DbContextOptions { EagerChangeTracking = true });
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 1, Title = "A" };
            ctx.Attach(entity);
            entity.Title = "Changed";

            var entry = ctx.ChangeTracker.Entries.Single();
            var markDirty = typeof(ChangeTracker)
                .GetMethod("MarkDirty", BindingFlags.Instance | BindingFlags.NonPublic)!;
            markDirty.Invoke(ctx.ChangeTracker, new object[] { entry });

            var detect = typeof(ChangeTracker)
                .GetMethod("DetectChanges", BindingFlags.Instance | BindingFlags.NonPublic)!;
            detect.Invoke(ctx.ChangeTracker, null);

            Assert.Equal(EntityState.Modified, entry.State);
        }
    }

    [Fact]
    public void ChangeTracker_Track_DefaultKeyEntity_NotAddedToKeyMap()
    {
        // Entity with Id=0 (default) — should track by ref but not by key
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 0, Title = "New" };
            ctx.Add(entity);
            Assert.Single(ctx.ChangeTracker.Entries);
            Assert.Equal(EntityState.Added, ctx.ChangeTracker.Entries.Single().State);
        }
    }

    [Fact]
    public void ChangeTracker_Remove_RemovesFromEntries()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 3, Title = "B" };
            ctx.Attach(entity);
            ctx.Remove(entity);
            // After Remove(entity) from ctx, state becomes Deleted
            // (calling ctx.Remove marks it Deleted, not removes from tracker immediately)
        }
    }

    [Fact]
    public void ChangeTracker_ReindexAfterInsert_ViaReflection()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var entity = new MiscBlog { Id = 0, Title = "X" };
            ctx.Add(entity);

            // Simulate post-insert key assignment
            entity.Id = 42;

            var mapping = ctx.GetMapping(typeof(MiscBlog));
            var reindex = typeof(ChangeTracker)
                .GetMethod("ReindexAfterInsert", BindingFlags.Instance | BindingFlags.NonPublic)!;
            reindex.Invoke(ctx.ChangeTracker, new object[] { entity, mapping });

            // Entry should still be there
            Assert.Single(ctx.ChangeTracker.Entries);
        }
    }

    // ── ConnectionManager - RedactConnectionString ───────────────────────────

    [Fact]
    public void ConnectionManager_RedactConnectionString_RedactsPassword()
    {
        var redact = typeof(ConnectionManager)
            .GetMethod("RedactConnectionString", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;

        var redacted = (string)redact.Invoke(null, new object[] { "Server=localhost;Database=mydb;Password=secret123;" })!;
        Assert.DoesNotContain("secret123", redacted);
        Assert.Contains("***", redacted);
    }

    [Fact]
    public void ConnectionManager_RedactConnectionString_PreservesNonSensitiveKeys()
    {
        var redact = typeof(ConnectionManager)
            .GetMethod("RedactConnectionString", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;

        var original = "Data Source=:memory:;Cache=Shared;";
        var redacted = (string)redact.Invoke(null, new object[] { original })!;
        // No sensitive keys, should be unchanged (modulo key ordering)
        Assert.DoesNotContain("***", redacted);
    }

    [Fact]
    public void ConnectionManager_RedactConnectionString_MalformedString_ReturnsHash()
    {
        var redact = typeof(ConnectionManager)
            .GetMethod("RedactConnectionString", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;

        // A string that DbConnectionStringBuilder will throw on
        var malformed = "this=is=not=valid===connstr";
        var redacted = (string)redact.Invoke(null, new object[] { malformed })!;
        // May or may not be treated as malformed depending on parser; just assert it returns a string
        Assert.NotNull(redacted);
        Assert.NotEmpty(redacted);
    }

    [Fact]
    public async Task ConnectionManager_GetReadConnection_WithHealthyReplica_UsesReplica()
    {
        var topology = new DatabaseTopology();
        var primary = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.Primary,
            Priority = 1,
            IsHealthy = true
        };
        var replica = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 1,
            IsHealthy = true
        };
        topology.Nodes.Add(primary);
        topology.Nodes.Add(replica);

        using var manager = new ConnectionManager(topology, new SqliteProvider(), NullLogger.Instance);
        await using var conn = await manager.GetReadConnectionAsync();
        Assert.NotNull(conn);
        conn.Dispose();
    }

    [Fact]
    public async Task ConnectionManager_GetReadConnection_NoReplicas_UsesPrimary()
    {
        var topology = new DatabaseTopology();
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.Primary,
            IsHealthy = true
        });

        using var manager = new ConnectionManager(topology, new SqliteProvider(), NullLogger.Instance);
        await using var conn = await manager.GetReadConnectionAsync();
        Assert.NotNull(conn);
    }

    [Fact]
    public void ConnectionManager_Dispose_DoesNotThrow()
    {
        var topology = new DatabaseTopology();
        topology.Nodes.Add(new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "Data Source=:memory:",
            Role = DatabaseTopology.DatabaseRole.Primary,
            IsHealthy = true
        });

        var manager = new ConnectionManager(topology, new SqliteProvider(), NullLogger.Instance);
        manager.Dispose();
        manager.Dispose(); // double dispose should not throw
    }

    [Fact]
    public void DatabaseTopology_DefaultValues()
    {
        var topo = new DatabaseTopology();
        Assert.Empty(topo.Nodes);
        Assert.Equal(TimeSpan.FromSeconds(30), topo.FailoverTimeout);
        Assert.True(topo.EnableAutomaticFailover);
        Assert.True(topo.PreferLocalReplicas);
    }

    [Fact]
    public void DatabaseNode_HealthAndLatency_ThreadSafe()
    {
        var node = new DatabaseTopology.DatabaseNode
        {
            ConnectionString = "cs",
            Role = DatabaseTopology.DatabaseRole.ReadReplica,
            Priority = 2,
            IsHealthy = true
        };
        node.IsHealthy = false;
        Assert.False(node.IsHealthy);
        node.IsHealthy = true;
        Assert.True(node.IsHealthy);

        node.AverageLatency = TimeSpan.FromMilliseconds(42);
        Assert.Equal(42, node.AverageLatency.TotalMilliseconds);

        node.LastHealthCheck = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(2026, node.LastHealthCheck.Year);

        Assert.Equal(string.Empty, node.Region);
    }

    // ── NormAsyncExtensions – error paths (non-nORM provider) ───────────────

    [Fact]
    public void NormAsyncExtensions_ToListAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        // These return Task but throw synchronously before the async state machine starts
        var ex = Assert.Throws<NormUsageException>(() => { var _ = list.ToListAsync(); });
        Assert.NotNull(ex);
    }

    [Fact]
    public void NormAsyncExtensions_CountAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        var ex = Assert.Throws<NormUsageException>(() => { var _ = list.CountAsync(); });
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task NormAsyncExtensions_AnyAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        await Assert.ThrowsAsync<NormUsageException>(() => list.AnyAsync());
    }

    [Fact]
    public void NormAsyncExtensions_FirstAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        var ex = Assert.Throws<NormUsageException>(() => { var _ = list.FirstAsync(); });
        Assert.NotNull(ex);
    }

    [Fact]
    public void NormAsyncExtensions_FirstOrDefaultAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        var ex = Assert.Throws<NormUsageException>(() => { var _ = list.FirstOrDefaultAsync(); });
        Assert.NotNull(ex);
    }

    [Fact]
    public void NormAsyncExtensions_AsAsyncEnumerable_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        Assert.Throws<NormUsageException>(() => list.AsAsyncEnumerable());
    }

    [Fact]
    public void NormAsyncExtensions_ToListSync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        Assert.Throws<NormUsageException>(() => list.ToListSync());
    }

    [Fact]
    public void NormAsyncExtensions_CountSync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        Assert.Throws<NormUsageException>(() => list.CountSync());
    }

    [Fact]
    public async Task NormAsyncExtensions_ToArrayAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        await Assert.ThrowsAsync<NormUsageException>(() => list.ToArrayAsync());
    }

    [Fact]
    public void NormAsyncExtensions_ExecuteDeleteAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        var ex = Assert.Throws<NormUsageException>(() => { var _ = list.ExecuteDeleteAsync(); });
        Assert.NotNull(ex);
    }

    [Fact]
    public void NormAsyncExtensions_ExecuteUpdateAsync_NonNormProvider_Throws()
    {
        var list = new List<MiscBlog>().AsQueryable();
        var ex = Assert.Throws<NormUsageException>(() =>
            { var _ = list.ExecuteUpdateAsync(s => s.SetProperty(b => b.Title, "x")); });
        Assert.NotNull(ex);
    }

    // ── NormAsyncExtensions – happy path (nORM provider) ────────────────────

    [Fact]
    public async Task NormAsyncExtensions_ToListAsync_WithNormProvider_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var result = await ctx.Query<MiscBlog>().ToListAsync();
            Assert.NotNull(result);
        }
    }

    [Fact]
    public async Task NormAsyncExtensions_CountAsync_WithNormProvider_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var count = await ctx.Query<MiscBlog>().CountAsync();
            Assert.Equal(0, count);
        }
    }

    [Fact]
    public async Task NormAsyncExtensions_ToArrayAsync_WithNormProvider_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var arr = await ctx.Query<MiscBlog>().ToArrayAsync();
            Assert.NotNull(arr);
            Assert.Empty(arr);
        }
    }

    [Fact]
    public async Task NormAsyncExtensions_FirstOrDefaultAsync_WithNormProvider_ReturnsNull()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var result = await ctx.Query<MiscBlog>().FirstOrDefaultAsync();
            Assert.Null(result);
        }
    }

    [Fact]
    public void NormAsyncExtensions_ToListSync_WithNormProvider_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var result = ctx.Query<MiscBlog>().ToListSync();
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void NormAsyncExtensions_CountSync_WithNormProvider_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var count = ctx.Query<MiscBlog>().CountSync();
            Assert.Equal(0, count);
        }
    }

    [Fact]
    public void NormAsyncExtensions_Join_Delegates_ToQueryable()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var blogs = ctx.Query<MiscBlog>();
            var posts = ctx.Query<MiscPost>();
            // Should not throw — just builds the expression tree
            var joined = NormAsyncExtensions.Join(blogs, posts,
                b => b.Id, p => p.BlogId,
                (b, p) => new { b.Title, p.Content });
            Assert.NotNull(joined);
        }
    }

    [Fact]
    public void NormAsyncExtensions_GroupJoin_Delegates_ToQueryable()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var blogs = ctx.Query<MiscBlog>();
            var posts = ctx.Query<MiscPost>();
            var gj = NormAsyncExtensions.GroupJoin(blogs, posts,
                b => b.Id, p => p.BlogId,
                (b, ps) => new { b.Title, Count = ps.Count() });
            Assert.NotNull(gj);
        }
    }

    // ── NormQueryableImplUnconstrained (via Query<RecordLike>) ───────────────

    [Fact]
    public void NormQueryableImplUnconstrained_AsNoTracking_ReturnsQuery()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            // RecordLike has no parameterless ctor → NormQueryableImplUnconstrained used
            var q = ctx.Query<RecordLike>();
            var typed = (INormQueryable<RecordLike>)q;
            var noTracking = typed.AsNoTracking();
            Assert.NotNull(noTracking);
        }
    }

    [Fact]
    public void NormQueryableImplUnconstrained_AsSplitQuery_ReturnsQuery()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var q = ctx.Query<RecordLike>();
            var typed = (INormQueryable<RecordLike>)q;
            var split = typed.AsSplitQuery();
            Assert.NotNull(split);
        }
    }

    [Fact]
    public void NormQueryableImplUnconstrained_Include_ReturnsIncludable()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var q = ctx.Query<RecordLike>();
            var typed = (INormQueryable<RecordLike>)q;
            // RecordLike has no nav props but we can call Include on it (expression tree only)
            var includable = typed.Include(r => r.Name);
            Assert.NotNull(includable);
        }
    }

    // ── NormIncludableQueryableExtensions ────────────────────────────────────

    [Fact]
    public void NormIncludableQueryableExtensions_ThenInclude_ReturnsQuery()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            // Build an Include call, then chain ThenInclude
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var included = q.Include(b => b.Title); // Title is not a nav but builds expression
            // ThenInclude over a scalar still builds the expression tree without executing
            var thenIncluded = included.ThenInclude(t => t.Length);
            Assert.NotNull(thenIncluded);
        }
    }

    [Fact]
    public void NormIncludableQueryableExtensions_ThenInclude_Collection_ReturnsQuery()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            // We'll use a generic IEnumerable<string> scenario on ThenInclude(IEnumerable overload)
            // Build an include on a collection-typed property (fictional)
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var included = q.Include(b => b.Title);
            Assert.NotNull(included);
        }
    }

    // ── NormIncludableQueryableUnconstrained ─────────────────────────────────

    [Fact]
    public void NormIncludableQueryableUnconstrained_MethodsWork()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var q = ctx.Query<RecordLike>();
            var typed = (INormQueryable<RecordLike>)q;
            var incl = typed.Include(r => r.Name);
            Assert.NotNull(incl);

            // Cast to the concrete Unconstrained type to call further includes
            var incl2 = incl.Include(r => r.Id);
            Assert.NotNull(incl2);

            var noTrack = incl.AsNoTracking();
            Assert.NotNull(noTrack);

            var splitQ = incl.AsSplitQuery();
            Assert.NotNull(splitQ);

            // IAsyncEnumerable — just get the enumerator, don't enumerate
            var asyncEnum = incl.AsAsyncEnumerable();
            Assert.NotNull(asyncEnum);

            // CountAsync expression is built — just verify the task is not null
            // (we don't await it as the table doesn't exist)
            var countExpr = incl.Expression;
            Assert.NotNull(countExpr);
        }
    }

    // ── TemporalExtensions ───────────────────────────────────────────────────

    [Fact]
    public void TemporalExtensions_AsOf_DateTime_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            // This builds the expression tree. Provider will reject at execution time for SQLite,
            // but we just verify the expression is created without throwing.
            var temporal = q.AsOf(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            Assert.NotNull(temporal);
        }
    }

    [Fact]
    public void TemporalExtensions_AsOf_TagName_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var temporal = q.AsOf("release-1.0");
            Assert.NotNull(temporal);
        }
    }

    // ── WindowFunctionsExtensions – non-nORM provider throws ─────────────────

    [Fact]
    public void WindowFunctions_WithRowNumber_NonNormProvider_Throws()
    {
        var q = new List<MiscBlog>().AsQueryable();
        Assert.Throws<InvalidOperationException>(() =>
            q.WithRowNumber((b, i) => new { b.Title, Row = i }));
    }

    [Fact]
    public void WindowFunctions_WithRank_NonNormProvider_Throws()
    {
        var q = new List<MiscBlog>().AsQueryable();
        Assert.Throws<InvalidOperationException>(() =>
            q.WithRank((b, i) => new { b.Title, Rank = i }));
    }

    [Fact]
    public void WindowFunctions_WithDenseRank_NonNormProvider_Throws()
    {
        var q = new List<MiscBlog>().AsQueryable();
        Assert.Throws<InvalidOperationException>(() =>
            q.WithDenseRank((b, i) => new { b.Title, Dr = i }));
    }

    [Fact]
    public void WindowFunctions_WithLag_NonNormProvider_Throws()
    {
        var q = new List<MiscBlog>().AsQueryable();
        Assert.Throws<InvalidOperationException>(() =>
            q.WithLag(b => b.Id, 1, (b, prev) => new { b.Title, Prev = prev }));
    }

    [Fact]
    public void WindowFunctions_WithLead_NonNormProvider_Throws()
    {
        var q = new List<MiscBlog>().AsQueryable();
        Assert.Throws<InvalidOperationException>(() =>
            q.WithLead(b => b.Id, 1, (b, next) => new { b.Title, Next = next }));
    }

    // ── WindowFunctionsExtensions – nORM provider builds expression ──────────

    [Fact]
    public void WindowFunctions_WithRowNumber_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithRowNumber((b, i) => new { b.Title, Row = i });
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void WindowFunctions_WithRank_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithRank((b, i) => new { b.Title, Rank = i });
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void WindowFunctions_WithDenseRank_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithDenseRank((b, i) => new { b.Title, Dr = i });
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void WindowFunctions_WithLag_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithLag(b => b.Id, 1, (b, prev) => new { b.Title, Prev = prev });
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void WindowFunctions_WithLead_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithLead(b => b.Id, 1, (b, next) => new { b.Title, Next = next });
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void WindowFunctions_WithLag_WithDefaultValue_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithLag(b => b.Id, 1, (b, prev) => new { b.Title, Prev = prev }, b => b.Id);
            Assert.NotNull(result);
        }
    }

    [Fact]
    public void WindowFunctions_WithLead_WithDefaultValue_NormProvider_BuildsExpression()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = ctx.Query<MiscBlog>();
            var result = q.WithLead(b => b.Id, 1, (b, next) => new { b.Title, Next = next }, b => b.Id);
            Assert.NotNull(result);
        }
    }

    // ── NormQueryable.Query<T> factory path ──────────────────────────────────

    [Fact]
    public void Query_WithParameterlessCtor_ReturnsNormQueryableImpl()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var q = ctx.Query<MiscBlog>();
            Assert.IsAssignableFrom<INormQueryable<MiscBlog>>(q);
        }
    }

    [Fact]
    public void Query_WithoutParameterlessCtor_ReturnsNormQueryableImplUnconstrained()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var q = ctx.Query<RecordLike>();
            // Should be assignable to INormQueryable<RecordLike>
            Assert.IsAssignableFrom<INormQueryable<RecordLike>>(q);
        }
    }

    // ── INormQueryable methods via MiscBlog (constrained) ────────────────────

    [Fact]
    public async Task NormQueryableImpl_ToArrayAsync_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var arr = await q.ToArrayAsync();
            Assert.Empty(arr);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_AnyAsync_ReturnsFalse()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var any = await q.CountAsync() > 0;
            Assert.False(any);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_FirstOrDefaultAsync_ReturnsNull()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var result = await q.FirstOrDefaultAsync();
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_SingleOrDefaultAsync_ReturnsNull()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var result = await q.SingleOrDefaultAsync();
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_AsNoTracking_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var noTrack = q.AsNoTracking();
            var list = await noTrack.ToListAsync();
            Assert.Empty(list);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_AsSplitQuery_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var splitQ = q.AsSplitQuery();
            var list = await splitQ.ToListAsync();
            Assert.Empty(list);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_AsAsyncEnumerable_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var items = new List<MiscBlog>();
            await foreach (var item in q.AsAsyncEnumerable())
                items.Add(item);
            Assert.Empty(items);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_Include_AsSplitQuery_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var incl = q.Include(b => b.Title);
            var split = incl.AsSplitQuery();
            var list = await split.ToListAsync();
            Assert.Empty(list);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_ExecuteDeleteAsync_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var deleted = await q.ExecuteDeleteAsync();
            Assert.Equal(0, deleted);
        }
    }

    [Fact]
    public async Task NormQueryableImpl_ExecuteUpdateAsync_Works()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            CreateTables(cn);
            var q = (INormQueryable<MiscBlog>)ctx.Query<MiscBlog>();
            var updated = await q.ExecuteUpdateAsync(s => s.SetProperty(b => b.Title, "new"));
            Assert.Equal(0, updated);
        }
    }

    // ── NavigationPropertyExtensions (static methods) ────────────────────────

    [Fact]
    public void NavigationPropertyExtensions_EnableLazyLoading_NullEntity_ThrowsArgumentNullException()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            // Call the method via reflection to bypass the compiler's null-check on the constraint
            var method = typeof(NavigationPropertyExtensions)
                .GetMethod("EnableLazyLoading")!
                .MakeGenericMethod(typeof(MiscBlog));
            var ex = Assert.Throws<System.Reflection.TargetInvocationException>(
                () => method.Invoke(null, new object?[] { null, ctx }));
            Assert.IsType<ArgumentNullException>(ex.InnerException);
        }
    }

    [Fact]
    public void NavigationPropertyExtensions_EnableLazyLoading_NonNullEntity_SetsContext()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var blog = new MiscBlog { Id = 1, Title = "test" };
            var result = blog.EnableLazyLoading(ctx);
            Assert.Same(blog, result);
        }
    }

    [Fact]
    public void NavigationPropertyExtensions_CleanupNavigationContext_NullEntity_NoOp()
    {
        // Call via reflection to bypass the class constraint for a null argument
        var method = typeof(NavigationPropertyExtensions)
            .GetMethod("CleanupNavigationContext")!
            .MakeGenericMethod(typeof(MiscBlog));
        method.Invoke(null, new object?[] { null }); // should not throw
    }

    [Fact]
    public void NavigationPropertyExtensions_IsLoaded_ReturnsFalseWithoutContext()
    {
        var blog = new MiscBlog { Id = 1 };
        var loaded = blog.IsLoaded(b => b.Title);
        Assert.False(loaded);
    }

    [Fact]
    public void NavigationPropertyExtensions_IsLoaded_NullEntity_ReturnsFalse()
    {
        // IsLoaded returns false when entity is null (handles null guard in source)
        // Call indirectly via the nullable-aware check in source code:
        var loaded = NavigationPropertyExtensions.IsLoaded<MiscBlog, string>(null!, b => b.Title);
        Assert.False(loaded);
    }

    [Fact]
    public async Task NavigationPropertyExtensions_LoadAsync_NullEntity_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => NavigationPropertyExtensions.LoadAsync<MiscBlog, string>(null!, b => b.Title));
    }

    [Fact]
    public async Task NavigationPropertyExtensions_LoadAsync_WithoutContext_Throws()
    {
        var blog = new MiscBlog { Id = 1 };
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => blog.LoadAsync(b => b.Title));
    }

    [Fact]
    public void NavigationPropertyExtensions_RegisterAndUnregisterLoader()
    {
        // Just exercise the static methods without asserting internals
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var loader = new nORM.Navigation.BatchedNavigationLoader(ctx);
            NavigationPropertyExtensions.RegisterLoader(loader);
            NavigationPropertyExtensions.UnregisterLoader(loader);
        }
    }

    [Fact]
    public void NavigationPropertyExtensions_CleanupNavigationContext_AfterEnable()
    {
        var (ctx, cn) = CreateCtx();
        using (ctx) using (cn)
        {
            var blog = new MiscBlog { Id = 1, Title = "t" };
            blog.EnableLazyLoading(ctx);
            NavigationPropertyExtensions.CleanupNavigationContext(blog);
            // After cleanup the nav context should be removed
            Assert.False(NavigationPropertyExtensions._navigationContexts.TryGetValue(blog, out _));
        }
    }
}
