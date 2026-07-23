using System;
using System.Data;
using Microsoft.Data.Sqlite;
using nORM.Internal;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Behavior tests for two nORM.Internal utility types with DIFFERENT public status after the 1.0 freeze
/// audit: <see cref="ConcurrentLruCache{TKey,TValue}"/> is a genuine implementation detail — internalized so
/// it does not lock into the stable contract (the test project reaches it via InternalsVisibleTo);
/// ParameterOptimizer stays PUBLIC by design because the source generator emits calls to it in consumer
/// assemblies (see SG1_ParameterOptimizer_Type_Is_Public).
/// </summary>

// ══════════════════════════════════════════════════════════════════════════════
// ConcurrentLruCache<K,V> — internal behavior coverage
// ══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
[Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
public class ConcurrentLruCacheBehaviorTests
{
    // ── GetOrAdd ──────────────────────────────────────────────────────────────

    [Fact]
    public void GetOrAdd_MissingKey_InvokesFactory()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        var factoryCalled = 0;
        var result = cache.GetOrAdd(1, k => { factoryCalled++; return $"v{k}"; });
        Assert.Equal("v1", result);
        Assert.Equal(1, factoryCalled);
    }

    [Fact]
    public void GetOrAdd_ExistingKey_DoesNotInvokeFactory()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.GetOrAdd(1, _ => "first");
        var factoryCalled = 0;
        var result = cache.GetOrAdd(1, k => { factoryCalled++; return "second"; });
        Assert.Equal("first", result);
        Assert.Equal(0, factoryCalled);
    }

    [Fact]
    public void GetOrAdd_NullFactory_ThrowsArgumentNullException()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        Assert.Throws<ArgumentNullException>(() => cache.GetOrAdd(1, null!));
    }

    // ── TryGet ────────────────────────────────────────────────────────────────

    [Fact]
    public void TryGet_ExistingKey_ReturnsTrueAndValue()
    {
        using var cache = new ConcurrentLruCache<string, int>(maxSize: 10);
        cache.Set("key", 42);
        Assert.True(cache.TryGet("key", out var value));
        Assert.Equal(42, value);
    }

    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
    {
        using var cache = new ConcurrentLruCache<string, int>(maxSize: 10);
        Assert.False(cache.TryGet("missing", out var value));
        Assert.Equal(0, value);
    }

    // ── Clear ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Clear_RemovesAllEntries()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Set(1, "a");
        cache.Set(2, "b");
        cache.Set(3, "c");
        Assert.Equal(3, cache.Count);

        cache.Clear();

        Assert.Equal(0, cache.Count);
        Assert.False(cache.TryGet(1, out _));
        Assert.False(cache.TryGet(2, out _));
        Assert.False(cache.TryGet(3, out _));
    }

    [Fact]
    public void Clear_ResetsHitMissEvictionCounters()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 2);
        cache.GetOrAdd(1, _ => "a");
        cache.GetOrAdd(1, _ => "a"); // hit
        cache.GetOrAdd(2, _ => "b");
        cache.GetOrAdd(3, _ => "c"); // forces eviction

        cache.Clear();

        Assert.Equal(0, cache.Hits);
        Assert.Equal(0, cache.Misses);
        Assert.Equal(0, cache.Evictions);
    }

    // ── Count ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Count_ReflectsNumberOfStoredEntries()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 100);
        Assert.Equal(0, cache.Count);
        cache.Set(1, "a");
        Assert.Equal(1, cache.Count);
        cache.Set(2, "b");
        Assert.Equal(2, cache.Count);
    }

    // ── Hits / Misses / HitRate ───────────────────────────────────────────────

    [Fact]
    public void Hits_IncrementOnSuccessfulTryGet()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Set(1, "one");
        cache.TryGet(1, out _);
        cache.TryGet(1, out _);
        Assert.Equal(2, cache.Hits);
    }

    [Fact]
    public void Misses_IncrementOnUnsuccessfulTryGet()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.TryGet(99, out _);
        cache.TryGet(100, out _);
        Assert.Equal(2, cache.Misses);
    }

    [Fact]
    public void HitRate_IsRatioOfHitsToTotalAccesses()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Set(1, "a");
        cache.TryGet(1, out _);  // hit
        cache.TryGet(99, out _); // miss
        // 1 hit out of 2 accesses = 0.5
        Assert.Equal(0.5, cache.HitRate, precision: 10);
    }

    [Fact]
    public void HitRate_IsZero_WhenNoAccesses()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        Assert.Equal(0.0, cache.HitRate);
    }

    // ── Evictions ─────────────────────────────────────────────────────────────

    [Fact]
    public void Evictions_IncrementWhenCacheExceedsCapacity()
    {
        using var cache = new ConcurrentLruCache<int, string>(maxSize: 2);
        cache.Set(1, "a");
        cache.Set(2, "b");
        Assert.Equal(0, cache.Evictions);

        cache.Set(3, "c"); // must evict one entry
        Assert.Equal(1, cache.Evictions);
        Assert.Equal(2, cache.Count);
    }

    [Fact]
    public void EvictionBehavior_AtCapacity_CountNeverExceedsMaxSize()
    {
        const int max = 5;
        using var cache = new ConcurrentLruCache<int, string>(maxSize: max);
        for (var i = 0; i < 20; i++)
            cache.Set(i, $"v{i}");

        Assert.Equal(max, cache.Count);
        Assert.True(cache.Evictions >= 15); // at least 15 evictions for 20 inserts into 5-slot cache
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    [Fact]
    public void Dispose_ClearsCount()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Set(1, "a");
        cache.Set(2, "b");
        cache.Dispose();
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void Dispose_TryGet_ReturnsFalseWithoutException()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Set(1, "a");
        cache.Dispose();
        // Should return false, not throw
        Assert.False(cache.TryGet(1, out var result));
        Assert.Null(result);
    }

    [Fact]
    public void Dispose_GetOrAdd_ThrowsObjectDisposedException()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Dispose();
        Assert.Throws<ObjectDisposedException>(() => cache.GetOrAdd(1, _ => "x"));
    }

    [Fact]
    public void Dispose_Clear_IsNoOp()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Set(1, "a");
        cache.Dispose();
        // Should not throw
        var ex = Record.Exception(() => cache.Clear());
        Assert.Null(ex);
    }

    [Fact]
    public void Dispose_DoubleDispose_DoesNotThrow()
    {
        var cache = new ConcurrentLruCache<int, string>(maxSize: 10);
        cache.Dispose();
        var ex = Record.Exception(() => cache.Dispose());
        Assert.Null(ex);
    }

    // ── Constructor guard ─────────────────────────────────────────────────────

    [Fact]
    public void Constructor_ZeroMaxSize_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ConcurrentLruCache<int, string>(0));
    }

    [Fact]
    public void Constructor_NegativeMaxSize_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ConcurrentLruCache<int, string>(-1));
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// ParameterOptimizer — internal behavior coverage
// ══════════════════════════════════════════════════════════════════════════════

[Xunit.Trait("Category", "Fast")]
public class ParameterOptimizerBehaviorTests
{
    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    // ── AddParam ──────────────────────────────────────────────────────────────

    [Fact]
    public void AddParam_AddsParameterWithCorrectNameAndValue()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        cmd.AddParam("@id", 99);

        Assert.Single(cmd.Parameters);
        Assert.Equal("@id", cmd.Parameters[0].ParameterName);
        Assert.Equal(99, cmd.Parameters[0].Value);
    }

    [Fact]
    public void AddParam_NullValue_BindsAsDBNull()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        cmd.AddParam("@name", null);

        Assert.Single(cmd.Parameters);
        Assert.Equal(DBNull.Value, cmd.Parameters[0].Value);
    }

    [Fact]
    public void AddParam_StringValue_SetsDbTypeString()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        cmd.AddParam("@s", "hello");

        Assert.Single(cmd.Parameters);
        Assert.Equal("hello", cmd.Parameters[0].Value);
    }

    // ── AddOptimizedParam ─────────────────────────────────────────────────────

    [Fact]
    public void AddOptimizedParam_Int32_SetsInt32DbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", 42);

        Assert.Single(cmd.Parameters);
        Assert.Equal(42, cmd.Parameters[0].Value);
        Assert.Equal(DbType.Int32, cmd.Parameters[0].DbType);
    }

    [Fact]
    public void AddOptimizedParam_Bool_SetsBooleanDbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@flag", true);

        Assert.Single(cmd.Parameters);
        Assert.Equal(DbType.Boolean, cmd.Parameters[0].DbType);
        Assert.Equal(true, cmd.Parameters[0].Value);
    }

    [Fact]
    public void AddOptimizedParam_NullWithKnownType_BindsDBNullWithCorrectDbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", null, typeof(int));

        Assert.Single(cmd.Parameters);
        Assert.Equal(DBNull.Value, cmd.Parameters[0].Value);
        Assert.Equal(DbType.Int32, cmd.Parameters[0].DbType);
    }

    [Fact]
    public void AddOptimizedParam_NullName_ThrowsArgumentNullException()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        Assert.Throws<ArgumentNullException>(() =>
            ParameterOptimizer.AddOptimizedParam(cmd, null!, 42));
    }

    [Fact]
    public void AddOptimizedParam_Guid_BindsCanonicalTextForSqlite()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        var id = Guid.NewGuid();

        ParameterOptimizer.AddOptimizedParam(cmd, "@id", id);

        Assert.Single(cmd.Parameters);
        Assert.Equal(DbType.String, cmd.Parameters[0].DbType);
        Assert.Equal(id.ToString("D"), cmd.Parameters[0].Value);
    }

    [Fact]
    public void AddOptimizedParam_LongValue_SetsInt64DbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@ts", 123456789L);

        Assert.Single(cmd.Parameters);
        Assert.Equal(DbType.Int64, cmd.Parameters[0].DbType);
        Assert.Equal(123456789L, cmd.Parameters[0].Value);
    }

    [Fact]
    public void AddOptimizedParam_MultipleParams_AllAdded()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", 1);
        ParameterOptimizer.AddOptimizedParam(cmd, "@p1", "hello");
        ParameterOptimizer.AddOptimizedParam(cmd, "@p2", true);

        Assert.Equal(3, cmd.Parameters.Count);
        Assert.Equal("@p0", cmd.Parameters[0].ParameterName);
        Assert.Equal("@p1", cmd.Parameters[1].ParameterName);
        Assert.Equal("@p2", cmd.Parameters[2].ParameterName);
    }
}
