using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
#nullable enable
namespace nORM.Query
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("NormQueryProvider builds and executes Expression trees via reflection; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("NormQueryProvider reflects over entity and query types; trimming may remove the required members.")]
    internal sealed partial class NormQueryProvider : IQueryProvider, IDisposable
    {
        /// <summary>Default initial capacity for list materialization when no Take hint is available.</summary>
        private const int DefaultListCapacity = 16;
        /// <summary>Maximum string parameter length that gets an explicit Size hint (avoids NVARCHAR(MAX) on SQL Server).</summary>
        // C-4/A-2 fix: reference the single authoritative constant from ParameterOptimizer
        // instead of duplicating the magic value 4000 here.
        private const int MaxInlineStringSize = ParameterOptimizer.MaxInlineStringSize;
        /// <summary>Threshold below which UTF-8 encoding uses stackalloc instead of ArrayPool rental.</summary>
        private const int StackAllocUtf8Threshold = 256;
        /// <summary>Fallback average plan size in bytes when no samples have been collected yet.</summary>
        private const int FallbackAvgPlanSizeBytes = 16 * 1024;
        /// <summary>Maximum bytes the plan cache is allowed to consume.</summary>
        private const long MaxPlanCacheBytes = 64L * 1024 * 1024;
        /// <summary>Percentage of total available memory to allocate for the plan cache (1%).</summary>
        private const int MemoryBudgetDivisor = 100;
        /// <summary>Minimum number of entries the plan cache will hold regardless of memory pressure.</summary>
        private const int MinPlanCacheEntries = 100;
        /// <summary>Maximum number of entries the plan cache will hold regardless of available memory.</summary>
        private const int MaxPlanCacheEntries = 10_000;
        /// <summary>Interval at which the plan cache monitor adjusts cache size based on memory pressure.</summary>
        private static readonly TimeSpan PlanCacheMonitorInterval = TimeSpan.FromMinutes(1);
        /// <summary>How long a plan stays in the LRU cache before expiry.</summary>
        private static readonly TimeSpan PlanCacheTimeToLive = TimeSpan.FromHours(1);
        /// <summary>Interval at which unused cache lock semaphores are cleaned up.</summary>
        private static readonly TimeSpan CacheLockCleanupInterval = TimeSpan.FromHours(1);

        internal readonly DbContext _ctx;
        private static readonly ConcurrentLruCache<ExpressionFingerprint, QueryPlan> _planCache =
            new(maxSize: CalculateInitialPlanCacheSize(), timeToLive: PlanCacheTimeToLive);
        private static readonly Timer _planCacheMonitor = new(AdjustPlanCacheSize, null, PlanCacheMonitorInterval, PlanCacheMonitorInterval);
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _cacheLocks = new();
        private static readonly Timer _cacheLockCleanupTimer = new(CleanupCacheLocks, null, CacheLockCleanupInterval, CacheLockCleanupInterval);
        // Cache GetElementType results to avoid repeated reflection.
        private static readonly ConcurrentDictionary<Type, Type> _elementTypeCache = new();
        // Singleton MaterializerFactory - only wraps static caches, no instance state.
        private static readonly MaterializerFactory _sharedMaterializerFactory = new();
        // Cache constructor existence checks to avoid repeated reflection.
        private static readonly ConcurrentDictionary<Type, bool> _constrainedQueryableCache = new();
        // Cache compiled queryable factory delegates to avoid Activator.CreateInstance on every LINQ chain step
        private static readonly ConcurrentDictionary<Type, Func<IQueryProvider, Expression, IQueryable>> _queryableFactoryCache = new();
        private static long _totalPlanSize;
        private static int _planSizeSamples;
        // Track live provider count so timers can be stopped when all providers are disposed.
        private static int _activeProviderCount;
        private readonly QueryExecutor _executor;
        private readonly IncludeProcessor _includeProcessor;
        private readonly BulkCudBuilder _cudBuilder;
        private readonly ConcurrentDictionary<string, string> _simpleSqlCache = new();
        // Caches the fully-built QueryPlan for the simple path per (SQL, result type). Everything in the plan
        // is stable per shape — the materializer delegates, element type, table set — and MaterializeAsync
        // never reads plan.Parameters (the per-call parameter values flow only through the DbCommand), so one
        // plan instance is reused across calls, eliminating the per-call record allocation, GetMapping, and
        // two materializer-factory lookups. Instance-scoped like _simpleSqlCache: no cross-context coupling.
        private readonly ConcurrentDictionary<(string Sql, Type ResultType), QueryPlan> _simplePlanCache = new();
        private readonly ConcurrentDictionary<ExpressionFingerprint, PooledPlanCommand> _pooledPlanCommands = new();
        /// <summary>PERF: Dedicated count SQL cache with structured keys to avoid per-call string composition and predicate-shape collisions.</summary>
        private readonly ConcurrentDictionary<CountSqlCacheKey, (string Sql, bool NeedsParam)> _countSqlCache = new();
        /// <summary>PERF: Pooled prepared commands for parameterless count queries (keyed by SQL), each paired with a lock object to serialize concurrent access.</summary>
        private readonly ConcurrentDictionary<string, (DbCommand Cmd, object Lock)> _pooledCountCommands = new();

        private enum CountPredicateShape : byte
        {
            None,
            BoolTrue,
            BoolFalse,
            EqualityValue,
            EqualityNull,
            Complex
        }

        private readonly record struct CountSqlCacheKey(Type ElementType, string PredicateKey, CountPredicateShape Shape);

        private sealed class PooledPlanCommand
        {
            public PooledPlanCommand(DbCommand command, int fixedParameterCount)
            {
                Command = command;
                FixedParameterCount = fixedParameterCount;
            }

            public DbCommand Command { get; }
            public int FixedParameterCount { get; }
            public object Lock { get; } = new();
        }

        public NormQueryProvider(DbContext ctx)
        {
            _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
            _includeProcessor = new IncludeProcessor(ctx);
            _executor = new QueryExecutor(ctx, _includeProcessor);
            _cudBuilder = new BulkCudBuilder(ctx);
            // C1: restart timers if they were stopped when a new provider is created.
            if (Interlocked.Increment(ref _activeProviderCount) == 1)
            {
                _planCacheMonitor.Change(PlanCacheMonitorInterval, PlanCacheMonitorInterval);
                _cacheLockCleanupTimer.Change(CacheLockCleanupInterval, CacheLockCleanupInterval);
            }
        }
        public void Dispose()
        {
            foreach (var entry in _pooledCountCommands.Values)
                try { entry.Cmd.Dispose(); } catch (ObjectDisposedException) { /* already disposed - safe to ignore */ }
            _pooledCountCommands.Clear();
            foreach (var entry in _pooledPlanCommands.Values)
                try { entry.Command.Dispose(); } catch (ObjectDisposedException) { /* already disposed - safe to ignore */ }
            _pooledPlanCommands.Clear();

            // C1: stop background timers when the last provider is disposed so the process
            // can undergo deterministic teardown (e.g. in test runs / hosting teardown).
            if (Interlocked.Decrement(ref _activeProviderCount) <= 0)
            {
                Volatile.Write(ref _activeProviderCount, 0);
                _planCacheMonitor.Change(Timeout.Infinite, Timeout.Infinite);
                _cacheLockCleanupTimer.Change(Timeout.Infinite, Timeout.Infinite);
            }
        }
        public IQueryable CreateQuery(Expression expression)
        {
            var typeArgs = expression.Type.GetGenericArguments();
            if (typeArgs.Length == 0)
                throw new ArgumentException($"Expression type '{expression.Type}' has no generic arguments. Expected IQueryable<T>.", nameof(expression));
            var elementType = typeArgs[0];
            return CreateQueryInternal(elementType, expression);
        }
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            var query = CreateQueryInternal(typeof(TElement), expression);
            if (query is IQueryable<TElement> typedQuery)
            {
                return typedQuery;
            }
            throw new NormQueryException($"Unable to create IQueryable for type '{typeof(TElement)}'.");
        }
        private IQueryable CreateQueryInternal(Type elementType, Expression expression)
        {
            // Use cached compiled factory instead of Activator.CreateInstance on every LINQ chain step.
            // Each .Where/.Take/.OrderBy calls CreateQuery, so this is on a very hot path.
            var factory = _queryableFactoryCache.GetOrAdd(elementType, static t =>
            {
                bool constrained = _constrainedQueryableCache.GetOrAdd(t, static t2 =>
                {
                    if (!t2.IsClass) return false;
                    if (t2.Name.StartsWith("<>", StringComparison.Ordinal)) return false;
                    return t2.GetConstructor(Type.EmptyTypes) != null;
                });

                var queryableType = constrained
                    ? typeof(NormQueryableImpl<>).MakeGenericType(t)
                    : typeof(NormQueryableImplUnconstrained<>).MakeGenericType(t);

                var ctor = queryableType.GetConstructor(new[] { typeof(IQueryProvider), typeof(Expression) })!;
                var providerParam = System.Linq.Expressions.Expression.Parameter(typeof(IQueryProvider), "p");
                var exprParam = System.Linq.Expressions.Expression.Parameter(typeof(Expression), "e");
                var newExpr = System.Linq.Expressions.Expression.New(ctor, providerParam, exprParam);
                var cast = System.Linq.Expressions.Expression.Convert(newExpr, typeof(IQueryable));
                return System.Linq.Expressions.Expression.Lambda<Func<IQueryProvider, Expression, IQueryable>>(cast, providerParam, exprParam).Compile();
            });

            return factory(this, expression);
        }
        /// <summary>
        /// PERFORMANCE OPTIMIZATION: Enhanced cache lock cleanup.
        /// - Limits cleanup to prevent unbounded growth
        /// - Removes unused semaphores to allow GC collection
        /// - Processes locks in batches to reduce iteration overhead
        /// </summary>
        private static void CleanupCacheLocks(object? state)
        {
            const int MaxLocksToKeep = 1000;
            const int CleanupBatchSize = 100;

            if (_cacheLocks.Count <= MaxLocksToKeep)
                return;

            var locksToRemove = new List<(string Key, SemaphoreSlim Semaphore)>(CleanupBatchSize);

            foreach (var kvp in _cacheLocks)
            {
                // Only remove locks that are not currently in use
                if (kvp.Value.CurrentCount == 1)
                {
                    locksToRemove.Add((kvp.Key, kvp.Value));

                    // Process in batches to avoid holding iterator too long
                    if (locksToRemove.Count >= CleanupBatchSize)
                        break;
                }
            }

            // Remove in separate pass to avoid concurrent modification.
            // Use value-matching TryRemove so we don't accidentally remove a
            // concurrently re-inserted semaphore for the same key. Do NOT dispose -
            // threads holding a reference obtained via GetOrAdd before this removal can
            // still call Wait/WaitAsync safely on the semaphore. GC will collect it once
            // all references drop. SemaphoreSlim only allocates its underlying event lazily
            // (on contended WaitAsync); we checked CurrentCount == 1 (no waiters) so the
            // event is not allocated and no unmanaged resources are held.
            foreach (var (key, semaphore) in locksToRemove)
            {
                _cacheLocks.TryRemove(new KeyValuePair<string, SemaphoreSlim>(key, semaphore));
            }
        }
        private static int CalculateInitialPlanCacheSize()
            => CalculatePlanCacheSize(GC.GetGCMemoryInfo());
        private static void AdjustPlanCacheSize(object? state)
        {
            var info = GC.GetGCMemoryInfo();
            if (info.MemoryLoadBytes >= info.HighMemoryLoadThresholdBytes)
            {
                _planCache.Clear();
            }
            _planCache.SetMaxSize(CalculatePlanCacheSize(info));
        }
        private static int CalculatePlanCacheSize(GCMemoryInfo info)
        {
            var samples = Volatile.Read(ref _planSizeSamples);
            var avgPlanSize = samples == 0
                ? FallbackAvgPlanSizeBytes
                : (int)(Volatile.Read(ref _totalPlanSize) / samples);
            var cacheBytes = Math.Min(info.TotalAvailableMemoryBytes / MemoryBudgetDivisor, MaxPlanCacheBytes);
            var size = (int)(cacheBytes / avgPlanSize);
            return Math.Clamp(size, MinPlanCacheEntries, MaxPlanCacheEntries);
        }
        public TResult Execute<TResult>(Expression expression)
            => ExecuteSync<TResult>(expression);

        /// <summary>
        /// Synchronous query execution that uses a true synchronous code path rather than blocking on
        /// async methods. Used by IEnumerable.GetEnumerator() to avoid deadlocks from GetAwaiter().GetResult()
        /// and thread starvation on bounded thread pools.
        /// </summary>
        public TResult ExecuteSync<TResult>(Expression expression)
        {
            // Queryable.Aggregate with a sum-fold accumulator - rewrite to
            // SUM at translation time so we don't materialise the full result
            // set client-side. Supported shapes: 1-arg `(acc, x) => acc + x`
            // and 2-arg `seed, (acc, x) => acc + sub(x)`. Other Aggregate
            // shapes (resultSelector, non-additive fold) remain untranslated
            // and fall through to the unsupported-method error below.
            if (TryRewriteAggregateToSum<TResult>(expression, out var aggResult))
                return aggResult;

            if (TryGetCountQuery(expression, out var countSql, out var countParameters))
            {
                return ExecuteCountSync<TResult>(countSql, countParameters);
            }

            if (TryGetSimpleQuery(expression, out var sql, out var parameters, out var simpleMethodName))
            {
                return ExecuteSimpleSync<TResult>(sql, parameters, simpleMethodName);
            }
            // THREAD STARVATION FIX: Use true synchronous execution path
            return ExecuteInternalSync<TResult>(expression);
        }
    }
}
