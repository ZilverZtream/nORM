using System;
using System.Buffers;
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
    internal sealed class NormQueryProvider : IQueryProvider, IDisposable
    {
        /// <summary>Default initial capacity for list materialization when no Take hint is available.</summary>
        private const int DefaultListCapacity = 16;
        /// <summary>Maximum string parameter length that gets an explicit Size hint (avoids NVARCHAR(MAX) on SQL Server).</summary>
        private const int MaxInlineStringSize = 4000;
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
        // Singleton MaterializerFactory — only wraps static caches, no instance state.
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
        /// <summary>PERF: Dedicated count SQL cache with ValueTuple keys to avoid string.Concat allocation per count call.</summary>
        private readonly ConcurrentDictionary<(Type ElementType, string PredicateKey), (string Sql, bool NeedsParam)> _countSqlCache = new();
        /// <summary>PERF: Pooled prepared commands for parameterless count queries (keyed by SQL), each paired with a lock object to serialize concurrent access.</summary>
        private readonly ConcurrentDictionary<string, (DbCommand Cmd, object Lock)> _pooledCountCommands = new();
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
                try { entry.Cmd.Dispose(); } catch (ObjectDisposedException) { /* already disposed — safe to ignore */ }
            _pooledCountCommands.Clear();

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
            throw new InvalidOperationException($"Unable to create IQueryable for type '{typeof(TElement)}'.");
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
                    if (t2.Name.StartsWith("<>")) return false;
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
            // concurrently re-inserted semaphore for the same key. Do NOT dispose —
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
        public object? Execute(Expression expression) => Execute<object>(expression);
        public Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
        {
            if (TryGetCountQuery(expression, out var countSql, out var countParameters))
            {
                if (_ctx.Options.RetryPolicy != null)
                    return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCountAsync<TResult>(countSql, countParameters, token), ct);
                return ExecuteCountAsync<TResult>(countSql, countParameters, ct);
            }

            // Fast path – bypass translator for recognized simple patterns
            if (TryExecuteFastPath<TResult>(expression, ct, out var fastResult))
                return fastResult;
            // Simple query path (slightly higher overhead than fast path but handles more patterns)
            if (TryGetSimpleQuery(expression, out var sql, out var parameters, out var simpleMethodName))
            {
                if (_ctx.Options.RetryPolicy != null)
                    return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteSimpleAsync<TResult>(sql, parameters, simpleMethodName, token), ct);
                return ExecuteSimpleAsync<TResult>(sql, parameters, simpleMethodName, ct);
            }
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteInternalAsync<TResult>(expression, token), ct);
            return ExecuteInternalAsync<TResult>(expression, ct);
        }
        /// <summary>
        /// Executes a translated <c>DELETE</c> query asynchronously.
        /// </summary>
        /// <param name="expression">Expression representing the delete query.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>A task containing the number of rows affected.</returns>
        public Task<int> ExecuteDeleteAsync(Expression expression, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteDeleteInternalAsync(expression, token), ct);
            return ExecuteDeleteInternalAsync(expression, ct);
        }
        public Task<int> ExecuteUpdateAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteUpdateInternalAsync(expression, set, token), ct);
            return ExecuteUpdateInternalAsync(expression, set, ct);
        }
        /// <summary>
        /// Fast path execution using cached delegates instead of reflection.
        /// Eliminates MakeGenericMethod and Invoke overhead.
        /// </summary>
        private bool TryExecuteFastPath<TResult>(Expression expression, CancellationToken ct, out Task<TResult> result)
        {
            result = default!;
            var resultType = typeof(TResult);
            Type? elementType = null;

            if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(List<>))
            {
                elementType = resultType.GetGenericArguments()[0];
            }
            else if (resultType == typeof(int) || resultType == typeof(long))
            {
                if (expression is MethodCallExpression mc && mc.Arguments.Count > 0)
                {
                    var sourceType = mc.Arguments[0].Type;
                    if (sourceType.IsGenericType)
                        elementType = sourceType.GetGenericArguments()[0];
                }
            }

            if (elementType != null)
            {
                try
                {
                    // Use cached delegate instead of MakeGenericMethod + Invoke.
                    if (FastPathQueryExecutor.TryExecuteNonGeneric(elementType, expression, _ctx, ct, out var taskObject))
                    {
                        // Cast Task<object> to Task<TResult> without additional overhead
                        result = CastTaskResult<TResult>(taskObject);
                        return true;
                    }
                }
                catch (NotSupportedException)
                {
                    // ignore and fall back to full translation path
                }
            }
            return false;
        }

        /// <summary>
        /// Efficiently converts Task&lt;object&gt; to Task&lt;TResult&gt;.
        /// Avoids async state machine allocation when the task is already completed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Task<TResult> CastTaskResult<TResult>(Task<object> task)
        {
            // If the task is already completed (common for cached/fast queries),
            // skip the async state machine entirely.
            if (task.IsCompletedSuccessfully)
            {
                return Task.FromResult(ConvertScalarResult<TResult>(task.Result));
            }
            return CastTaskResultAsync<TResult>(task);
        }

        private static async Task<TResult> CastTaskResultAsync<TResult>(Task<object> task)
        {
            var result = await task.ConfigureAwait(false);
            return ConvertScalarResult<TResult>(result);
        }

        /// <summary>
        /// Converts scalar results without boxing for value types.
        /// Avoids Convert.ChangeType which forces heap allocation for value types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static TResult ConvertScalarResult<TResult>(object result)
        {
            // Guard: null/DBNull should have been handled by callers (ExecuteScalarPlan*),
            // but defend here to avoid NullReferenceException in the Convert paths below.
            if (result is null || result is DBNull)
                return default!;

            // Direct cast for reference types.
            if (typeof(TResult).IsClass || typeof(TResult).IsInterface)
            {
                return (TResult)result;
            }

            // Handle value types without Convert.ChangeType boxing.
            var resultType = typeof(TResult);
            var underlyingType = Nullable.GetUnderlyingType(resultType) ?? resultType;

            // Fast path for common scalar types - avoids boxing
            if (underlyingType == typeof(int))
                return (TResult)(object)Convert.ToInt32(result);
            if (underlyingType == typeof(long))
                return (TResult)(object)Convert.ToInt64(result);
            if (underlyingType == typeof(double))
                return (TResult)(object)Convert.ToDouble(result);
            if (underlyingType == typeof(decimal))
                return (TResult)(object)Convert.ToDecimal(result);
            if (underlyingType == typeof(bool))
                return (TResult)(object)Convert.ToBoolean(result);
            if (underlyingType == typeof(short))
                return (TResult)(object)Convert.ToInt16(result);
            if (underlyingType == typeof(byte))
                return (TResult)(object)Convert.ToByte(result);
            if (underlyingType == typeof(float))
                return (TResult)(object)Convert.ToSingle(result);
            if (underlyingType == typeof(DateTime))
                return (TResult)(object)Convert.ToDateTime(result);
            if (underlyingType == typeof(Guid))
                return (TResult)(object)(Guid)result;

            // Fallback for other types (still better than ChangeType for common cases above)
            return (TResult)Convert.ChangeType(result, underlyingType)!;
        }
        private Task<TResult> ExecuteInternalAsync<TResult>(Expression expression, CancellationToken ct)
        {
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            // For cached queries, use the closure-based path (rare).
            // For non-cached queries (common), return task directly — no async state machine needed.
            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                return ExecuteInternalCachedAsync<TResult>(plan, paramValues, sw, ct);
            }
            return ExecuteQueryFromPlanAsync<TResult>(plan, paramValues, sw, ct);
        }

        private async Task<TResult> ExecuteInternalCachedAsync<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            var parameterDictionary = EnsureParameterDictionary(plan, paramValues);
            Func<Task<TResult>> queryExecutorFactory = () => ExecuteQueryFromPlanAsync<TResult>(plan, paramValues, sw, ct);
            var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, parameterDictionary);
            var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
            return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
        }
        /// <summary>
        /// Non-async entry point — does synchronous command setup when connection is ready,
        /// then dispatches to the appropriate async materializer. Avoids one async state machine
        /// allocation on the hot path.
        /// </summary>
        private Task<TResult> ExecuteQueryFromPlanAsync<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            // Check if connection is ready without awaiting
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteQueryFromPlanSlowAsync<TResult>(ensureTask, plan, paramValues, sw, ct);

            // Synchronous command setup — no async state machine needed
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);

            // Dispatch directly to materializer — avoids wrapping in another async method
            if (plan.IsScalar)
            {
                // Sync scalar for providers without true async I/O (SQLite)
                if (_ctx.Provider.PrefersSyncExecution)
                    return ExecuteScalarPlanSync<TResult>(plan, cmd, sw);
                return ExecuteScalarPlanAsync<TResult>(plan, cmd, sw, ct);
            }

            // For providers that don't support true async I/O (SQLite), use fully synchronous
            // materialization to eliminate per-row ReadAsync state machine overhead (~50ns × N rows).
            if (_ctx.Provider.PrefersSyncExecution)
                return ExecuteListPlanSyncWrapped<TResult>(plan, cmd, sw);

            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (Task<TResult>)(object)ExecuteObjectListPlanAsync(plan, cmd, sw, ct);

            return ExecuteListPlanAsync<TResult>(plan, cmd, sw, ct);
        }

        /// <summary>PERF: Slow path — connection needs initialization.</summary>
        private async Task<TResult> ExecuteQueryFromPlanSlowAsync<TResult>(Task<DbConnection> ensureTask, QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);

            if (plan.IsScalar)
                return await ExecuteScalarPlanAsync<TResult>(plan, cmd, sw, ct).ConfigureAwait(false);

            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (TResult)(object)await ExecuteObjectListPlanAsync(plan, cmd, sw, ct).ConfigureAwait(false);

            return await ExecuteListPlanAsync<TResult>(plan, cmd, sw, ct).ConfigureAwait(false);
        }

        /// <summary>PERF: Extracted parameter binding to share between fast and slow paths.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BindPlanParameters(DbCommand cmd, QueryPlan plan, IReadOnlyList<object?>? paramValues)
        {
            var compiledParams = plan.CompiledParameters;
            if (compiledParams.Count == 0)
            {
                foreach (var p in plan.Parameters)
                    cmd.AddOptimizedParam(p.Key, p.Value);
            }
            else
            {
                if (!_compiledParamSets.TryGet(plan, out var compiledSet))
                {
                    compiledSet = new HashSet<string>(compiledParams, StringComparer.Ordinal);
                    _compiledParamSets.Set(plan, compiledSet);
                }
                foreach (var p in plan.Parameters)
                {
                    if (!compiledSet.Contains(p.Key))
                        cmd.AddOptimizedParam(p.Key, p.Value);
                }
                if (paramValues != null)
                {
                    var count = Math.Min(compiledParams.Count, paramValues.Count);
                    for (int i = 0; i < count; i++)
                        cmd.AddOptimizedParam(compiledParams[i], paramValues[i] ?? DBNull.Value);
                }
            }
        }

        /// <summary>PERF: Scalar materialization path.</summary>
        private async Task<TResult> ExecuteScalarPlanAsync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw, CancellationToken ct)
        {
            await using var command = cmd;
            var scalarResult = await command.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
            if (scalarResult == null || scalarResult is DBNull)
            {
                if (plan.MethodName is "Min" or "Max" or "Average" &&
                    typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                    throw new InvalidOperationException("Sequence contains no elements");
                return default(TResult)!;
            }
            return ConvertScalarResult<TResult>(scalarResult)!;
        }

        /// <summary>PERF: Fully synchronous scalar path for providers without true async I/O.</summary>
        private Task<TResult> ExecuteScalarPlanSync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw)
        {
            using var command = cmd;
            var scalarResult = command.ExecuteScalarWithInterception(_ctx);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
            if (scalarResult == null || scalarResult is DBNull)
            {
                if (plan.MethodName is "Min" or "Max" or "Average" &&
                    typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                    throw new InvalidOperationException("Sequence contains no elements");
                return Task.FromResult(default(TResult)!);
            }
            return Task.FromResult(ConvertScalarResult<TResult>(scalarResult)!);
        }

        /// <summary>PERF: List&lt;object&gt; materialization path — avoids covariant copy.</summary>
        private async Task<List<object>> ExecuteObjectListPlanAsync(QueryPlan plan, DbCommand cmd, Stopwatch? sw, CancellationToken ct)
        {
            var objectList = await _executor.MaterializeAsObjectListAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, objectList.Count);
            return objectList;
        }

        /// <summary>
        /// Fully synchronous materialization wrapped in Task.FromResult.
        /// Eliminates async state machine overhead for providers without true async I/O (SQLite).
        /// Saves ~50-100ns per Read() call → ~1-4μs for typical result sets.
        /// </summary>
        private Task<TResult> ExecuteListPlanSyncWrapped<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw)
        {
            var list = _executor.Materialize(plan, cmd);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);

            object? result;
            if (plan.SingleResult)
            {
                result = plan.MethodName switch
                {
                    "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                    "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                    "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                    "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                    "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                    "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "LastOrDefault" => list.Count > 0 ? list[0] : null,
                    _ => list
                };
            }
            else
            {
                if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                {
                    var countList = nonGenericList.Count;
                    var covariantList = new List<object>(countList);
                    for (int i = 0; i < countList; i++)
                        covariantList.Add(nonGenericList[i]!);
                    result = covariantList;
                }
                else
                {
                    result = list;
                }
            }
            return Task.FromResult((TResult)result!);
        }

        /// <summary>PERF: Typed list materialization path.</summary>
        private async Task<TResult> ExecuteListPlanAsync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw, CancellationToken ct)
        {
            var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);

            object? result;
            if (plan.SingleResult)
            {
                result = plan.MethodName switch
                {
                    "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                    "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                    "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                    "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                    "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                    "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    "LastOrDefault" => list.Count > 0 ? list[0] : null,
                    _ => list
                };
            }
            else
            {
                if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                {
                    var countList = nonGenericList.Count;
                    var covariantList = new List<object>(countList);
                    for (int i = 0; i < countList; i++)
                        covariantList.Add(nonGenericList[i]!);
                    result = covariantList;
                }
                else
                {
                    result = list;
                }
            }
            return (TResult)result!;
        }

        // INTERNAL API: Optimized version that accepts array of values instead of Dictionary
        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, token), ct);
            return ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, ct);
        }

        // Overload that accepts pre-computed fixedParams to avoid:
        // 1. Expensive QueryPlan.GetHashCode() in _compiledParamSets ConcurrentDictionary on every call
        //    (QueryPlan is a sealed record with 20+ properties — auto-generated GetHashCode costs ~200ns)
        // 2. HashSet.Contains per parameter on every call (fixed params pre-filtered at compile time)
        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, object?[] parameterValues, KeyValuePair<string, object>[]? fixedParams, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledPreparedAsync<TResult>(plan, parameterValues, fixedParams, token), ct);
            return ExecuteCompiledPreparedAsync<TResult>(plan, parameterValues, fixedParams, ct);
        }

        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledInternalAsync<TResult>(plan, parameters, token), ct);
            return ExecuteCompiledInternalAsync<TResult>(plan, parameters, ct);
        }
        private async Task<TResult> ExecuteCompiledInternalAsync<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters, CancellationToken ct)
        {
            // Merge template parameters from the plan with the live execution values
            var finalParameters = new Dictionary<string, object>(plan.Parameters);
            foreach (var p in parameters)
            {
                finalParameters[p.Key] = p.Value;
            }
            // For cached queries (rare), use closure-based path.
            // For non-cached queries (common), inline execution directly to avoid closure allocation.
            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
                Func<Task<TResult>> queryExecutorFactory = () => ExecuteCompiledDictAsync<TResult>(plan, finalParameters, sw, ct);
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, finalParameters);
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
            }
            else
            {
                var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
                return await ExecuteCompiledDictAsync<TResult>(plan, finalParameters, sw, ct).ConfigureAwait(false);
            }
        }
        /// <summary>
        /// Extracted from lambda to avoid closure allocation on every compiled query call.
        /// </summary>
        private async Task<TResult> ExecuteCompiledDictAsync<TResult>(QueryPlan plan, Dictionary<string, object> finalParameters, Stopwatch? sw, CancellationToken ct)
        {
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            foreach (var p in finalParameters) cmd.AddOptimizedParam(p.Key, p.Value);
            object? result;
            if (plan.IsScalar)
            {
                var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                sw?.Stop();
                _ctx.Options.Logger?.LogQuery(plan.Sql, finalParameters, sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                if (scalarResult == null || scalarResult is DBNull)
                {
                    if (plan.MethodName is "Min" or "Max" or "Average" &&
                        typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                        throw new InvalidOperationException("Sequence contains no elements");
                    return default!;
                }
                result = ConvertScalarResult<TResult>(scalarResult)!;
            }
            else
            {
                // When TResult is List<object>, materialize directly to avoid covariant copy
                if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                {
                    var objectList = await _executor.MaterializeAsObjectListAsync(plan, cmd, ct).ConfigureAwait(false);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, finalParameters, sw?.Elapsed ?? default, objectList.Count);
                    return (TResult)(object)objectList;
                }

                var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                sw?.Stop();
                _ctx.Options.Logger?.LogQuery(plan.Sql, finalParameters, sw?.Elapsed ?? default, list.Count);
                if (plan.SingleResult)
                {
                    result = plan.MethodName switch
                    {
                        "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                        "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                        "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                        "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                        "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                        "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                        "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                        "LastOrDefault" => list.Count > 0 ? list[0] : null,
                        _ => list
                    };
                }
                else
                {
                    if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                    {
                        var countList = nonGenericList.Count;
                        var covariantList = new List<object>(countList);
                        for (int i = 0; i < countList; i++)
                            covariantList.Add(nonGenericList[i]!);
                        result = covariantList;
                    }
                    else
                    {
                        result = list;
                    }
                }
            }
            return (TResult)result!;
        }

        /// <summary>
        /// Bounded FIFO cache mapping QueryPlan → compiled parameter name set (O(1) vs O(N) lookup).
        /// Capped at 10 000 entries to prevent unbounded memory growth under adversarial or
        /// pathological query-shape churn (Q1/X1 audit finding). FIFO eviction: oldest-inserted
        /// entry is dropped first when the cap is exceeded.
        /// </summary>
        private static readonly nORM.Internal.BoundedCache<QueryPlan, HashSet<string>> _compiledParamSets
            = new(maxSize: 10_000);

        private Task<TResult> ExecuteCompiledInternalArrayAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            // For the caching path (rare), use async method.
            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                return ExecuteCompiledInternalArrayCachedAsync<TResult>(plan, parameterValues, ct);
            }

            // Ensure connection is ready (fast no-op when already open).
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCompiledInternalArraySlowAsync<TResult>(ensureTask, plan, parameterValues, ct);

            // Build command synchronously, then delegate to materialization.
            // This avoids an async state machine for the command setup work.
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;

            var compiledParams = plan.CompiledParameters;
            if (compiledParams.Count == 0)
            {
                foreach (var p in plan.Parameters)
                    cmd.AddOptimizedParam(p.Key, p.Value);
            }
            else
            {
                if (!_compiledParamSets.TryGet(plan, out var compiledSet))
                {
                    compiledSet = new HashSet<string>(compiledParams, StringComparer.Ordinal);
                    _compiledParamSets.Set(plan, compiledSet);
                }

                foreach (var p in plan.Parameters)
                {
                    if (!compiledSet.Contains(p.Key))
                        cmd.AddOptimizedParam(p.Key, p.Value);
                }
            }

            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            for (int i = 0; i < count; i++)
            {
                cmd.AddOptimizedParam(compiledParams[i], parameterValues[i] ?? DBNull.Value);
            }

            // Dispatch directly to materialization — avoids wrapping in another async method.
            // MaterializeAsObjectListAsync/MaterializeAsync handle cmd disposal via 'await using'.
            if (!plan.IsScalar && typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
            {
                return (Task<TResult>)(object)_executor.MaterializeAsObjectListAsync(plan, cmd, ct);
            }

            return ExecuteCompiledMaterializeAsync<TResult>(plan, cmd, ct);
        }

        private async Task<TResult> ExecuteCompiledInternalArraySlowAsync<TResult>(Task<DbConnection> ensureTask, QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);

            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;

            var compiledParams = plan.CompiledParameters;
            if (compiledParams.Count == 0)
            {
                foreach (var p in plan.Parameters)
                    cmd.AddOptimizedParam(p.Key, p.Value);
            }
            else
            {
                if (!_compiledParamSets.TryGet(plan, out var compiledSet))
                {
                    compiledSet = new HashSet<string>(compiledParams, StringComparer.Ordinal);
                    _compiledParamSets.Set(plan, compiledSet);
                }

                foreach (var p in plan.Parameters)
                {
                    if (!compiledSet.Contains(p.Key))
                        cmd.AddOptimizedParam(p.Key, p.Value);
                }
            }

            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            for (int i = 0; i < count; i++)
            {
                cmd.AddOptimizedParam(compiledParams[i], parameterValues[i] ?? DBNull.Value);
            }

            if (!plan.IsScalar && typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
            {
                return (TResult)(object)await _executor.MaterializeAsObjectListAsync(plan, cmd, ct).ConfigureAwait(false);
            }

            return await ExecuteCompiledMaterializeAsync<TResult>(plan, cmd, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Optimized compiled query execution that accepts pre-computed fixedParams.
        /// Avoids: (1) _compiledParamSets ConcurrentDictionary lookup (QueryPlan.GetHashCode on every call),
        /// (2) HashSet.Contains per param (fixed params pre-filtered at compile time).
        /// Routes through the same execution path as non-compiled queries for consistent JIT optimization.
        /// </summary>
        private Task<TResult> ExecuteCompiledPreparedAsync<TResult>(QueryPlan plan, object?[] parameterValues, KeyValuePair<string, object>[]? fixedParams, CancellationToken ct)
        {
            // For the caching path (rare), fall back to standard compiled path.
            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
                return ExecuteCompiledInternalArrayCachedAsync<TResult>(plan, parameterValues, ct);

            // Ensure connection is ready (fast no-op when already open).
            // This matches the non-compiled path to ensure identical behavior.
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCompiledPreparedSlowAsync<TResult>(ensureTask, plan, parameterValues, fixedParams, ct);

            // Synchronous command setup — same as non-compiled ExecuteQueryFromPlanAsync
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindCompiledParameters(cmd, plan, parameterValues, fixedParams);

            // Dispatch directly to the same materializer methods used by non-compiled queries.
            if (plan.IsScalar)
            {
                if (_ctx.Provider.PrefersSyncExecution)
                    return ExecuteScalarPlanSync<TResult>(plan, cmd, null);
                return ExecuteScalarPlanAsync<TResult>(plan, cmd, null, ct);
            }
            // Sync materialization for providers without true async I/O
            if (_ctx.Provider.PrefersSyncExecution)
                return ExecuteListPlanSyncWrapped<TResult>(plan, cmd, null);
            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (Task<TResult>)(object)ExecuteObjectListPlanAsync(plan, cmd, null, ct);
            return ExecuteListPlanAsync<TResult>(plan, cmd, null, ct);
        }

        private async Task<TResult> ExecuteCompiledPreparedSlowAsync<TResult>(Task<DbConnection> ensureTask, QueryPlan plan, object?[] parameterValues, KeyValuePair<string, object>[]? fixedParams, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindCompiledParameters(cmd, plan, parameterValues, fixedParams);

            if (plan.IsScalar)
            {
                if (_ctx.Provider.PrefersSyncExecution)
                    return await ExecuteScalarPlanSync<TResult>(plan, cmd, null).ConfigureAwait(false);
                return await ExecuteScalarPlanAsync<TResult>(plan, cmd, null, ct).ConfigureAwait(false);
            }
            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (TResult)(object)await ExecuteObjectListPlanAsync(plan, cmd, null, ct).ConfigureAwait(false);
            return await ExecuteListPlanAsync<TResult>(plan, cmd, null, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Inline parameter binding for compiled queries with pre-computed fixed params.
        /// When fixedParams is non-null, iterates a pre-filtered array instead of the full
        /// Parameters dictionary with HashSet lookups. Saves ~5 HashSet.Contains per call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void BindCompiledParameters(DbCommand cmd, QueryPlan plan, object?[] parameterValues, KeyValuePair<string, object>[]? fixedParams)
        {
            var compiledParams = plan.CompiledParameters;
            if (fixedParams != null)
            {
                // Fast path: pre-computed fixed params — no HashSet lookup needed
                for (int i = 0; i < fixedParams.Length; i++)
                    cmd.AddOptimizedParam(fixedParams[i].Key, fixedParams[i].Value);
            }
            else
            {
                // Fallback: no pre-computed fixedParams available, add all plan parameters unfiltered
                foreach (var p in plan.Parameters)
                    cmd.AddOptimizedParam(p.Key, p.Value);
            }

            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            for (int i = 0; i < count; i++)
                cmd.AddOptimizedParam(compiledParams[i], parameterValues[i] ?? DBNull.Value);
        }

        /// <summary>
        /// Compiled query execution with command pooling. The DbCommand is created once
        /// (with Prepare()) and reused across calls — only parameter values are updated.
        /// This eliminates per-call costs of: DbCommand allocation (~0.5μs), DbParameter creation
        /// (~0.3μs per param), and SQL compilation via sqlite3_prepare_v2 (~2-5μs).
        /// Falls back to standard path for retry policies, caching, and first-call initialization.
        /// </summary>
        internal Task<TResult> ExecuteCompiledPooledAsync<TResult>(
            QueryPlan plan, object?[] parameterValues,
            KeyValuePair<string, object>[]? fixedParams,
            CompiledQueryState state, CancellationToken ct)
        {
            // Fall back to standard path for retry policies and caching
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy)
                    .ExecuteAsync((_, token) => ExecuteCompiledPooledInternalAsync<TResult>(plan, parameterValues, fixedParams, state, token), ct);

            return ExecuteCompiledPooledInternalAsync<TResult>(plan, parameterValues, fixedParams, state, ct);
        }

        private Task<TResult> ExecuteCompiledPooledInternalAsync<TResult>(
            QueryPlan plan, object?[] parameterValues,
            KeyValuePair<string, object>[]? fixedParams,
            CompiledQueryState state, CancellationToken ct)
        {
            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
                return ExecuteCompiledInternalArrayCachedAsync<TResult>(plan, parameterValues, ct);

            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCompiledPooledSlowAsync<TResult>(ensureTask, plan, parameterValues, fixedParams, state, ct);

            // Q1 fix: reuse pooled prepared command to avoid repeated allocations; create new if pool is empty
            if (!state.CommandPool.TryDequeue(out var cmd))
                cmd = CreateAndPreparePooledCommand(plan, fixedParams, state);

            // Only update compiled parameter values — fixed params are already set
            UpdateCompiledParameterValues(cmd, plan.CompiledParameters, parameterValues, state.FixedParamCount);

            // Inline materialization — command returned to pool after use
            if (plan.IsScalar)
                return ReturnCommandToPool(state.CommandPool, cmd, ExecutePooledScalarAsync<TResult>(plan, cmd, ct));
            // Sync materialization for providers without true async I/O
            if (_ctx.Provider.PrefersSyncExecution)
            {
                var result = ExecutePooledListSync<TResult>(plan, cmd);
                state.CommandPool.Enqueue(cmd);
                return result;
            }
            // Handle List<object> covariant case
            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (Task<TResult>)(object)ReturnCommandToPool(state.CommandPool, cmd, ExecutePooledObjectListAsync(plan, cmd, ct));
            return ReturnCommandToPool(state.CommandPool, cmd, ExecutePooledListAsync<TResult>(plan, cmd, ct));
        }

        private async Task<TResult> ExecuteCompiledPooledSlowAsync<TResult>(
            Task<DbConnection> ensureTask, QueryPlan plan, object?[] parameterValues,
            KeyValuePair<string, object>[]? fixedParams,
            CompiledQueryState state, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            // Q1 fix: reuse pooled prepared command to avoid repeated allocations; create new if pool is empty
            if (!state.CommandPool.TryDequeue(out var cmd))
                cmd = CreateAndPreparePooledCommand(plan, fixedParams, state);
            UpdateCompiledParameterValues(cmd, plan.CompiledParameters, parameterValues, state.FixedParamCount);
            try
            {
                if (plan.IsScalar)
                    return await ExecutePooledScalarAsync<TResult>(plan, cmd, ct).ConfigureAwait(false);
                return await ExecutePooledListAsync<TResult>(plan, cmd, ct).ConfigureAwait(false);
            }
            finally
            {
                state.CommandPool.Enqueue(cmd);
            }
        }

        private DbCommand CreateAndPreparePooledCommand(
            QueryPlan plan, KeyValuePair<string, object>[]? fixedParams, CompiledQueryState state)
        {
            var cmd = _ctx.CreateCommand();
            cmd.CommandText = plan.Sql;
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;

            // Pre-create fixed parameters (values never change)
            int fixedCount = 0;
            if (fixedParams != null)
            {
                for (int i = 0; i < fixedParams.Length; i++)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = fixedParams[i].Key;
                    p.Value = fixedParams[i].Value;
                    // Set DbType from value for optimal binding
                    if (fixedParams[i].Value is int) p.DbType = DbType.Int32;
                    else if (fixedParams[i].Value is string s) { p.DbType = DbType.String; p.Size = s.Length <= MaxInlineStringSize ? s.Length : -1; }
                    else if (fixedParams[i].Value is long) p.DbType = DbType.Int64;
                    else if (fixedParams[i].Value is bool) p.DbType = DbType.Boolean;
                    else if (fixedParams[i].Value is decimal) p.DbType = DbType.Decimal;
                    else if (fixedParams[i].Value is DateTime) p.DbType = DbType.DateTime2;
                    else if (fixedParams[i].Value is Guid) p.DbType = DbType.Guid;
                    else if (fixedParams[i].Value is double) p.DbType = DbType.Double;
                    else if (fixedParams[i].Value is float) p.DbType = DbType.Single;
                    else if (fixedParams[i].Value is short) p.DbType = DbType.Int16;
                    else if (fixedParams[i].Value is byte) p.DbType = DbType.Byte;
                    else if (fixedParams[i].Value is byte[]) p.DbType = DbType.Binary;
                    cmd.Parameters.Add(p);
                }
                fixedCount = fixedParams.Length;
            }
            else
            {
                // No compiled params — add all plan parameters
                foreach (var kvp in plan.Parameters)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = kvp.Key;
                    p.Value = kvp.Value;
                    cmd.Parameters.Add(p);
                    fixedCount++;
                }
            }

            // Pre-create compiled parameter slots (values updated per call)
            for (int i = 0; i < plan.CompiledParameters.Count; i++)
            {
                var p = cmd.CreateParameter();
                p.ParameterName = plan.CompiledParameters[i];
                p.Value = DBNull.Value;
                cmd.Parameters.Add(p);
            }

            // Prepare the command — compiles SQL once, subsequent executions skip sqlite3_prepare_v2.
            // Prepare() is optional — some providers (e.g., in-memory) throw NotSupportedException.
            try { cmd.Prepare(); } catch (NotSupportedException) { } catch (InvalidOperationException) { }

            state.FixedParamCount = fixedCount;
            return cmd;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void UpdateCompiledParameterValues(
            DbCommand cmd, IReadOnlyList<string> compiledParams,
            object?[] parameterValues, int fixedParamCount)
        {
            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            // P1 fix: call AssignValue (not direct .Value assignment) so that DbType and Size
            // are reset when the value is null, preventing stale metadata carry-over.
            for (int i = 0; i < count; i++)
                ParameterAssign.AssignValue(cmd.Parameters[fixedParamCount + i], parameterValues[i]);
        }

        /// <summary>
        /// Fully synchronous materialization for pooled commands.
        /// Eliminates async state machine overhead for providers like SQLite that lack true async I/O.
        /// The command is NOT disposed (pooled for reuse). Only the reader is disposed.
        /// </summary>
        private Task<TResult> ExecutePooledListSync<TResult>(QueryPlan plan, DbCommand cmd)
        {
            var capacity = plan.SingleResult ? 1 : (plan.Take ?? DefaultListCapacity);
            var list = _executor.CreateListForType(plan.ElementType, capacity);
            var materializer = plan.SyncMaterializer;

            using var reader = cmd.ExecuteReaderWithInterception(_ctx, CommandBehavior.SingleResult);

            if (plan.SingleResult)
            {
                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                for (int row = 0; row < maxRows; row++)
                {
                    if (!reader.Read()) break;
                    list.Add(materializer(reader));
                }
            }
            else
            {
                while (reader.Read())
                    list.Add(materializer(reader));
            }

            if (plan.SingleResult)
                return Task.FromResult((TResult)HandleSingleResult(plan, list));

            // Handle List<object> covariant case (e.g., join queries returning anonymous types)
            if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
            {
                var countList = nonGenericList.Count;
                var covariantList = new List<object>(countList);
                for (int i = 0; i < countList; i++)
                    covariantList.Add(nonGenericList[i]!);
                return Task.FromResult((TResult)(object)covariantList);
            }

            return Task.FromResult((TResult)(object)list);
        }

        /// <summary>Awaits the materializer task then returns the pooled command for reuse.</summary>
        private static async Task<TResult> ReturnCommandToPool<TResult>(
            ConcurrentQueue<DbCommand> pool,
            DbCommand cmd, Task<TResult> work)
        {
            try { return await work.ConfigureAwait(false); }
            finally { pool.Enqueue(cmd); }
        }

        /// <summary>
        /// Optimized compiled query path using fresh commands with sync materialization.
        /// Microsoft.Data.Sqlite internally caches prepared statements, so pooled DbCommand reuse
        /// adds overhead (sqlite3_reset, internal state management) without saving SQL compilation.
        /// Fresh commands avoid this overhead and match the non-compiled execution path's performance.
        /// </summary>
        private Task<TResult> ExecuteCompiledFreshSync<TResult>(
            QueryPlan plan, object?[] parameterValues,
            KeyValuePair<string, object>[]? fixedParams)
        {
            using var cmd = _ctx.CreateCommand();
            cmd.CommandText = plan.Sql;

            // Bind fixed parameters (constants from the compiled expression)
            if (fixedParams != null)
            {
                for (int i = 0; i < fixedParams.Length; i++)
                    cmd.AddOptimizedParam(fixedParams[i].Key, fixedParams[i].Value);
            }
            else
            {
                // No compiled params — add all plan parameters
                foreach (var kvp in plan.Parameters)
                    cmd.AddOptimizedParam(kvp.Key, kvp.Value);
            }

            // Bind compiled parameters (values from the caller)
            var compiledParams = plan.CompiledParameters;
            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            for (int i = 0; i < count; i++)
                cmd.AddOptimizedParam(compiledParams[i], parameterValues[i] ?? DBNull.Value);

            // Sync materialization — no async state machine overhead
            var capacity = plan.SingleResult ? 1 : (plan.Take ?? DefaultListCapacity);
            var list = _executor.CreateListForType(plan.ElementType, capacity);
            var materializer = plan.SyncMaterializer;

            using var reader = cmd.ExecuteReaderWithInterception(_ctx, CommandBehavior.SingleResult);

            if (plan.SingleResult)
            {
                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                for (int row = 0; row < maxRows; row++)
                {
                    if (!reader.Read()) break;
                    list.Add(materializer(reader));
                }
                return Task.FromResult((TResult)HandleSingleResult(plan, list));
            }

            while (reader.Read())
                list.Add(materializer(reader));

            // Handle List<object> covariant case
            if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
            {
                var countList = nonGenericList.Count;
                var covariantList = new List<object>(countList);
                for (int i = 0; i < countList; i++)
                    covariantList.Add(nonGenericList[i]!);
                return Task.FromResult((TResult)(object)covariantList);
            }

            return Task.FromResult((TResult)(object)list);
        }

        /// <summary>PERF: Inline list materialization for pooled commands (not disposed).</summary>
        private async Task<TResult> ExecutePooledListAsync<TResult>(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var capacity = plan.SingleResult ? 1 : (plan.Take ?? DefaultListCapacity);
            var list = _executor.CreateListForType(plan.ElementType, capacity);
            var materializer = plan.SyncMaterializer;

            // Use interception-aware reader for correctness
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(
                _ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);

            if (plan.SingleResult)
            {
                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                for (int row = 0; row < maxRows; row++)
                {
                    if (!await reader.ReadAsync(ct).ConfigureAwait(false)) break;
                    list.Add(materializer(reader));
                }
            }
            else
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    list.Add(materializer(reader));
            }

            if (plan.SingleResult)
            {
                return (TResult)HandleSingleResult(plan, list);
            }
            return (TResult)(object)list;
        }

        /// <summary>PERF: Materialization into List&lt;object&gt; for pooled commands (handles covariant anonymous types).</summary>
        private async Task<List<object>> ExecutePooledObjectListAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var capacity = plan.Take ?? DefaultListCapacity;
            var list = new List<object>(capacity);
            var materializer = plan.SyncMaterializer;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(
                _ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                list.Add(materializer(reader));

            return list;
        }

        /// <summary>PERF: Inline scalar execution for pooled commands (not disposed).</summary>
        private async Task<TResult> ExecutePooledScalarAsync<TResult>(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            if (scalarResult == null || scalarResult is DBNull)
            {
                if (plan.MethodName is "Min" or "Max" or "Average" &&
                    typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                    throw new InvalidOperationException("Sequence contains no elements");
                return default(TResult)!;
            }
            return ConvertScalarResult<TResult>(scalarResult)!;
        }

        private static object HandleSingleResult(QueryPlan plan, IList list)
        {
            return plan.MethodName switch
            {
                "First" => list.Count > 0 ? list[0]! : throw new InvalidOperationException("Sequence contains no elements"),
                "FirstOrDefault" => list.Count > 0 ? list[0]! : null!,
                "Single" => list.Count == 1 ? list[0]! : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                "SingleOrDefault" => list.Count == 0 ? null! : list.Count == 1 ? list[0]! : throw new InvalidOperationException("Sequence contains more than one element"),
                "ElementAt" => list.Count > 0 ? list[0]! : throw new ArgumentOutOfRangeException("index"),
                "ElementAtOrDefault" => list.Count > 0 ? list[0]! : null!,
                "Last" => list.Count > 0 ? list[0]! : throw new InvalidOperationException("Sequence contains no elements"),
                "LastOrDefault" => list.Count > 0 ? list[0]! : null!,
                _ => list
            };
        }

        private async Task<TResult> ExecuteCompiledMaterializeAsync<TResult>(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            object? result;
            if (plan.IsScalar)
            {
                var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                if (scalarResult == null || scalarResult is DBNull)
                {
                    if (plan.MethodName is "Min" or "Max" or "Average" &&
                        typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                        throw new InvalidOperationException("Sequence contains no elements");
                    return default!;
                }
                result = ConvertScalarResult<TResult>(scalarResult)!;
            }
            else
            {
                // When TResult is List<object> but plan.ElementType is a concrete type,
                // materialize directly into List<object> to avoid creating List<ConcreteType>
                // then copying all elements to a new List<object> (covariant copy).
                if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                {
                    var objectList = await _executor.MaterializeAsObjectListAsync(plan, cmd, ct).ConfigureAwait(false);
                    return (TResult)(object)objectList;
                }

                var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);

                if (plan.SingleResult)
                {
                    result = plan.MethodName switch
                    {
                        "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                        "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                        "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                        "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                        "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                        "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                        "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                        "LastOrDefault" => list.Count > 0 ? list[0] : null,
                        _ => list
                    };
                }
                else
                {
                    if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                    {
                        var countList = nonGenericList.Count;
                        var covariantList = new List<object>(countList);
                        for (int i2 = 0; i2 < countList; i2++)
                            covariantList.Add(nonGenericList[i2]!);
                        result = covariantList;
                    }
                    else
                    {
                        result = list;
                    }
                }
            }
            return (TResult)result!;
        }

        private async Task<TResult> ExecuteCompiledInternalArrayCachedAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            var dict = new Dictionary<string, object>(plan.Parameters);
            for (int i = 0; i < plan.CompiledParameters.Count && i < parameterValues.Length; i++)
                dict[plan.CompiledParameters[i]] = parameterValues[i] ?? DBNull.Value;

            var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, dict);
            var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
            return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration,
                () => ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, ct), ct).ConfigureAwait(false);
        }
        private bool TryGetSimpleQuery(Expression expr, out string sql, out Dictionary<string, object> parameters, out string? resultMethodName)
        {
            sql = string.Empty;
            parameters = _emptyParams;
            resultMethodName = null;
            if (_ctx.Options.GlobalFilters.Count > 0 || _ctx.Options.TenantProvider != null)
                return false;
            // Traverse to find root query and optional Where predicate
            MethodCallExpression? whereCall = null;
            Expression current = expr;
            // Unwrap result operators like First/Single/Take and skip AsNoTracking
            while (current is MethodCallExpression mc)
            {
                if (mc.Method.DeclaringType == typeof(Queryable))
                {
                    if (mc.Method.Name == nameof(Queryable.Where))
                    {
                        if (whereCall != null) return false; // only support single Where
                        whereCall = mc;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault))
                    {
                        resultMethodName = mc.Method.Name;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.Take))
                    {
                        // Accept Take so queries like .Where(...).Take(10) hit simple path;
                        // reject non-constant or negative Take values so validation falls through.
                        if (mc.Arguments[1] is not ConstantExpression takeConst || takeConst.Value is not int tv || tv < 0)
                            return false;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault))
                    {
                        return false;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (mc.Method.Name == "AsNoTracking" && mc.Arguments.Count >= 1)
                {
                    // Skip AsNoTracking (nORM-specific method, not on Queryable)
                    current = mc.Arguments[0];
                }
                else
                {
                    return false;
                }
            }
            if (current is not ConstantExpression constant)
                return false;
            var elementType = GetElementType(constant);
            var map = _ctx.GetMapping(elementType);
            // Use structural key instead of full ToString() to avoid string allocation.
            // For simple predicates (member == value), the member name is sufficient.
            string whereKey;
            if (whereCall == null)
                whereKey = "";
            else
            {
                var wLambda = StripQuotes(whereCall.Arguments[1]) as LambdaExpression;
                if (wLambda == null) return false;
                if (wLambda.Body is MemberExpression wm)
                    whereKey = wm.Member.Name;
                else if (wLambda.Body is BinaryExpression wb && wb.Left is MemberExpression wbm)
                    whereKey = wbm.Member.Name;
                else
                    whereKey = wLambda.Body.ToString(); // fallback for complex predicates
            }
            var cacheKey = string.Concat("SIMPLE:", elementType.FullName, ":", resultMethodName ?? "", ":", whereKey);
            if (!_simpleSqlCache.TryGetValue(cacheKey, out var cachedSql))
            {
                // Use string interpolation instead of StringBuilder for small queries;
                // StringBuilder overhead is not worth it for simple SELECT statements.
                var columnList = string.Join(", ", map.Columns.Select(c => c.EscCol));
                string whereClause = "";

                if (whereCall != null)
                {
                    var lambda = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                    // Support boolean member: u => u.IsActive
                    if (lambda.Body is MemberExpression boolMember && boolMember.Type == typeof(bool))
                    {
                        if (!map.ColumnsByName.TryGetValue(boolMember.Member.Name, out var boolCol))
                            return false;
                        whereClause = $" WHERE {boolCol.EscCol} = {_ctx.Provider.BooleanTrueLiteral}";
                    }
                    // Support negated boolean member: u => !u.IsActive
                    else if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr
                             && notExpr.Operand is MemberExpression negBoolMember
                             && negBoolMember.Type == typeof(bool))
                    {
                        if (!map.ColumnsByName.TryGetValue(negBoolMember.Member.Name, out var boolCol))
                            return false;
                        whereClause = $" WHERE {boolCol.EscCol} = {_ctx.Provider.BooleanFalseLiteral}";
                    }
                    else
                    {
                        if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                            return false;
                        if (be.Left is not MemberExpression me)
                            return false;
                        if (!map.ColumnsByName.TryGetValue(me.Member.Name, out var column))
                            return false;
                        var paramName = _ctx.Provider.ParamPrefix + "p0";
                        whereClause = $" WHERE {column.EscCol} = {paramName}";
                        // Use ExpressionValueExtractor instead of Compile().DynamicInvoke();
                        // DynamicInvoke is significantly slower and poses RCE risks.
                        if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                            return false;
                        parameters = new Dictionary<string, object>(1) { [paramName] = value! };
                    }
                }

                sql = $"SELECT {columnList} FROM {map.EscTable}{whereClause}";
                _simpleSqlCache[cacheKey] = sql;
                return true;
            }
            sql = cachedSql!;
            // SQL cached; still need parameter value if where present
            if (whereCall != null)
            {
                var lambda = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                if ((lambda.Body is MemberExpression boolMember2 && boolMember2.Type == typeof(bool))
                    || (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr3
                        && notExpr3.Operand is MemberExpression negBm && negBm.Type == typeof(bool)))
                {
                    // no parameter to bind for boolean literal predicate
                }
                else
                {
                    if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                        return false;
                    var paramName = _ctx.Provider.ParamPrefix + "p0";
                    if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                        return false;
                    parameters = new Dictionary<string, object>(1) { [paramName] = value! };
                }
            }
            return true;
        }

        // Reusable empty dictionary to avoid allocation when count has no parameters.
        // INVARIANT: _emptyParams must NEVER be mutated. It is shared across all callers as a
        // sentinel for "no parameters". Code that needs to add entries must first check
        // ReferenceEquals(parameters, _emptyParams) and allocate a new Dictionary before writing.
        private static readonly Dictionary<string, object> _emptyParams = new();
        // Cached empty includes list and table name lists for simple query path
        private static readonly List<IncludePlan> _emptyIncludes = new();
        private static readonly ConcurrentDictionary<string, List<string>> _simpleQueryTableCache = new();

        /// <summary>
        /// Direct count path that works on the source expression directly,
        /// bypassing the need to wrap in Expression.Call(Queryable.Count) and re-parse.
        /// Saves one MethodCallExpression + Type[] allocation per count call.
        /// </summary>
        internal bool TryDirectCountAsync(Expression sourceExpression, CancellationToken ct, out Task<int> result)
        {
            result = default!;
            if (_ctx.Options.GlobalFilters.Count > 0 || _ctx.Options.TenantProvider != null)
                return false;

            // Unwrap the source expression to find the root and optional Where predicate
            Expression source = sourceExpression;
            LambdaExpression? predicate = null;

            // Unwrap Where
            if (source is MethodCallExpression whereCall && whereCall.Method.DeclaringType == typeof(Queryable) && whereCall.Method.Name == nameof(Queryable.Where))
            {
                predicate = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                source = whereCall.Arguments[0];
            }

            // Unwrap AsNoTracking and similar passthrough methods
            while (source is MethodCallExpression m && m.Arguments.Count == 1 &&
                   m.Method.Name is "AsNoTracking" or "AsTracking")
                source = m.Arguments[0];

            if (source is not ConstantExpression constant)
                return false;

            var elementType = GetElementType(constant);
            var map = _ctx.GetMapping(elementType);

            // Use structural key instead of string.Concat to avoid string allocation per count call.
            // Q1 fix: include null-shape in key so col==null and col==value produce distinct cache entries.
            string predicateKey;
            if (predicate == null)
                predicateKey = "";
            else if (predicate.Body is MemberExpression pm)
                predicateKey = pm.Member.Name; // interned by CLR, no alloc
            else if (predicate.Body is BinaryExpression pb && pb.Left is MemberExpression pbm)
                predicateKey = IsNullConstant(pb.Right) ? pbm.Member.Name + "==NULL" : pbm.Member.Name;
            else
                return false; // Complex predicates fall back to normal path

            var cacheKey = (elementType, predicateKey);
            Dictionary<string, object> parameters = _emptyParams;

            if (!_countSqlCache.TryGetValue(cacheKey, out var cached))
            {
                string whereClause = string.Empty;
                if (predicate != null)
                {
                    if (!TryBuildCountWhereClause(predicate, map, ref parameters, out whereClause, populateParameters: true))
                        return false;
                }
                var sql2 = $"SELECT COUNT(*) FROM {map.EscTable}{whereClause}";
                bool needsParam = !ReferenceEquals(parameters, _emptyParams);
                _countSqlCache[cacheKey] = (sql2, needsParam);
                cached = (sql2, needsParam);
            }
            else if (predicate != null && cached.NeedsParam)
            {
                // Only re-extract parameter value when the predicate actually uses a parameter
                if (!TryBuildCountWhereClause(predicate, map, ref parameters, out _, populateParameters: true))
                    return false;
            }
            var sql = cached.Sql;

            if (_ctx.Options.RetryPolicy != null)
                result = new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCountAsync<int>(sql, parameters, token), ct);
            else
                result = ExecuteCountAsync<int>(sql, parameters, ct);
            return true;
        }

        private bool TryGetCountQuery(Expression expr, out string sql, out Dictionary<string, object> parameters)
        {
            sql = string.Empty;
            parameters = _emptyParams;

            if (_ctx.Options.GlobalFilters.Count > 0 || _ctx.Options.TenantProvider != null)
                return false;

            if (expr is not MethodCallExpression rootCall || rootCall.Method.DeclaringType != typeof(Queryable))
                return false;

            var methodName = rootCall.Method.Name;
            if (methodName is not nameof(Queryable.Count) and not nameof(Queryable.LongCount))
                return false;

            Expression source = rootCall.Arguments[0];
            LambdaExpression? predicate = rootCall.Arguments.Count > 1 ? (LambdaExpression)StripQuotes(rootCall.Arguments[1]) : null;

            if (source is MethodCallExpression whereCall && whereCall.Method.DeclaringType == typeof(Queryable) && whereCall.Method.Name == nameof(Queryable.Where))
            {
                if (predicate != null)
                    return false; // Only support a single predicate source

                predicate = (LambdaExpression)StripQuotes(whereCall.Arguments[1]);
                source = whereCall.Arguments[0];
            }

            if (source is not ConstantExpression constant)
                return false;

            var elementType = GetElementType(constant);
            // Use structural hash instead of predicate.Body.ToString() to avoid
            // string allocation on every count call. For simple predicates (bool member,
            // equality), the member name is sufficient as a cache key.
            // Q1 fix: include null-shape in key so col==null and col==value produce distinct cache entries.
            string predicateKey;
            if (predicate == null)
                predicateKey = "";
            else if (predicate.Body is MemberExpression pm)
                predicateKey = pm.Member.Name;
            else if (predicate.Body is BinaryExpression pb && pb.Left is MemberExpression pbm)
                predicateKey = IsNullConstant(pb.Right)
                    ? string.Concat(pbm.Member.Name, "=", pb.NodeType.ToString(), ":NULL")
                    : string.Concat(pbm.Member.Name, "=", pb.NodeType.ToString());
            else
                predicateKey = predicate.Body.ToString(); // fallback for complex predicates

            // Use ValueTuple key to avoid string.Concat allocation per count call
            var countCacheKey = (elementType, predicateKey);

            if (_countSqlCache.TryGetValue(countCacheKey, out var cached))
            {
                sql = cached.Sql;
                if (cached.NeedsParam && predicate != null)
                {
                    // Only call GetMapping when actually needed for parameter extraction
                    var map2 = _ctx.GetMapping(elementType);
                    if (!TryBuildCountWhereClause(predicate, map2, ref parameters, out _, populateParameters: true))
                        return false;
                }
                return true;
            }

            {
                // First-time path: must call GetMapping for SQL generation
                var map = _ctx.GetMapping(elementType);
                string whereClause = string.Empty;
                bool needsParam = false;

                if (predicate != null)
                {
                    if (!TryBuildCountWhereClause(predicate, map, ref parameters, out whereClause, populateParameters: true))
                        return false;
                    needsParam = !ReferenceEquals(parameters, _emptyParams);
                }

                sql = $"SELECT COUNT(*) FROM {map.EscTable}{whereClause}";
                _countSqlCache[countCacheKey] = (sql, needsParam);
                return true;
            }
        }

        /// <summary>
        /// Q1 fix: returns true when <paramref name="e"/> is a null literal or a Nullable&lt;T&gt; Convert
        /// wrapping a null literal, so cache keys distinguish col==null from col==value.
        /// </summary>
        private static bool IsNullConstant(Expression e) =>
            e is ConstantExpression { Value: null } ||
            (e is UnaryExpression { NodeType: ExpressionType.Convert } ue &&
             ue.Operand is ConstantExpression { Value: null });

        /// <summary>
        /// Builds a WHERE clause for count queries from a simple lambda predicate.
        /// <para>
        /// NOTE on the <c>ref parameters</c> pattern: the <paramref name="parameters"/> argument is passed
        /// by-ref so that this method can replace the caller's <see cref="_emptyParams"/> sentinel with a
        /// freshly-allocated dictionary when the predicate requires a parameter binding. Callers must not
        /// cache the dictionary reference across calls — each invocation may replace it. The <c>out whereClause</c>
        /// is always set (to <see cref="string.Empty"/> on failure) so callers can safely ignore it when the
        /// method returns <c>false</c>.
        /// </para>
        /// </summary>
        private bool TryBuildCountWhereClause(LambdaExpression lambda, TableMapping map, ref Dictionary<string, object> parameters, out string whereClause, bool populateParameters)
        {
            whereClause = string.Empty;

            if (lambda.Body is MemberExpression boolMember && boolMember.Type == typeof(bool))
            {
                if (!map.ColumnsByName.TryGetValue(boolMember.Member.Name, out var boolCol))
                    return false;

                whereClause = $" WHERE {boolCol.EscCol} = {_ctx.Provider.BooleanTrueLiteral}";
                return true;
            }

            if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr2
                && notExpr2.Operand is MemberExpression negBoolMember2
                && negBoolMember2.Type == typeof(bool))
            {
                if (!map.ColumnsByName.TryGetValue(negBoolMember2.Member.Name, out var boolCol2))
                    return false;

                whereClause = $" WHERE {boolCol2.EscCol} = {_ctx.Provider.BooleanFalseLiteral}";
                return true;
            }

            if (lambda.Body is BinaryExpression be && be.NodeType == ExpressionType.Equal && be.Left is MemberExpression me)
            {
                if (!map.ColumnsByName.TryGetValue(me.Member.Name, out var column))
                    return false;

                if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var constValue))
                    return false;

                if (constValue == null)
                {
                    whereClause = $" WHERE {column.EscCol} IS NULL";
                    return true;
                }

                var paramName = _ctx.Provider.ParamPrefix + "p0";
                whereClause = $" WHERE {column.EscCol} = {paramName}";

                if (populateParameters)
                {
                    // Only allocate a new dictionary when we actually need to add parameters
                    if (ReferenceEquals(parameters, _emptyParams))
                        parameters = new Dictionary<string, object>(1);
                    parameters[paramName] = constValue!;
                }

                return true;
            }

            return false;
        }
        private Task<TResult> ExecuteCountAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            // Split into fast (no logger) and slow (with logger) paths.
            // The fast path avoids Stopwatch allocation and logger null checks.
            if (_ctx.Options.Logger != null)
                return ExecuteCountSlowAsync<TResult>(sql, parameters, ct);
            return ExecuteCountFastAsync<TResult>(sql, parameters, ct);
        }

        private Task<TResult> ExecuteCountFastAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            // Non-async entry point — avoids state machine when connection is already open.
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCountFastSlowAsync<TResult>(ensureTask, sql, parameters, ct);

            // For providers without true async I/O (SQLite), use pooled prepared command
            // for parameterless count queries to eliminate per-call command creation/disposal.
            if (_ctx.Provider.PrefersSyncExecution && ReferenceEquals(parameters, _emptyParams))
            {
                var entry = _pooledCountCommands.GetOrAdd(sql, static (s, ctx) =>
                {
                    var c = ctx.CreateCommand();
                    c.CommandText = s;
                    // Prepare() is optional — some providers throw NotSupportedException or InvalidOperationException.
                    try { c.Prepare(); } catch (NotSupportedException) { } catch (InvalidOperationException) { }
                    return (c, new object());
                }, _ctx);

                lock (entry.Lock)
                {
                    ct.ThrowIfCancellationRequested();
                    // Rebind CurrentTransaction on every use — CreateCommand() only binds the
                    // transaction at creation time; reuse across transaction changes would run
                    // the count against a stale (or null) transaction binding.
                    entry.Cmd.Transaction = _ctx.CurrentTransaction;
                    var scalar = entry.Cmd.ExecuteScalarWithInterception(_ctx);
                    if (scalar == null || scalar is DBNull)
                        return Task.FromResult(default(TResult)!);
                    return Task.FromResult(ConvertScalarResult<TResult>(scalar));
                }
            }

            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            // For providers without true async I/O (SQLite), use sync ExecuteScalar directly
            // to avoid the ValueTask/Task wrapper overhead from ExecuteScalarAsync.
            if (_ctx.Provider.PrefersSyncExecution)
            {
                ct.ThrowIfCancellationRequested();
                var scalar = cmd.ExecuteScalarWithInterception(_ctx);
                cmd.Dispose();
                if (scalar == null || scalar is DBNull)
                    return Task.FromResult(default(TResult)!);
                return Task.FromResult(ConvertScalarResult<TResult>(scalar));
            }

            var scalarTask = cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct);
            if (scalarTask.IsCompletedSuccessfully)
            {
                var scalar = scalarTask.Result;
                cmd.Dispose();
                if (scalar == null || scalar is DBNull)
                    return Task.FromResult(default(TResult)!);
                return Task.FromResult(ConvertScalarResult<TResult>(scalar));
            }
            return ExecuteCountFinalizeAsync<TResult>(scalarTask, cmd);
        }

        private async Task<TResult> ExecuteCountFastSlowAsync<TResult>(Task<DbConnection> ensureTask, string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var scalar = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            if (scalar == null || scalar is DBNull)
                return default!;
            return ConvertScalarResult<TResult>(scalar);
        }

        private async Task<TResult> ExecuteCountFinalizeAsync<TResult>(Task<object?> scalarTask, DbCommand cmd)
        {
            try
            {
                var scalar = await scalarTask.ConfigureAwait(false);
                if (scalar == null || scalar is DBNull)
                    return default!;
                return ConvertScalarResult<TResult>(scalar);
            }
            finally
            {
                cmd.Dispose();
            }
        }

        private async Task<TResult> ExecuteCountSlowAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var scalar = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw.Elapsed, scalar == null || scalar is DBNull ? 0 : 1);
            if (scalar == null || scalar is DBNull)
                return default!;

            return ConvertScalarResult<TResult>(scalar);
        }

        private TResult ExecuteCountSync<TResult>(string sql, Dictionary<string, object> parameters)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);

            var scalar = cmd.ExecuteScalarWithInterception(_ctx);
            sw?.Stop();
            if (sw != null)
                _ctx.Options.Logger?.LogQuery(sql, parameters, sw.Elapsed, scalar == null || scalar is DBNull ? 0 : 1);
            if (scalar == null || scalar is DBNull)
                return default!;

            return ConvertScalarResult<TResult>(scalar);
        }

        private async Task<TResult> ExecuteSimpleAsync<TResult>(string sql, Dictionary<string, object> parameters, string? requestedMethodName, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            Type resultType = typeof(TResult);
            bool returnsList = false;
            Type elementType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(List<>) || genDef == typeof(IEnumerable<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                    returnsList = true;
                }
                else
                {
                    elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
                }
            }
            else
            {
                elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
            }
            var mapping = _ctx.GetMapping(elementType);

            // Use singleton MaterializerFactory (it only wraps static caches)
            var materializer = _sharedMaterializerFactory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(mapping, elementType);

            var plan = new QueryPlan(
                sql,
                parameters,
                Array.Empty<string>(),
                materializer,
                syncMaterializer,
                elementType,
                IsScalar: false,
                SingleResult: !returnsList,
                NoTracking: false,
                MethodName: returnsList ? "ToList" : nameof(Queryable.FirstOrDefault),
                Includes: _emptyIncludes,
                GroupJoinInfo: null,
                Tables: _simpleQueryTableCache.GetOrAdd(mapping.TableName, static t => new List<string>(1) { t }),
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw?.Elapsed ?? default, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // Preserve First vs FirstOrDefault semantics.
            if (list.Count > 0) return (TResult)list[0]!;
            if (requestedMethodName == nameof(Queryable.First))
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }
        private TResult ExecuteSimpleSync<TResult>(string sql, Dictionary<string, object> parameters, string? requestedMethodName = null)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            Type resultType = typeof(TResult);
            bool returnsList = false;
            Type elementType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(List<>) || genDef == typeof(IEnumerable<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                    returnsList = true;
                }
                else
                {
                    elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
                }
            }
            else
            {
                elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
            }
            var mapping = _ctx.GetMapping(elementType);

            // Use singleton MaterializerFactory (it only wraps static caches)
            var materializer = _sharedMaterializerFactory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(mapping, elementType);

            var plan = new QueryPlan(
                sql,
                parameters,
                Array.Empty<string>(),
                materializer,
                syncMaterializer,
                elementType,
                IsScalar: false,
                SingleResult: !returnsList,
                NoTracking: false,
                MethodName: returnsList ? "ToList" : nameof(Queryable.FirstOrDefault),
                Includes: _emptyIncludes,
                GroupJoinInfo: null,
                Tables: _simpleQueryTableCache.GetOrAdd(mapping.TableName, static t => new List<string>(1) { t }),
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = _executor.Materialize(plan, cmd);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw?.Elapsed ?? default, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // Preserve First vs FirstOrDefault semantics.
            if (list.Count > 0) return (TResult)list[0]!;
            if (requestedMethodName == nameof(Queryable.First))
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }

        /// <summary>
        /// THREAD STARVATION FIX: True synchronous execution path for complex queries.
        /// Uses synchronous ADO.NET methods instead of blocking on async code.
        /// </summary>
        private TResult ExecuteInternalSync<TResult>(Expression expression)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            IReadOnlyDictionary<string, object>? parameterDictionary = null;
            IReadOnlyDictionary<string, object> GetParameterDictionary()
            {
                parameterDictionary ??= EnsureParameterDictionary(plan, paramValues);
                return parameterDictionary;
            }

            Func<TResult> queryExecutorFactory = () =>
            {
                _ctx.EnsureConnection();
                using var cmd = _ctx.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                BindPlanParameters(cmd, plan, paramValues);

                object? result;
                if (plan.IsScalar)
                {
                    var scalarResult = cmd.ExecuteScalarWithInterception(_ctx);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull)
                    {
                        if (plan.MethodName is "Min" or "Max" or "Average" &&
                            typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                            throw new InvalidOperationException("Sequence contains no elements");
                        return default(TResult)!;
                    }
                    result = ConvertScalarResult<TResult>(scalarResult)!;
                }
                else
                {
                    var list = _executor.Materialize(plan, cmd);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw?.Elapsed ?? default, list.Count);
                    if (plan.SingleResult)
                    {
                        result = plan.MethodName switch
                        {
                            "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                            "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                            "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                            "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                            "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                            "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                            "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                            "LastOrDefault" => list.Count > 0 ? list[0] : null,
                            _ => list
                        };
                    }
                    else
                    {
                        if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                        {
                            var countList = nonGenericList.Count;
                            var covariantList = new List<object>(countList);
                            for (int i = 0; i < countList; i++)
                            {
                                covariantList.Add(nonGenericList[i]!);
                            }
                            result = covariantList;
                        }
                        else
                        {
                            result = list;
                        }
                    }
                }
                return (TResult)result!;
            };

            if (plan.IsCacheable && _ctx.Options.CacheProvider != null)
            {
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, GetParameterDictionary());
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return ExecuteWithCacheSync(cacheKey, plan.Tables, expiration, queryExecutorFactory);
            }
            else
            {
                return queryExecutorFactory();
            }
        }

        /// <summary>
        /// THREAD STARVATION FIX: Synchronous cache execution wrapper.
        /// </summary>
        private TResult ExecuteWithCacheSync<TResult>(string cacheKey, IReadOnlyCollection<string> tables, TimeSpan expiration, Func<TResult> factory)
        {
            var cache = _ctx.Options.CacheProvider;
            if (cache == null)
                return factory();

            if (cache.TryGet(cacheKey, out TResult? cached))
                return cached!;

            var semaphore = _cacheLocks.GetOrAdd(cacheKey, _ => new SemaphoreSlim(1, 1));
            semaphore.Wait();
            try
            {
                if (!cache.TryGet(cacheKey, out cached))
                {
                    cached = factory();
                    cache.Set(cacheKey, cached!, expiration, tables);
                }
                return cached!;
            }
            finally
            {
                semaphore.Release();
                // Cleanup is intentionally deferred to the periodic CleanupCacheLocks timer.
                // Checking CurrentCount immediately after Release has a race where another thread could
                // Wait() between Release() and the check, causing removal of a semaphore still in use.
            }
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
                e = ((UnaryExpression)e).Operand;
            return e;
        }
        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, IReadOnlyCollection<string> tables, TimeSpan expiration, Func<Task<TResult>> factory, CancellationToken ct)
        {
            var cache = _ctx.Options.CacheProvider;
            if (cache == null)
                return await factory().ConfigureAwait(false);
            if (cache.TryGet(cacheKey, out TResult? cached))
                return cached!;
            var semaphore = _cacheLocks.GetOrAdd(cacheKey, _ => new SemaphoreSlim(1, 1));
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (!cache.TryGet(cacheKey, out cached))
                {
                    cached = await factory().ConfigureAwait(false);
                    cache.Set(cacheKey, cached!, expiration, tables);
                }
                return cached!;
            }
            finally
            {
                semaphore.Release();
                // Cleanup is intentionally deferred to the periodic CleanupCacheLocks timer.
                // Checking CurrentCount immediately after Release has a race where another thread could
                // Wait() between Release() and the check, causing removal of a semaphore still in use.
            }
        }
        private string BuildCacheKeyFromPlan<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
        {
            var hasher = new XxHash128();
            AppendUtf8(hasher, plan.Sql.AsSpan());
            AppendByte(hasher, (byte)'|');
            AppendUtf8(hasher, typeof(TResult).FullName!.AsSpan());
            // Include a stable database identity in the result cache key so that two contexts
            // pointing to different databases with the same schema, query, and parameters
            // do not share a cache entry and return data from the wrong database.
            var dbIdentity = NormalizeConnectionStringForCacheKey(_ctx.Connection.ConnectionString);
            AppendUtf8(hasher, "|DB:".AsSpan());
            AppendUtf8(hasher, dbIdentity.AsSpan());
            var tenant = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            if (_ctx.Options.TenantProvider != null)
            {
                // Null tenant is allowed when the tenant column is nullable (ApplyGlobalFilters
                // emits IS NULL in that case). Use a distinct cache key segment so null-tenant
                // results are never confused with non-null-tenant results.
                AppendUtf8(hasher, "|TENANT:".AsSpan());
                AppendUtf8(hasher, (tenant?.ToString() ?? "<null>").AsSpan());
            }
            // Sort parameters deterministically so identical parameter sets produce the same hash
            // regardless of insertion order.
            foreach (var kvp in parameters.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                AppendByte(hasher, (byte)'|');
                AppendUtf8(hasher, kvp.Key.AsSpan());
                AppendByte(hasher, (byte)'=');
                if (kvp.Value is null)
                {
                    AppendUtf8(hasher, "null".AsSpan());
                    continue;
                }
                AppendUtf8(hasher, kvp.Value.GetType().FullName!.AsSpan());
                AppendByte(hasher, (byte)':');
                if (kvp.Value is byte[] bytesValue)
                {
                    hasher.Append(bytesValue);
                }
                else if (kvp.Value is IFormattable formattable)
                {
                    AppendUtf8(hasher, formattable.ToString(null, CultureInfo.InvariantCulture)!.AsSpan());
                }
                else
                {
                    AppendUtf8(hasher, kvp.Value.ToString()!.AsSpan());
                }
            }
            Span<byte> hash = stackalloc byte[16];
            hasher.GetCurrentHash(hash);
            return Convert.ToHexString(hash);
        }
        /// <summary>
        /// Normalizes a connection string for use as a cache key component.
        /// Splits on ';', trims each pair, sorts case-insensitively, and rejoins so that
        /// different orderings of the same connection string map to the same key.
        /// Sensitive keys (credentials) are stripped so they never appear in cache key material.
        /// </summary>
        private static readonly HashSet<string> _sensitiveConnectionStringKeys = new(StringComparer.OrdinalIgnoreCase)
        {
            "password", "pwd", "user password", "access token", "accesstoken", "token", "secret"
        };

        private static string NormalizeConnectionStringForCacheKey(string? cs)
        {
            if (string.IsNullOrEmpty(cs)) return string.Empty;
            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = cs };
                return string.Join(";",
                    builder.Keys.Cast<string>()
                        .Where(k => !_sensitiveConnectionStringKeys.Contains(k))
                        .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                        .Select(k => $"{k}={builder[k]}"));
            }
            catch (ArgumentException)
            {
                return Convert.ToHexString(
                    System.Security.Cryptography.SHA256.HashData(
                        System.Text.Encoding.UTF8.GetBytes(cs)));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendUtf8(XxHash128 hasher, ReadOnlySpan<char> value)
        {
            if (value.IsEmpty)
                return;
            var byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount <= StackAllocUtf8Threshold)
            {
                Span<byte> buffer = stackalloc byte[byteCount];
                Encoding.UTF8.GetBytes(value, buffer);
                hasher.Append(buffer);
            }
            else
            {
                var rented = ArrayPool<byte>.Shared.Rent(byteCount);
                try
                {
                    var bytesWritten = Encoding.UTF8.GetBytes(value, rented);
                    hasher.Append(new ReadOnlySpan<byte>(rented, 0, bytesWritten));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented, clearArray: true);
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendByte(XxHash128 hasher, byte value)
        {
            Span<byte> b = stackalloc byte[1];
            b[0] = value;
            hasher.Append(b);
        }
        /// <summary>
        /// Executes a DELETE statement represented by the provided LINQ expression. The method
        /// validates the generated plan, constructs the final SQL and executes it, returning the
        /// number of affected rows.
        /// </summary>
        /// <param name="expression">The LINQ expression describing the entities to delete.</param>
        /// <param name="ct">A token used to cancel the asynchronous operation.</param>
        /// <returns>The count of rows removed from the database.</returns>
        private async Task<int> ExecuteDeleteInternalAsync(Expression expression, CancellationToken ct)
        {
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteDeleteAsync only supports single table queries.");
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            _cudBuilder.ValidateCudPlan(plan.Sql);
            var whereClause = _cudBuilder.ExtractWhereClause(plan.Sql, mapping.EscTable);
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            var finalSql = $"DELETE FROM {mapping.EscTable}{whereClause}";
            cmd.CommandText = finalSql;
            BindPlanParameters(cmd, plan, paramValues);
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(finalSql, EnsureParameterDictionary(plan, paramValues), sw?.Elapsed ?? default, affected);
            return affected;
        }
        private async Task<int> ExecuteUpdateInternalAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteUpdateAsync only supports single table queries.");
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            _cudBuilder.ValidateCudPlan(plan.Sql);
            var whereClause = _cudBuilder.ExtractWhereClause(plan.Sql, mapping.EscTable);
            var (setClause, setParams) = _cudBuilder.BuildSetClause(mapping, set);
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            var finalSql = $"UPDATE {mapping.EscTable} SET {setClause}{whereClause}";
            cmd.CommandText = finalSql;
            BindPlanParameters(cmd, plan, paramValues);
            foreach (var p in setParams)
                cmd.AddOptimizedParam(p.Key, p.Value);
            // EnsureParameterDictionary returns a new Dictionary when compiled params exist,
            // or the plan's own Parameters dict when there are none. Only copy when needed.
            var baseDict = EnsureParameterDictionary(plan, paramValues);
            var allParams = baseDict is Dictionary<string, object> mutableDict && !ReferenceEquals(baseDict, plan.Parameters)
                ? mutableDict
                : new Dictionary<string, object>(baseDict);
            foreach (var p in setParams)
                allParams[p.Key] = p.Value;
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(finalSql, allParams, sw?.Elapsed ?? default, affected);
            return affected;
        }
        public async IAsyncEnumerable<T> AsAsyncEnumerable<T>(Expression expression, [EnumeratorCancellation] CancellationToken ct = default)
        {
            // Execute in true streaming mode so only one row is materialized at a time.
            var plan = GetPlan(expression, out _, out var paramValues);
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);
            if (plan.Includes.Count > 0 || plan.GroupJoinInfo != null)
                throw new NotSupportedException("AsAsyncEnumerable does not support Include or GroupJoin operations.");
            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null &&
                             _ctx.IsMapped(plan.ElementType);   // only mapped entity roots
            if (trackable)
                _ctx.GetMapping(plan.ElementType);
            var count = 0;
            await using var reader = await cmd
                .ExecuteReaderWithInterceptionAsync(
                    _ctx,
                    CommandBehavior.SingleResult,
                    ct)
                .ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = (T)await plan.Materializer(reader, ct).ConfigureAwait(false);
                if (trackable)
                {
                    var actualMap = _ctx.GetMapping(entity!.GetType());
                    var entry = _ctx.ChangeTracker.Track(entity!, EntityState.Unchanged, actualMap);
                    entity = (T)entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading((object)entity!, _ctx);
                }
                count++;
                yield return entity;
            }
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, EnsureParameterDictionary(plan, paramValues), sw?.Elapsed ?? default, count);
        }
        /// <summary>
        /// Returns the cached plan and binds parameters separately to avoid cloning the
        /// parameters dictionary on every cache hit.
        /// </summary>
        internal QueryPlan GetPlan(Expression expression, out Expression filtered, out IReadOnlyList<object?>? parameterValues)
        {
            filtered = ApplyGlobalFilters(expression);
            var elementType = GetElementType(UnwrapQueryExpression(filtered));
            var tenantHash = _ctx.Options.TenantProvider?.GetCurrentTenantId()?.GetHashCode() ?? 0;
            // Use cached mapping hash instead of recomputing on every query
            int mappingHash = _ctx.GetMappingHash();

            // Batch all 5 extends into a single hash operation (saves 4 XxHash128 calls)
            var fingerprint = ExpressionFingerprint
                .Compute(filtered)
                .Extend(tenantHash, elementType.GetHashCode(), filtered.Type.GetHashCode(),
                        _ctx.Provider.GetType().GetHashCode(), mappingHash);

            if (_planCache.TryGet(fingerprint, out var cached))
            {
                parameterValues = ExtractParameterValues(filtered, cached);
                return cached;
            }

            var localFiltered = filtered;
            var plan = _planCache.GetOrAdd(fingerprint, _ =>
            {
                using var translator = new QueryTranslator(_ctx);
                var before = GC.GetAllocatedBytesForCurrentThread();
                var p = translator.Translate(localFiltered);
                var after = GC.GetAllocatedBytesForCurrentThread();
                var size = after - before;
                Interlocked.Add(ref _totalPlanSize, size);
                Interlocked.Increment(ref _planSizeSamples);
                var clonedParams = new Dictionary<string, object>(p.Parameters);
                var clonedCompiledParams = new List<string>(p.CompiledParameters);
                return p with
                {
                    Fingerprint = fingerprint,
                    Parameters = clonedParams,
                    CompiledParameters = clonedCompiledParams
                };
            });

            parameterValues = ExtractParameterValues(filtered, plan);
            return plan;
        }

        /// <summary>
        /// Returns ONLY the extracted values, no Dictionary allocation.
        /// Reuses a thread-local extractor to avoid allocating a new visitor + List per query.
        /// </summary>
        [ThreadStatic] private static ParameterValueExtractor? t_extractor;
        private IReadOnlyList<object?>? ExtractParameterValues(Expression expression, QueryPlan plan)
        {
            if (plan.CompiledParameters.Count == 0)
                return null;

            var extractor = t_extractor ??= new ParameterValueExtractor();
            extractor.Reset();
            extractor.Visit(expression);

            // Copy values to a new list since the extractor will be reused
            return extractor.GetValuesCopy();
        }

        private IReadOnlyDictionary<string, object> EnsureParameterDictionary(QueryPlan plan, IReadOnlyList<object?>? parameterValues)
        {
            if (plan.CompiledParameters.Count == 0)
                return plan.Parameters;

            var parameters = new Dictionary<string, object>(plan.Parameters);
            if (parameterValues != null)
            {
                var count = Math.Min(plan.CompiledParameters.Count, parameterValues.Count);
                for (int i = 0; i < count; i++)
                {
                    parameters[plan.CompiledParameters[i]] = parameterValues[i] ?? DBNull.Value;
                }
            }
            else
            {
                for (int i = 0; i < plan.CompiledParameters.Count; i++)
                {
                    parameters[plan.CompiledParameters[i]] = DBNull.Value;
                }
            }

            return parameters;
        }
        private static Expression UnwrapQueryExpression(Expression expression)
        {
            return expression is MethodCallExpression mc &&
                   !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                   mc.Arguments.Count > 0
                ? mc.Arguments[0]
                : expression;
        }
        private Expression ApplyGlobalFilters(Expression expression)
        {
            // Skip the entire recursive walk when no global filters or tenant provider exist.
            // The recursion allocates new expression nodes (ToArray + Update) on every node even
            // when there are no filters to apply.
            if (_ctx.Options.GlobalFilters.Count == 0 && _ctx.Options.TenantProvider == null)
                return expression;

            if (expression is MethodCallExpression mc &&
                !typeof(IQueryable).IsAssignableFrom(expression.Type) &&
                mc.Arguments.Count > 0)
            {
                var filteredSource = ApplyGlobalFilters(mc.Arguments[0]);
                var args = mc.Arguments.ToArray();
                args[0] = filteredSource;
                return mc.Update(mc.Object, args);
            }
            var entityType = GetElementType(expression);
            if (_ctx.Options.GlobalFilters.Count > 0)
            {
                foreach (var kvp in _ctx.Options.GlobalFilters)
                {
                    if (!kvp.Key.IsAssignableFrom(entityType)) continue;
                    foreach (var filter in kvp.Value)
                    {
                        LambdaExpression lambda;
                        if (filter.Parameters.Count == 2)
                        {
                            var replacer = new ParameterReplacer(filter.Parameters[0], Expression.Constant(_ctx));
                            var body = replacer.Visit(filter.Body)!;
                            lambda = Expression.Lambda(body, filter.Parameters[1]);
                        }
                        else
                        {
                            lambda = filter;
                        }
                        expression = Expression.Call(
                            typeof(Queryable),
                            nameof(Queryable.Where),
                            new[] { entityType },
                            expression,
                            Expression.Quote(lambda));
                    }
                }
            }
            if (_ctx.Options.TenantProvider != null)
            {
                var map = _ctx.GetMapping(entityType);
                var tenantCol = map.TenantColumn;
                if (tenantCol != null)
                {
                    var param = Expression.Parameter(entityType, "t");
                    var prop = Expression.Property(param, tenantCol.Prop.Name);
                    var tenantId = _ctx.Options.TenantProvider.GetCurrentTenantId();
                    // Coerce the tenant ID to the mapped property type before building the expression
                    // constant. If TenantProvider.GetCurrentTenantId() returns a boxed long but the
                    // entity property is int (or any other cross-type mismatch), Expression.Constant
                    // would throw ArgumentException before translation. Convert.ChangeType handles
                    // common numeric widening/narrowing and string representations. If conversion is
                    // genuinely impossible (e.g., string "abc" → int), throw a deterministic
                    // NormConfigurationException with an actionable message.
                    var propType = tenantCol.Prop.PropertyType;
                    object coercedTenantId;
                    if (tenantId == null)
                    {
                        var isNullable = !propType.IsValueType || Nullable.GetUnderlyingType(propType) != null;
                        if (!isNullable)
                            throw new nORM.Core.NormConfigurationException(
                                $"TenantProvider.GetCurrentTenantId() returned null but the tenant column " +
                                $"'{tenantCol.PropName}' on entity '{entityType.Name}' has non-nullable type " +
                                $"'{propType.FullName}'. Return a valid tenant ID or use a nullable column type.");
                        coercedTenantId = null!;  // safe: propType is nullable reference or Nullable<T>
                    }
                    else if (propType.IsInstanceOfType(tenantId))
                    {
                        coercedTenantId = tenantId;
                    }
                    else
                    {
                        var underlyingType = Nullable.GetUnderlyingType(propType) ?? propType;
                        try
                        {
                            coercedTenantId = Convert.ChangeType(tenantId, underlyingType,
                                System.Globalization.CultureInfo.InvariantCulture);
                        }
                        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
                        {
                            throw new nORM.Core.NormConfigurationException(
                                $"TenantProvider.GetCurrentTenantId() returned a value of type " +
                                $"'{tenantId.GetType().FullName}' which cannot be converted to the " +
                                $"tenant column property type '{propType.FullName}' " +
                                $"(column: '{tenantCol.PropName}', entity: '{entityType.Name}'). " +
                                $"Ensure TenantProvider returns a compatible type or update the entity mapping.", ex);
                        }
                    }
                    var constant = Expression.Constant(coercedTenantId, propType);
                    var body = Expression.Equal(prop, constant);
                    var lambda = Expression.Lambda(body, param);
                    expression = Expression.Call(
                        typeof(Queryable),
                        nameof(Queryable.Where),
                        new[] { entityType },
                        expression,
                        Expression.Quote(lambda));
                }
            }
            return expression;
        }
        /// <summary>
        /// Cached version of GetElementType to avoid repeated reflection.
        /// GetInterfaces() is expensive and this is called frequently in hot paths.
        /// </summary>
        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;

            // Fast path for generic types with arguments
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }

            // Cached reflection path
            return _elementTypeCache.GetOrAdd(type, static t =>
            {
                var iface = t.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
                if (iface != null) return iface.GetGenericArguments()[0];
                throw new ArgumentException($"Cannot determine element type from expression of type {t}");
            });
        }
        private sealed class ParameterValueExtractor : ExpressionVisitor
        {
            private readonly List<object?> _values = new();

            public void Reset() => _values.Clear();

            /// <summary>
            /// Returns a copy of extracted values (caller owns the array).
            /// Uses array for less overhead than List when count is small.
            /// </summary>
            public object?[] GetValuesCopy() => _values.ToArray();

            protected override Expression VisitConstant(ConstantExpression node)
            {
                // Skip all constants - they are either IQueryable roots or values already baked
                // into the plan's Parameters dictionary by AppendConstant during translation.
                return node;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                // Closure-captured variables (member access whose root resolves to a constant)
                // are now emitted as compiled parameters by ExpressionToSqlVisitor.VisitMember.
                // Extract the live value here so it can be bound at execution time.
                if (QueryTranslator.TryGetConstantValue(node, out var value))
                {
                    _values.Add(value ?? DBNull.Value);
                    return node; // early return: do NOT visit children (avoids double-counting)
                }
                return base.VisitMember(node);
            }
        }
    }
}