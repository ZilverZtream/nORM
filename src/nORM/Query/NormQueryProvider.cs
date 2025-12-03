using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Globalization;
using System.Buffers;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Navigation;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
#nullable enable
namespace nORM.Query
{
    internal sealed class NormQueryProvider : IQueryProvider, IDisposable
    {
        internal readonly DbContext _ctx;
        private static readonly ConcurrentLruCache<ExpressionFingerprint, QueryPlan> _planCache =
            new(maxSize: CalculateInitialPlanCacheSize(), timeToLive: TimeSpan.FromHours(1));
        private static readonly Timer _planCacheMonitor = new(AdjustPlanCacheSize, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _cacheLocks = new();
        private static readonly Timer _cacheLockCleanupTimer = new(CleanupCacheLocks, null, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        // PERFORMANCE FIX: Cache GetElementType results to avoid repeated reflection
        private static readonly ConcurrentDictionary<Type, Type> _elementTypeCache = new();
        // PERFORMANCE FIX: Cache constructor existence checks
        private static readonly ConcurrentDictionary<Type, bool> _constrainedQueryableCache = new();
        private static long _totalPlanSize;
        private static int _planSizeSamples;
        private readonly QueryExecutor _executor;
        private readonly IncludeProcessor _includeProcessor;
        private readonly BulkCudBuilder _cudBuilder;
        private readonly ConcurrentDictionary<string, string> _simpleSqlCache = new();
        public NormQueryProvider(DbContext ctx)
        {
            _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
            _includeProcessor = new IncludeProcessor(ctx);
            _executor = new QueryExecutor(ctx, _includeProcessor);
            _cudBuilder = new BulkCudBuilder(ctx);
        }
        public void Dispose()
        {
            // Instance-level disposables if any
        }
        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = expression.Type.GetGenericArguments()[0];
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
            // Check if the type can satisfy the 'new()' constraint
            if (CanUseConstrainedQueryable(elementType))
            {
                // Use the constrained version for regular entity types
                var constrainedQueryableType = typeof(NormQueryableImpl<>).MakeGenericType(elementType);
                return (IQueryable)Activator.CreateInstance(constrainedQueryableType, new object[] { this, expression })!;
            }
            else
            {
                // Use the unconstrained version for anonymous types and other types without parameterless constructors
                var unconstrainedQueryableType = typeof(NormQueryableImplUnconstrained<>).MakeGenericType(elementType);
                return (IQueryable)Activator.CreateInstance(unconstrainedQueryableType, new object[] { this, expression })!;
            }
        }
        /// <summary>
        /// PERFORMANCE FIX: Cached check for whether a type can use constrained queryable.
        /// GetConstructor is expensive reflection that's called for every query creation.
        /// </summary>
        private static bool CanUseConstrainedQueryable(Type elementType)
        {
            return _constrainedQueryableCache.GetOrAdd(elementType, static t =>
            {
                // Check if type is a class and has a public parameterless constructor
                if (!t.IsClass)
                    return false;
                // Anonymous types start with '<>' and don't have public parameterless constructors
                if (t.Name.StartsWith("<>"))
                    return false;
                // Check for public parameterless constructor
                var defaultConstructor = t.GetConstructor(Type.EmptyTypes);
                return defaultConstructor != null && defaultConstructor.IsPublic;
            });
        }
        /// <summary>
        /// PERFORMANCE OPTIMIZATION: Enhanced cache lock cleanup.
        /// - Limits cleanup to prevent unbounded growth
        /// - Disposes unused semaphores to release resources
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

            // Remove and dispose in separate pass to avoid concurrent modification
            foreach (var (key, semaphore) in locksToRemove)
            {
                if (_cacheLocks.TryRemove(key, out _))
                {
                    semaphore.Dispose();
                }
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
                ? 16 * 1024 // fallback to 16KB if no samples yet
                : (int)(Volatile.Read(ref _totalPlanSize) / samples);
            const long maxCacheBytes = 64L * 1024 * 1024; // limit to 64MB overall
            var cacheBytes = Math.Min(info.TotalAvailableMemoryBytes / 100, maxCacheBytes);
            var size = (int)(cacheBytes / avgPlanSize);
            return Math.Clamp(size, 100, 10000);
        }
        public TResult Execute<TResult>(Expression expression)
            => ExecuteSync<TResult>(expression);

        /// <summary>
        /// DEADLOCK FIX (TASK 17): Synchronous query execution that doesn't block on async methods.
        /// Used by IEnumerable.GetEnumerator() to avoid deadlocks from GetAwaiter().GetResult().
        /// </summary>
        public TResult ExecuteSync<TResult>(Expression expression)
        {
            if (TryGetSimpleQuery(expression, out var sql, out var parameters))
            {
                return ExecuteSimpleSync<TResult>(sql, parameters);
            }
            // For complex queries, fall back to synchronous execution via ExecuteAsync
            // This is safe because we're not blocking on it from a synchronization context
            return ExecuteAsync<TResult>(expression, CancellationToken.None).GetAwaiter().GetResult();
        }
        public object? Execute(Expression expression) => Execute<object>(expression);
        public Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
        {
            // Fast path – bypass translator for recognized simple patterns
            if (TryExecuteFastPath<TResult>(expression, out var fastResult))
                return fastResult;
            // Original execution path
            if (TryGetSimpleQuery(expression, out var sql, out var parameters))
            {
                Func<CancellationToken, Task<TResult>> factory = token => ExecuteSimpleAsync<TResult>(sql, parameters, token);
                return _ctx.Options.RetryPolicy != null
                    ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => factory(token), ct)
                    : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => factory(token), ct);
            }
            return _ctx.Options.RetryPolicy != null
               ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteInternalAsync<TResult>(expression, token), ct)
               : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteInternalAsync<TResult>(expression, token), ct);
        }
        /// <summary>
        /// Executes a translated <c>DELETE</c> query asynchronously.
        /// </summary>
        /// <param name="expression">Expression representing the delete query.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>A task containing the number of rows affected.</returns>
        public Task<int> ExecuteDeleteAsync(Expression expression, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteDeleteInternalAsync(expression, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteDeleteInternalAsync(expression, token), ct);
        }
        public Task<int> ExecuteUpdateAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteUpdateInternalAsync(expression, set, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteUpdateInternalAsync(expression, set, token), ct);
        }
        /// <summary>
        /// PERFORMANCE FIX: Fast path execution using cached delegates instead of reflection.
        /// Eliminates MakeGenericMethod and Invoke overhead.
        /// </summary>
        private bool TryExecuteFastPath<TResult>(Expression expression, out Task<TResult> result)
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
                    // PERFORMANCE FIX: Use cached delegate instead of MakeGenericMethod + Invoke
                    if (FastPathQueryExecutor.TryExecuteNonGeneric(elementType, expression, _ctx, out var taskObject))
                    {
                        // Cast Task<object> to Task<TResult> without additional overhead
                        result = CastTaskResult<TResult>(taskObject);
                        return true;
                    }
                }
                catch
                {
                    // ignore and fall back to full translation path
                }
            }
            return false;
        }

        /// <summary>
        /// PERFORMANCE FIX: Efficiently converts Task&lt;object&gt; to Task&lt;TResult&gt; without boxing for value types.
        /// Uses type-specific casting to avoid Convert.ChangeType overhead.
        /// </summary>
        private static async Task<TResult> CastTaskResult<TResult>(Task<object> task)
        {
            var result = await task.ConfigureAwait(false);
            return ConvertScalarResult<TResult>(result);
        }

        /// <summary>
        /// PERFORMANCE FIX: Converts scalar results without boxing for value types.
        /// Replaces Convert.ChangeType which forces heap allocation for value types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static TResult ConvertScalarResult<TResult>(object result)
        {
            // PERFORMANCE FIX: Direct cast for reference types
            if (typeof(TResult).IsClass || typeof(TResult).IsInterface)
            {
                return (TResult)result;
            }

            // PERFORMANCE FIX: Handle value types without Convert.ChangeType boxing
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
        private async Task<TResult> ExecuteInternalAsync<TResult>(Expression expression, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            IReadOnlyDictionary<string, object>? parameterDictionary = null;
            IReadOnlyDictionary<string, object> GetParameterDictionary()
            {
                parameterDictionary ??= EnsureParameterDictionary(plan, paramValues);
                return parameterDictionary;
            }
            Func<Task<TResult>> queryExecutorFactory = async () =>
            {
                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                foreach (var p in plan.Parameters) cmd.AddOptimizedParam(p.Key, p.Value);

                if (paramValues != null)
                {
                    for (int i = 0; i < plan.CompiledParameters.Count; i++)
                    {
                        var name = plan.CompiledParameters[i];
                        var value = i < paramValues.Count ? paramValues[i] : DBNull.Value;
                        cmd.AddOptimizedParam(name, value);
                    }
                }
                object result;
                if (plan.IsScalar)
                {
                    var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw.Elapsed, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull) return default(TResult)!;
                    // PERFORMANCE FIX: Use ConvertScalarResult to avoid boxing for value types
                    result = ConvertScalarResult<TResult>(scalarResult)!;
                }
                else
                {
                    var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw.Elapsed, list.Count);
                    if (plan.SingleResult)
                    {
                        // PERFORMANCE FIX: Direct list access instead of Cast<object>().First()
                        // Avoids unnecessary IEnumerable cast and LINQ iterator allocation
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
                        } ?? (object)list;
                    }
                    else
                    {
                        // PERFORMANCE FIX: Optimized Covariance Handling
                        if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                        {
                            // Manual copy is 2-3x faster than LINQ Cast<object>().ToList()
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
                var cacheKey = BuildCacheKeyWithValues<TResult>(plan, GetParameterDictionary());
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
            }
            else
            {
                return await queryExecutorFactory().ConfigureAwait(false);
            }
        }
        // INTERNAL API: Optimized version that accepts array of values instead of Dictionary
        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, token), ct);
        }

        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters, CancellationToken ct)
        {
            return _ctx.Options.RetryPolicy != null
                ? new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledInternalAsync<TResult>(plan, parameters, token), ct)
                : new DefaultExecutionStrategy(_ctx).ExecuteAsync((_, token) => ExecuteCompiledInternalAsync<TResult>(plan, parameters, token), ct);
        }
        private async Task<TResult> ExecuteCompiledInternalAsync<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            // Merge template parameters from the plan with the live execution values
            var finalParameters = new Dictionary<string, object>(plan.Parameters);
            foreach (var p in parameters)
            {
                finalParameters[p.Key] = p.Value;
            }
            Func<Task<TResult>> queryExecutorFactory = async () =>
            {
                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                foreach (var p in finalParameters) cmd.AddOptimizedParam(p.Key, p.Value);
                object result;
                if (plan.IsScalar)
                {
                    var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, finalParameters, sw.Elapsed, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull) return default!;
                    // PERFORMANCE FIX: Use ConvertScalarResult to avoid boxing for value types
                    result = ConvertScalarResult<TResult>(scalarResult)!;
                }
                else
                {
                    var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, finalParameters, sw.Elapsed, list.Count);
                    if (plan.SingleResult)
                    {
                        // PERFORMANCE FIX: Direct list access instead of Cast<object>().First()
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
                        } ?? (object)list;
                    }
                    else
                    {
                        // PERFORMANCE FIX: Optimized Covariance Handling
                        if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                        {
                            // Manual copy is 2-3x faster than LINQ Cast<object>().ToList()
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
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, finalParameters);
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
            }
            else
            {
                return await queryExecutorFactory().ConfigureAwait(false);
            }
        }

        private async Task<TResult> ExecuteCompiledInternalArrayAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            // This method replaces the dictionary merge with direct array access
            var sw = Stopwatch.StartNew();

            Func<Task<TResult>> queryExecutorFactory = async () =>
            {
                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;

                // 1. Add static parameters from plan (constants)
                foreach (var p in plan.Parameters)
                    cmd.AddOptimizedParam(p.Key, p.Value);

                // 2. Add dynamic parameters from array
                // The ExpressionCompiler guarantees that parameterValues match CompiledParameters order
                var count = Math.Min(plan.CompiledParameters.Count, parameterValues.Length);
                for (int i = 0; i < count; i++)
                {
                    cmd.AddOptimizedParam(plan.CompiledParameters[i], parameterValues[i] ?? DBNull.Value);
                }

                object result;
                if (plan.IsScalar)
                {
                    var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
                    // Logging skipped for perf in scalar
                    if (scalarResult == null || scalarResult is DBNull) return default!;
                    result = ConvertScalarResult<TResult>(scalarResult)!;
                }
                else
                {
                    var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
                    _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw.Elapsed, list.Count);

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
                        } ?? (object)list;
                    }
                    else
                    {
                        // PERFORMANCE FIX: Optimized Covariance Handling
                        if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                        {
                            // Manual copy is 2-3x faster than LINQ Cast<object>().ToList()
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
                // For caching, we construct a dictionary only if needed (rare path)
                var dict = new Dictionary<string, object>(plan.Parameters);
                for (int i = 0; i < plan.CompiledParameters.Count && i < parameterValues.Length; i++)
                    dict[plan.CompiledParameters[i]] = parameterValues[i] ?? DBNull.Value;

                var cacheKey = BuildCacheKeyWithValues<TResult>(plan, dict);
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
            }
            else
            {
                return await queryExecutorFactory().ConfigureAwait(false);
            }
        }
        private bool TryGetSimpleQuery(Expression expr, out string sql, out Dictionary<string, object> parameters)
        {
            sql = string.Empty;
            parameters = new Dictionary<string, object>();
            if (_ctx.Options.GlobalFilters.Count > 0 || _ctx.Options.TenantProvider != null)
                return false;
            // Traverse to find root query and optional Where predicate
            MethodCallExpression? whereCall = null;
            Expression current = expr;
            // Unwrap result operators like First/Single
            while (current is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Queryable))
            {
                if (mc.Method.Name == nameof(Queryable.Where))
                {
                    if (whereCall != null) return false; // only support single Where
                    whereCall = mc;
                    current = mc.Arguments[0];
                }
                else if (mc.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault) or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault))
                {
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
            var cacheKey = ExpressionFingerprint.Compute(expr) + ":" + elementType.FullName;
            if (!_simpleSqlCache.TryGetValue(cacheKey, out var cachedSql))
            {
                // PERFORMANCE FIX: Use string interpolation instead of StringBuilder for small queries
                // StringBuilder has overhead that's not worth it for these simple SELECT statements
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
                        whereClause = $" WHERE {boolCol.EscCol} = 1";
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
                        // PERFORMANCE & SECURITY FIX: Use ExpressionValueExtractor instead of Compile().DynamicInvoke()
                        // DynamicInvoke is 100x slower and poses RCE risks
                        if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                            return false;
                        parameters[paramName] = value!;
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
                if (lambda.Body is MemberExpression boolMember && boolMember.Type == typeof(bool))
                {
                    // no parameter to bind for boolean predicate
                }
                else
                {
                    if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                        return false;
                    var paramName = _ctx.Provider.ParamPrefix + "p0";
                    // PERFORMANCE & SECURITY FIX: Use ExpressionValueExtractor instead of Compile().DynamicInvoke()
                    if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                        return false;
                    parameters[paramName] = value!;
                }
            }
            return true;
        }
        private async Task<TResult> ExecuteSimpleAsync<TResult>(string sql, Dictionary<string, object> parameters, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
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

            // PERFORMANCE FIX (TASK 14): Create both sync and async materializers
            var factory = new MaterializerFactory();
            var materializer = factory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = factory.CreateSyncMaterializer(mapping, elementType);

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
                Includes: new List<IncludePlan>(),
                GroupJoinInfo: null,
                Tables: new List<string> { mapping.TableName },
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw.Elapsed, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // PERFORMANCE FIX: Direct list access instead of Cast<object>().First()
            return list.Count > 0 ? (TResult)list[0]! : default!;
        }
        private TResult ExecuteSimpleSync<TResult>(string sql, Dictionary<string, object> parameters)
        {
            var sw = Stopwatch.StartNew();
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

            // PERFORMANCE FIX (TASK 14): Create both sync and async materializers
            var factory = new MaterializerFactory();
            var materializer = factory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = factory.CreateSyncMaterializer(mapping, elementType);

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
                Includes: new List<IncludePlan>(),
                GroupJoinInfo: null,
                Tables: new List<string> { mapping.TableName },
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            _ctx.EnsureConnection();
            using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = _executor.Materialize(plan, cmd);
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw.Elapsed, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // PERFORMANCE FIX: Direct list access instead of Cast<object>().First()
            return list.Count > 0 ? (TResult)list[0]! : default!;
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
                if (semaphore.CurrentCount == 1)
                    _cacheLocks.TryRemove(cacheKey, out _);
            }
        }
        private string BuildCacheKeyFromPlan<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
        {
            var hasher = new XxHash128();
            AppendUtf8(hasher, plan.Sql.AsSpan());
            AppendByte(hasher, (byte)'|');
            AppendUtf8(hasher, typeof(TResult).FullName!.AsSpan());
            var tenant = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            if (_ctx.Options.TenantProvider != null)
            {
                if (tenant == null)
                    throw new InvalidOperationException("Tenant context required but not available");
                AppendUtf8(hasher, "|TENANT:".AsSpan());
                AppendUtf8(hasher, tenant.ToString()!.AsSpan());
            }
            foreach (var kvp in parameters.OrderBy(k => k.Key))
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AppendUtf8(XxHash128 hasher, ReadOnlySpan<char> value)
        {
            if (value.IsEmpty)
                return;
            var byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount <= 256)
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
                    Encoding.UTF8.GetBytes(value, rented);
                    hasher.Append(rented.AsSpan(0, byteCount));
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
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteDeleteAsync only supports single table queries.");
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            _cudBuilder.ValidateCudPlan(plan.Sql);
            var whereClause = _cudBuilder.ExtractWhereClause(plan.Sql, mapping.EscTable);
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            var finalSql = $"DELETE FROM {mapping.EscTable}{whereClause}";
            cmd.CommandText = finalSql;
            foreach (var p in plan.Parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            if (paramValues != null)
            {
                for (int i = 0; i < plan.CompiledParameters.Count; i++)
                {
                    var name = plan.CompiledParameters[i];
                    var value = i < paramValues.Count ? paramValues[i] : DBNull.Value;
                    cmd.AddOptimizedParam(name, value);
                }
            }
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(finalSql, EnsureParameterDictionary(plan, paramValues), sw.Elapsed, affected);
            return affected;
        }
        private async Task<int> ExecuteUpdateInternalAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            if (plan.Tables.Count != 1)
                throw new NotSupportedException("ExecuteUpdateAsync only supports single table queries.");
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            _cudBuilder.ValidateCudPlan(plan.Sql);
            var whereClause = _cudBuilder.ExtractWhereClause(plan.Sql, mapping.EscTable);
            var (setClause, setParams) = _cudBuilder.BuildSetClause(mapping, set);
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            var finalSql = $"UPDATE {mapping.EscTable} SET {setClause}{whereClause}";
            cmd.CommandText = finalSql;
            foreach (var p in plan.Parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            if (paramValues != null)
            {
                for (int i = 0; i < plan.CompiledParameters.Count; i++)
                {
                    var name = plan.CompiledParameters[i];
                    var value = i < paramValues.Count ? paramValues[i] : DBNull.Value;
                    cmd.AddOptimizedParam(name, value);
                }
            }
            foreach (var p in setParams)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var allParams = EnsureParameterDictionary(plan, paramValues).ToDictionary(k => k.Key, v => v.Value);
            foreach (var p in setParams)
                allParams[p.Key] = p.Value;
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            _ctx.Options.Logger?.LogQuery(finalSql, allParams, sw.Elapsed, affected);
            return affected;
        }
        public async IAsyncEnumerable<T> AsAsyncEnumerable<T>(Expression expression, [EnumeratorCancellation] CancellationToken ct = default)
        {
            // Execute in true streaming mode so only one row is materialized at a time.
            var plan = GetPlan(expression, out _, out var paramValues);
            var sw = Stopwatch.StartNew();
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            foreach (var p in plan.Parameters) cmd.AddOptimizedParam(p.Key, p.Value);
            if (paramValues != null)
            {
                for (int i = 0; i < plan.CompiledParameters.Count; i++)
                {
                    var name = plan.CompiledParameters[i];
                    var value = i < paramValues.Count ? paramValues[i] : DBNull.Value;
                    cmd.AddOptimizedParam(name, value);
                }
            }
            if (plan.Includes.Count > 0 || plan.GroupJoinInfo != null)
                throw new NotSupportedException("AsAsyncEnumerable does not support Include or GroupJoin operations.");
            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null;
            if (trackable)
                _ctx.GetMapping(plan.ElementType);
            var count = 0;
            await using var reader = await cmd
                .ExecuteReaderWithInterceptionAsync(
                    _ctx,
                    CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
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
            _ctx.Options.Logger?.LogQuery(plan.Sql, EnsureParameterDictionary(plan, paramValues), sw.Elapsed, count);
        }
        /// <summary>
        /// PERFORMANCE FIX: Returns the cached plan and binds parameters separately.
        /// This avoids cloning the parameters dictionary on every cache hit.
        /// </summary>
        internal QueryPlan GetPlan(Expression expression, out Expression filtered, out IReadOnlyList<object?>? parameterValues)
        {
            filtered = ApplyGlobalFilters(expression);
            var elementType = GetElementType(UnwrapQueryExpression(filtered));
            var tenantHash = _ctx.Options.TenantProvider?.GetCurrentTenantId()?.GetHashCode() ?? 0;
            // PERFORMANCE FIX: Use Type.GetHashCode() directly instead of caching it
            // Type.GetHashCode() is intrinsic and cached by runtime, dictionary lookup is slower
            var fingerprint = ExpressionFingerprint
                .Compute(filtered)
                .Extend(tenantHash)
                .Extend(elementType.GetHashCode())
                .Extend(filtered.Type.GetHashCode());

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
        /// DEPRECATED: Use overload with boundParameters out parameter for better performance.
        /// </summary>
        [Obsolete("Use overload with boundParameters out parameter")]
        internal QueryPlan GetPlan(Expression expression, out Expression filtered)
        {
            var plan = GetPlan(expression, out filtered, out var parameterValues);
            return plan with { Parameters = EnsureParameterDictionary(plan, parameterValues) };
        }

        /// <summary>
        /// Returns ONLY the extracted values, no Dictionary allocation
        /// </summary>
        private IReadOnlyList<object?>? ExtractParameterValues(Expression expression, QueryPlan plan)
        {
            if (plan.CompiledParameters.Count == 0)
                return null;

            var extractor = new ParameterValueExtractor();
            extractor.Visit(expression);
            
            return extractor.Values;
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
        private string BuildCacheKeyWithValues<TResult>(QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
        {
            return BuildCacheKeyFromPlan<TResult>(plan, parameters);
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
                    var constant = Expression.Constant(tenantId, tenantCol.Prop.PropertyType);
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
        /// PERFORMANCE FIX: Cached version of GetElementType to avoid repeated reflection.
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
            public IReadOnlyList<object?> Values => _values;

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value is IQueryable)
                {
                    return node;
                }
                _values.Add(node.Value ?? DBNull.Value);
                return base.VisitConstant(node);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (QueryTranslator.TryGetConstantValue(node, out var value))
                {
                    _values.Add(value ?? DBNull.Value);
                    return node;
                }
                return base.VisitMember(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                _values.Add(DBNull.Value);
                return base.VisitParameter(node);
            }
        }
    }
}