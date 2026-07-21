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
    internal sealed partial class NormQueryProvider
    {        public object? Execute(Expression expression) => Execute<object>(expression);
        public Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
        {
            if (TryGetCountQuery(expression, out var countSql, out var countParameters))
            {
                if (_ctx.Options.RetryPolicy != null)
                    return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCountAsync<TResult>(countSql, countParameters, token), ct);
                return ExecuteCountAsync<TResult>(countSql, countParameters, ct);
            }

            // Fast path - bypass translator for recognized simple patterns
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
                try
                {
                    if (FastPathQueryExecutor.TryExecuteListNonGeneric(elementType, expression, _ctx, ct, out var typedListTask))
                    {
                        result = (Task<TResult>)typedListTask;
                        return true;
                    }
                }
                catch (NotSupportedException)
                {
                    // ignore and fall back to full translation path
                }
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
            else if (resultType.IsClass
                     && expression is MethodCallExpression firstCall
                     && firstCall.Method.DeclaringType == typeof(Queryable)
                     && (firstCall.Method.Name == nameof(Queryable.First) || firstCall.Method.Name == nameof(Queryable.FirstOrDefault))
                     && firstCall.Arguments.Count == 1)
            {
                // Scalar First/FirstOrDefault of an entity — route to the fast path (WHERE ... LIMIT 1) instead
                // of the higher-overhead simple-query path. The result type equals the element type here.
                try
                {
                    if (FastPathQueryExecutor.TryExecuteFirstNonGeneric(resultType, expression, _ctx,
                            throwOnEmpty: firstCall.Method.Name == nameof(Queryable.First), ct, out var firstTask))
                    {
                        result = CastTaskResult<TResult>(firstTask);
                        return true;
                    }
                }
                catch (NotSupportedException)
                {
                    // ignore and fall back to the simple-query / full translation path
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

            // Fast path for common scalar types - avoids boxing. Pass
            // InvariantCulture explicitly so non-English locales (Swedish,
            // German, etc.) don't parse '1.5' under comma-decimal rules and
            // throw FormatException. The database stores numeric scalars
            // using invariant formatting, so the read must use it too.
            var ic = System.Globalization.CultureInfo.InvariantCulture;
            if (underlyingType == typeof(int))
                return (TResult)(object)Convert.ToInt32(result, ic);
            if (underlyingType == typeof(long))
                return (TResult)(object)Convert.ToInt64(result, ic);
            if (underlyingType == typeof(double))
                return (TResult)(object)Convert.ToDouble(result, ic);
            if (underlyingType == typeof(decimal))
                return (TResult)(object)Convert.ToDecimal(result, ic);
            if (underlyingType == typeof(bool))
                return (TResult)(object)Convert.ToBoolean(result, ic);
            if (underlyingType == typeof(short))
                return (TResult)(object)Convert.ToInt16(result, ic);
            if (underlyingType == typeof(byte))
                return (TResult)(object)Convert.ToByte(result, ic);
            if (underlyingType == typeof(float))
                return (TResult)(object)Convert.ToSingle(result, ic);
            if (underlyingType == typeof(DateTime))
                return (TResult)(object)Convert.ToDateTime(result, ic);
            if (underlyingType == typeof(Guid))
                return (TResult)(object)(Guid)result;

            // Fallback for other types (still better than ChangeType for common cases above)
            return (TResult)Convert.ChangeType(result, underlyingType, ic)!;
        }
        private Task<TResult> ExecuteInternalAsync<TResult>(Expression expression, CancellationToken ct)
        {
            // Only allocate Stopwatch when logger is active
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            // For cached queries, use the closure-based path (rare).
            // For non-cached queries (common), return task directly - no async state machine needed.
            if (ResultCacheUsable(plan))
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
            return await ExecuteWithCacheAsync(cacheKey, plan.CacheTables ?? plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
        }
        /// <summary>
        /// Non-async entry point - does synchronous command setup when connection is ready,
        /// then dispatches to the appropriate async materializer. Avoids one async state machine
        /// allocation on the hot path.
        /// </summary>
        private Task<TResult> ExecuteQueryFromPlanAsync<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            // Check if connection is ready without awaiting
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteQueryFromPlanSlowAsync<TResult>(ensureTask, plan, paramValues, sw, ct);

            if (CanUsePooledPlanCommand<TResult>(plan, paramValues))
                return ExecutePooledQueryPlanSync<TResult>(plan, paramValues, sw, ct);

            // Synchronous command setup - no async state machine needed
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);

            // A1: pre-cancelled tokens are silently ignored on the warm sync path unless checked here.
            // EnsureConnectionAsync(ct) above returns immediately for warmed connections without
            // inspecting the token; the sync dispatchers below never check it either.
            ct.ThrowIfCancellationRequested();

            // Dispatch directly to materializer - avoids wrapping in another async method
            if (plan.IsScalar)
            {
                // Sync scalar for providers without true async I/O (SQLite)
                if (_ctx.RawProvider.PrefersSyncExecution || _ctx.RawProvider.PrefersSyncQueryPlanExecution)
                    return ExecuteScalarPlanSync<TResult>(plan, cmd, sw);
                return ExecuteScalarPlanAsync<TResult>(plan, cmd, sw, ct);
            }

            // For providers that don't support true async I/O (SQLite), use fully synchronous
            // materialization to eliminate per-row ReadAsync state machine overhead (~50ns x N rows).
            if (_ctx.RawProvider.PrefersSyncExecution || _ctx.RawProvider.PrefersSyncQueryPlanExecution)
                return ExecuteListPlanSyncWrapped<TResult>(plan, cmd, sw);

            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (Task<TResult>)(object)ExecuteObjectListPlanAsync(plan, cmd, sw, ct);

            return ExecuteListPlanAsync<TResult>(plan, cmd, sw, ct);
        }

        /// <summary>PERF: Slow path - connection needs initialization.</summary>
        private async Task<TResult> ExecuteQueryFromPlanSlowAsync<TResult>(Task<DbConnection> ensureTask, QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            await ensureTask.ConfigureAwait(false);
            if (CanUsePooledPlanCommand<TResult>(plan, paramValues))
                return await ExecutePooledQueryPlanSync<TResult>(plan, paramValues, sw, ct).ConfigureAwait(false);

            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, paramValues);

            if (plan.IsScalar)
            {
                if (_ctx.RawProvider.PrefersSyncExecution || _ctx.RawProvider.PrefersSyncQueryPlanExecution)
                    return await ExecuteScalarPlanSync<TResult>(plan, cmd, sw).ConfigureAwait(false);
                return await ExecuteScalarPlanAsync<TResult>(plan, cmd, sw, ct).ConfigureAwait(false);
            }

            if (typeof(TResult) == typeof(List<object>) && plan.ElementType != typeof(object) && !plan.SingleResult)
                return (TResult)(object)await ExecuteObjectListPlanAsync(plan, cmd, sw, ct).ConfigureAwait(false);

            if (_ctx.RawProvider.PrefersSyncExecution || _ctx.RawProvider.PrefersSyncQueryPlanExecution)
                return await ExecuteListPlanSyncWrapped<TResult>(plan, cmd, sw).ConfigureAwait(false);

            return await ExecuteListPlanAsync<TResult>(plan, cmd, sw, ct).ConfigureAwait(false);
        }

        private bool CanUsePooledPlanCommand<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues)
            => _ctx.RawProvider.SupportsQueryPlanPreparedCommandCache &&
               // Closure-folded SQL (marked by *_unused placeholders) is execution-specific;
               // the pooled command caches the FIRST plan's SQL by fingerprint and would
               // execute it for every later capture (the plan cache already re-translates).
               !HasClosureFoldedIntoSql(plan) &&
               _ctx.Options.CommandInterceptors.Count == 0 &&
               (plan.CompiledParameters.Count == 0 || paramValues != null) &&
               !plan.IsScalar &&
               !plan.SingleResult &&
               plan.GroupJoinInfo == null &&
               plan.Includes.Count == 0 &&
               !plan.SplitQuery &&
               plan.DependentQueries is not { Count: > 0 } &&
               plan.M2MIncludes is not { Count: > 0 } &&
               plan.ClientProjection == null &&
               plan.PostMaterializeTransform == null &&
               typeof(TResult) != typeof(List<object>) &&
               (!plan.ElementType.IsClass ||
                !_ctx.IsMapped(plan.ElementType) ||
                _ctx.GetMapping(plan.ElementType).OwnedCollections.Count == 0);

        private Task<TResult> ExecutePooledQueryPlanSync<TResult>(QueryPlan plan, IReadOnlyList<object?>? paramValues, Stopwatch? sw, CancellationToken ct)
        {
            var pooled = _pooledPlanCommands.GetOrAdd(plan.Fingerprint, _ => CreatePooledPlanCommand(plan));
            lock (pooled.Lock)
            {
                ct.ThrowIfCancellationRequested();
                pooled.Command.Transaction = _ctx.CurrentTransaction;
                if (paramValues != null)
                    BindPooledCompiledParameters(pooled.Command, pooled.FixedParameterCount, plan, paramValues);
                var list = _executor.MaterializePooled(plan, pooled.Command);
                if (plan.PostMaterializeTransform != null)
                    list = plan.PostMaterializeTransform(_ctx, list);
                sw?.Stop();
                _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);
                return Task.FromResult((TResult)(object)list);
            }
        }

        private PooledPlanCommand CreatePooledPlanCommand(QueryPlan plan)
        {
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindPlanParameters(cmd, plan, null);
            var fixedParameterCount = cmd.Parameters.Count;
            foreach (var parameterName in plan.CompiledParameters)
            {
                if (IsUnusedCompiledParameter(parameterName))
                    continue;
                cmd.AddOptimizedParam(parameterName, DBNull.Value);
            }
            try { cmd.Prepare(); } catch (Exception) { }
            return new PooledPlanCommand(cmd, fixedParameterCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void BindPooledCompiledParameters(
            DbCommand cmd,
            int fixedParameterCount,
            QueryPlan plan,
            IReadOnlyList<object?> paramValues)
        {
            var compiledParams = plan.CompiledParameters;
            var count = Math.Min(compiledParams.Count, paramValues.Count);
            var boundIndex = 0;
            for (int i = 0; i < count; i++)
            {
                if (IsUnusedCompiledParameter(compiledParams[i]))
                    continue;
                ParameterAssign.AssignValue(cmd.Parameters[fixedParameterCount + boundIndex], ApplyCompiledParamConverter(plan, compiledParams[i], paramValues[i]) ?? DBNull.Value);
                boundIndex++;
            }

            for (; boundIndex < cmd.Parameters.Count - fixedParameterCount; boundIndex++)
                ParameterAssign.AssignValue(cmd.Parameters[fixedParameterCount + boundIndex], DBNull.Value);
        }

        /// <summary>PERF: Extracted parameter binding to share between fast and slow paths.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsUnusedCompiledParameter(string parameterName)
            => parameterName.EndsWith("_unused", StringComparison.Ordinal);

        /// <summary>
        /// Applies the plan's per-compiled-parameter value converter (if any) so a closure value
        /// compared against a value-converter column binds its provider representation. Almost always
        /// a no-op (no converter columns in the predicate).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static object? ApplyCompiledParamConverter(QueryPlan plan, string name, object? value)
        {
            var converters = plan.ParameterConverters;
            if (value != null && converters != null && converters.TryGetValue(name, out var converter))
                return converter.ConvertToProvider(value);
            return value;
        }

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
                    {
                        if (IsUnusedCompiledParameter(compiledParams[i])) continue;
                        cmd.AddOptimizedParam(compiledParams[i], ApplyCompiledParamConverter(plan, compiledParams[i], paramValues[i]) ?? DBNull.Value);
                    }
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
                // LINQ-to-Objects Sum returns 0 for empty / all-null source - even for
                // nullable element types (`Enumerable.Sum(IEnumerable<decimal?>)` returns 0,
                // not null). SQL `SUM(col)` returns NULL on the same input, so map NULL to
                // zero-of-target so the materialized result matches LINQ semantics.
                if (plan.MethodName == "Sum")
                    return GetZeroOfTargetType<TResult>();
                return default(TResult)!;
            }
            // Min/Max over a value-converter column returns the stored provider value; convert it back to
            // the model representation, matching EF (Sum/Average carry no converter).
            if (plan.ScalarResultConverter != null)
                scalarResult = plan.ScalarResultConverter.ConvertFromProvider(scalarResult) ?? scalarResult;
            return ConvertScalarResult<TResult>(scalarResult)!;
        }

        /// <summary>PERF: Fully synchronous scalar path for providers without true async I/O.</summary>
        private Task<TResult> ExecuteScalarPlanSync<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw)
        {
            var scalarResult = cmd.ExecuteScalarWithInterceptionSerializedAndDispose(_ctx);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
            if (scalarResult == null || scalarResult is DBNull)
            {
                if (plan.MethodName is "Min" or "Max" or "Average" &&
                    typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                    throw new InvalidOperationException("Sequence contains no elements");
                if (plan.MethodName == "Sum")
                    return Task.FromResult(GetZeroOfTargetType<TResult>());
                return Task.FromResult(default(TResult)!);
            }
            if (plan.ScalarResultConverter != null)
                scalarResult = plan.ScalarResultConverter.ConvertFromProvider(scalarResult) ?? scalarResult;
            return Task.FromResult(ConvertScalarResult<TResult>(scalarResult)!);
        }

        // Returns the LINQ-Sum "empty source" value for TResult: 0 for value types
        // (including nullable wrappers like decimal? / int?), and default for reference
        // types (uncommon for Sum, but defensive).
        private static TResult GetZeroOfTargetType<TResult>()
        {
            var underlying = Nullable.GetUnderlyingType(typeof(TResult)) ?? typeof(TResult);
            if (!underlying.IsValueType)
                return default(TResult)!;
            // Activator.CreateInstance on a primitive value type yields the numeric zero;
            // boxing then casting to TResult (decimal / decimal? / int / int? / etc.) gives
            // back the correctly-typed zero, including the nullable wrapper case.
            return (TResult)Activator.CreateInstance(underlying)!;
        }

        /// <summary>PERF: List&lt;object&gt; materialization path - avoids covariant copy.</summary>
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
        /// Saves ~50-100ns per Read() call ? ~1-4us for typical result sets.
        /// </summary>
        private Task<TResult> ExecuteListPlanSyncWrapped<TResult>(QueryPlan plan, DbCommand cmd, Stopwatch? sw)
        {
            var list = _executor.Materialize(plan, cmd);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(plan.Sql, plan.Parameters, sw?.Elapsed ?? default, list.Count);

            object? result;
            if (plan.ClientScalar)
            {
                // The post-materialize transform reduced the reshaped rows to a single
                // boxed aggregate value; unwrap it as the query result. Coerce numeric
                // mismatches like the server scalar path does — nORM's typed aggregate
                // wrappers can declare a narrower result than the LINQ operator computes
                // (e.g. AverageAsync over ints declares int while Average yields double).
                var clientScalar = list[0];
                return Task.FromResult(clientScalar is TResult typedScalar
                    ? typedScalar
                    : ConvertScalarResult<TResult>(clientScalar!));
            }
            if (plan.SingleResult)
            {
                result = plan.MethodName switch
                {
                    "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    // LINQ MinBy/MaxBy return null for an empty sequence of reference-type
                    // or nullable elements; only non-nullable value types throw.
                    "MinBy" or "MaxBy" => list.Count > 0
                        ? list[0]
                        : plan.ElementType.IsValueType && Nullable.GetUnderlyingType(plan.ElementType) == null
                            ? throw new InvalidOperationException("Sequence contains no elements")
                            : null,
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
            if (plan.ClientScalar)
            {
                // The post-materialize transform reduced the reshaped rows to a single
                // boxed aggregate value; unwrap it as the query result, coercing numeric
                // mismatches like the server scalar path (see ExecuteListPlanSyncWrapped).
                var clientScalar = list[0];
                return clientScalar is TResult typedScalar
                    ? typedScalar
                    : ConvertScalarResult<TResult>(clientScalar!);
            }
            if (plan.SingleResult)
            {
                result = plan.MethodName switch
                {
                    "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                    // LINQ MinBy/MaxBy return null for an empty sequence of reference-type
                    // or nullable elements; only non-nullable value types throw.
                    "MinBy" or "MaxBy" => list.Count > 0
                        ? list[0]
                        : plan.ElementType.IsValueType && Nullable.GetUnderlyingType(plan.ElementType) == null
                            ? throw new InvalidOperationException("Sequence contains no elements")
                            : null,
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
    }
}
