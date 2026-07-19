using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {
        // INTERNAL API: Optimized version that accepts array of values instead of Dictionary
        internal Task<TResult> ExecuteCompiledAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            if (_ctx.Options.RetryPolicy != null)
                return new RetryingExecutionStrategy(_ctx, _ctx.Options.RetryPolicy).ExecuteAsync((_, token) => ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, token), ct);
            return ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, ct);
        }

        // Overload that accepts pre-computed fixedParams to avoid:
        // 1. Expensive QueryPlan.GetHashCode() in _compiledParamSets ConcurrentDictionary on every call
        //    (QueryPlan is a sealed record with 20+ properties - auto-generated GetHashCode costs ~200ns)
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
            if (ResultCacheUsable(plan))
            {
                var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
                Func<Task<TResult>> queryExecutorFactory = () => ExecuteCompiledDictAsync<TResult>(plan, finalParameters, sw, ct);
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, finalParameters);
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return await ExecuteWithCacheAsync(cacheKey, plan.CacheTables ?? plan.Tables, expiration, queryExecutorFactory, ct).ConfigureAwait(false);
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
                    if (plan.MethodName == "Sum")
                        return GetZeroOfTargetType<TResult>();
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
                        "First" or "MinBy" or "MaxBy" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
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
        /// Bounded FIFO cache mapping QueryPlan ? compiled parameter name set (O(1) vs O(N) lookup).
        /// Capped at 10 000 entries to prevent unbounded memory growth under adversarial or
        /// pathological query-shape churn (Q1/X1 audit finding). FIFO eviction: oldest-inserted
        /// entry is dropped first when the cap is exceeded.
        /// </summary>
        private static readonly nORM.Internal.BoundedCache<QueryPlan, HashSet<string>> _compiledParamSets
            = new(maxSize: 10_000);

        private Task<TResult> ExecuteCompiledInternalArrayAsync<TResult>(QueryPlan plan, object?[] parameterValues, CancellationToken ct)
        {
            // For the caching path (rare), use async method.
            if (ResultCacheUsable(plan))
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
                if (IsUnusedCompiledParameter(compiledParams[i])) continue;
                cmd.AddOptimizedParam(compiledParams[i], parameterValues[i] ?? DBNull.Value);
            }

            // Dispatch directly to materialization - avoids wrapping in another async method.
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
                if (IsUnusedCompiledParameter(compiledParams[i])) continue;
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
            if (ResultCacheUsable(plan))
                return ExecuteCompiledInternalArrayCachedAsync<TResult>(plan, parameterValues, ct);

            // Ensure connection is ready (fast no-op when already open).
            // This matches the non-compiled path to ensure identical behavior.
            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCompiledPreparedSlowAsync<TResult>(ensureTask, plan, parameterValues, fixedParams, ct);

            // Synchronous command setup - same as non-compiled ExecuteQueryFromPlanAsync
            var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = plan.Sql;
            BindCompiledParameters(cmd, plan, parameterValues, fixedParams);

            // Dispatch directly to the same materializer methods used by non-compiled queries.
            if (plan.IsScalar)
            {
                if (_ctx.RawProvider.PrefersSyncExecution)
                    return ExecuteScalarPlanSync<TResult>(plan, cmd, null);
                return ExecuteScalarPlanAsync<TResult>(plan, cmd, null, ct);
            }
            // Sync materialization for providers without true async I/O
            if (_ctx.RawProvider.PrefersSyncExecution)
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
                if (_ctx.RawProvider.PrefersSyncExecution)
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
                // Fast path: pre-computed fixed params - no HashSet lookup needed
                for (int i = 0; i < fixedParams.Length; i++)
                    cmd.AddOptimizedParam(fixedParams[i].Key, fixedParams[i].Value);
            }
            else
            {
                // Fallback: no pre-computed fixedParams available, add only SQL-visible constants.
                var compiledSet = compiledParams.Count == 0
                    ? null
                    : new HashSet<string>(compiledParams, StringComparer.Ordinal);
                foreach (var p in plan.Parameters)
                {
                    if (compiledSet?.Contains(p.Key) == true) continue;
                    cmd.AddOptimizedParam(p.Key, p.Value);
                }
            }

            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            for (int i = 0; i < count; i++)
            {
                if (IsUnusedCompiledParameter(compiledParams[i])) continue;
                cmd.AddOptimizedParam(compiledParams[i], ApplyCompiledParamConverter(plan, compiledParams[i], parameterValues[i]) ?? DBNull.Value);
            }
        }

        /// <summary>
        /// Compiled query execution with command pooling. The DbCommand is created once
        /// (with Prepare()) and reused across calls - only parameter values are updated.
        /// This eliminates per-call costs of: DbCommand allocation (~0.5us), DbParameter creation
        /// (~0.3us per param), and SQL compilation via sqlite3_prepare_v2 (~2-5us).
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
            if (ResultCacheUsable(plan))
                return ExecuteCompiledInternalArrayCachedAsync<TResult>(plan, parameterValues, ct);

            var ensureTask = _ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteCompiledPooledSlowAsync<TResult>(ensureTask, plan, parameterValues, fixedParams, state, ct);

            if (_ctx.RawProvider.PrefersSyncCompiledQueryExecution)
                return ExecuteCompiledFreshSync<TResult>(plan, parameterValues, fixedParams);

            // Q1 fix: reuse pooled prepared command to avoid repeated allocations; create new if pool is empty
            if (!state.CommandPool.TryDequeue(out var cmd))
                cmd = CreateAndPreparePooledCommand(plan, parameterValues, fixedParams, state);

            // Only update compiled parameter values - fixed params are already set
            UpdateCompiledParameterValues(cmd, plan, parameterValues, state.FixedParamCount);
            cmd.Transaction = _ctx.CurrentTransaction;

            // Inline materialization - command returned to pool after use
            if (plan.IsScalar)
                return ReturnCommandToPool(state.CommandPool, cmd, ExecutePooledScalarAsync<TResult>(plan, cmd, ct));
            // Sync materialization for providers without true async I/O
            if (_ctx.RawProvider.PrefersSyncCompiledQueryExecution)
            {
                try
                {
                    return ExecutePooledListSync<TResult>(plan, cmd);
                }
                finally
                {
                    state.CommandPool.Enqueue(cmd);
                }
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
            if (_ctx.RawProvider.PrefersSyncCompiledQueryExecution)
                return await ExecuteCompiledFreshSync<TResult>(plan, parameterValues, fixedParams).ConfigureAwait(false);

            // Q1 fix: reuse pooled prepared command to avoid repeated allocations; create new if pool is empty
            if (!state.CommandPool.TryDequeue(out var cmd))
                cmd = CreateAndPreparePooledCommand(plan, parameterValues, fixedParams, state);
            UpdateCompiledParameterValues(cmd, plan, parameterValues, state.FixedParamCount);
            cmd.Transaction = _ctx.CurrentTransaction;
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
            QueryPlan plan,
            object?[] parameterValues,
            KeyValuePair<string, object>[]? fixedParams,
            CompiledQueryState state)
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
                    ParameterAssign.AssignValue(p, fixedParams[i].Value);
                    cmd.Parameters.Add(p);
                }
                fixedCount = fixedParams.Length;
            }
            else
            {
                // No pre-computed fixed params: add only SQL-visible constants.
                var compiledSet = plan.CompiledParameters.Count == 0
                    ? null
                    : new HashSet<string>(plan.CompiledParameters, StringComparer.Ordinal);
                foreach (var kvp in plan.Parameters)
                {
                    if (compiledSet?.Contains(kvp.Key) == true) continue;
                    var p = cmd.CreateParameter();
                    p.ParameterName = kvp.Key;
                    ParameterAssign.AssignValue(p, kvp.Value);
                    cmd.Parameters.Add(p);
                    fixedCount++;
                }
            }

            // Pre-create compiled parameter slots with the first call's values. Providers
            // such as Npgsql infer prepared-statement parameter types during Prepare().
            for (int i = 0; i < plan.CompiledParameters.Count; i++)
            {
                if (IsUnusedCompiledParameter(plan.CompiledParameters[i])) continue;
                var p = cmd.CreateParameter();
                p.ParameterName = plan.CompiledParameters[i];
                if (i < parameterValues.Length)
                    ParameterAssign.AssignValue(p, parameterValues[i]);
                else
                    p.Value = DBNull.Value;
                cmd.Parameters.Add(p);
            }

            // Prepare the command - compiles SQL once, subsequent executions skip sqlite3_prepare_v2.
            // Prepare() is optional - some providers (e.g., in-memory) throw NotSupportedException.
            ApplyPreparedParameterSizeHints(cmd);
            try { cmd.Prepare(); } catch (Exception) { }

            state.FixedParamCount = fixedCount;
            return cmd;
        }

        private static void ApplyPreparedParameterSizeHints(DbCommand cmd)
        {
            foreach (DbParameter parameter in cmd.Parameters)
            {
                ApplyPreparedParameterSizeHint(cmd, parameter);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void UpdateCompiledParameterValues(
            DbCommand cmd, QueryPlan plan,
            object?[] parameterValues, int fixedParamCount)
        {
            var compiledParams = plan.CompiledParameters;
            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            // P1 fix: call AssignValue (not direct .Value assignment) so that DbType and Size
            // are reset when the value is null, preventing stale metadata carry-over.
            var slot = fixedParamCount;
            for (int i = 0; i < count; i++)
            {
                if (IsUnusedCompiledParameter(compiledParams[i])) continue;
                var parameter = cmd.Parameters[slot++];
                ParameterAssign.AssignValue(parameter, ApplyCompiledParamConverter(plan, compiledParams[i], parameterValues[i]));
                ApplyPreparedParameterSizeHint(cmd, parameter);
            }
        }

        private static void ApplyPreparedParameterSizeHint(DbCommand cmd, DbParameter parameter)
        {
            if (parameter.DbType is DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength)
                parameter.Size = nORM.Internal.ParameterOptimizer.MaxInlineStringSize;
            else if (parameter.DbType == DbType.Binary)
                parameter.Size = -1;
            else if (cmd.GetType().FullName == "Microsoft.Data.SqlClient.SqlCommand" && parameter.Size == 0)
                parameter.Size = 1;
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
                var (idMap, idMapping) = QueryExecutor.CreateIdentityResolutionMap(_ctx, plan);
                while (reader.Read())
                {
                    var entity = materializer(reader);
                    if (idMap != null)
                        entity = QueryExecutor.ResolveRootIdentity(entity, idMap, idMapping);
                    list.Add(entity);
                }
            }

            if (plan.PostReverse) QueryExecutor.ReverseListInPlace(list);
            if (plan.PostMaterializeTransform != null) list = plan.PostMaterializeTransform(_ctx, list);

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
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;

            // Bind fixed parameters (constants from the compiled expression)
            if (fixedParams != null)
            {
                for (int i = 0; i < fixedParams.Length; i++)
                    cmd.AddOptimizedParam(fixedParams[i].Key, fixedParams[i].Value);
            }
            else
            {
                // No pre-computed fixed params: add only SQL-visible constants.
                var compiledSet = plan.CompiledParameters.Count == 0
                    ? null
                    : new HashSet<string>(plan.CompiledParameters, StringComparer.Ordinal);
                foreach (var kvp in plan.Parameters)
                {
                    if (compiledSet?.Contains(kvp.Key) == true) continue;
                    cmd.AddOptimizedParam(kvp.Key, kvp.Value);
                }
            }

            // Bind compiled parameters (values from the caller)
            var compiledParams = plan.CompiledParameters;
            var count = Math.Min(compiledParams.Count, parameterValues.Length);
            for (int i = 0; i < count; i++)
            {
                if (IsUnusedCompiledParameter(compiledParams[i])) continue;
                cmd.AddOptimizedParam(compiledParams[i], parameterValues[i] ?? DBNull.Value);
            }

            if (plan.IsScalar)
            {
                var scalarResult = cmd.ExecuteScalarWithInterception(_ctx);
                if (scalarResult == null || scalarResult is DBNull)
                {
                    if (plan.MethodName is "Min" or "Max" or "Average" &&
                        typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                        throw new InvalidOperationException("Sequence contains no elements");
                    if (plan.MethodName == "Sum")
                        return Task.FromResult(GetZeroOfTargetType<TResult>());
                    return Task.FromResult(default(TResult)!);
                }

                return Task.FromResult(ConvertScalarResult<TResult>(scalarResult)!);
            }

            // Sync materialization - no async state machine overhead
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

            var (idMap, idMapping) = QueryExecutor.CreateIdentityResolutionMap(_ctx, plan);
            while (reader.Read())
            {
                var entity = materializer(reader);
                if (idMap != null)
                    entity = QueryExecutor.ResolveRootIdentity(entity, idMap, idMapping);
                list.Add(entity);
            }

            if (plan.PostReverse) QueryExecutor.ReverseListInPlace(list);
            if (plan.PostMaterializeTransform != null) list = plan.PostMaterializeTransform(_ctx, list);

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
            // Plans with a post-materialize transform read rows before the transform
            // produces the final element type, so materialize into List<object> and let
            // the transform build the typed result (mirrors QueryExecutor.MaterializeAsync).
            var list = plan.PostMaterializeTransform != null
                ? (System.Collections.IList)new List<object>(capacity)
                : _executor.CreateListForType(plan.ElementType, capacity);
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
                var (idMap, idMapping) = QueryExecutor.CreateIdentityResolutionMap(_ctx, plan);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var entity = materializer(reader);
                    if (idMap != null)
                        entity = QueryExecutor.ResolveRootIdentity(entity, idMap, idMapping);
                    list.Add(entity);
                }
            }

            if (plan.PostReverse) QueryExecutor.ReverseListInPlace(list);
            if (plan.PostMaterializeTransform != null) list = plan.PostMaterializeTransform(_ctx, list);

            if (plan.ClientScalar)
            {
                // The transform reduced the reshaped rows to a single boxed aggregate;
                // coerce numeric mismatches like the server scalar path.
                var clientScalar = list[0];
                return clientScalar is TResult typedScalar
                    ? typedScalar
                    : ConvertScalarResult<TResult>(clientScalar!);
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

            var (idMap, idMapping) = QueryExecutor.CreateIdentityResolutionMap(_ctx, plan);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = materializer(reader);
                if (idMap != null)
                    entity = QueryExecutor.ResolveRootIdentity(entity, idMap, idMapping);
                list.Add(entity);
            }

            if (plan.PostReverse) QueryExecutor.ReverseListInPlace(list);
            if (plan.PostMaterializeTransform != null)
            {
                var transformed = plan.PostMaterializeTransform(_ctx, list);
                var rebuilt = new List<object>(transformed.Count);
                foreach (var item in transformed) rebuilt.Add(item!);
                list = rebuilt;
            }

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
                if (plan.MethodName == "Sum")
                    return GetZeroOfTargetType<TResult>();
                return default(TResult)!;
            }
            return ConvertScalarResult<TResult>(scalarResult)!;
        }

        private static object HandleSingleResult(QueryPlan plan, IList list)
        {
            return plan.MethodName switch
            {
                "First" or "MinBy" or "MaxBy" => list.Count > 0 ? list[0]! : throw new InvalidOperationException("Sequence contains no elements"),
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
                    if (plan.MethodName == "Sum")
                        return GetZeroOfTargetType<TResult>();
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
                        "First" or "MinBy" or "MaxBy" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
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
            {
                if (IsUnusedCompiledParameter(plan.CompiledParameters[i])) continue;
                dict[plan.CompiledParameters[i]] = parameterValues[i] ?? DBNull.Value;
            }

            var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, dict);
            var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
            return await ExecuteWithCacheAsync(cacheKey, plan.CacheTables ?? plan.Tables, expiration,
                () => ExecuteCompiledInternalArrayAsync<TResult>(plan, parameterValues, ct), ct).ConfigureAwait(false);
        }

    }
}
