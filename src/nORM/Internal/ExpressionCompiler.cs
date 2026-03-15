using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Query;

#nullable enable

namespace nORM.Internal
{
    /// <summary>
    /// Holds pooled command state for a compiled query. The command is created once (with Prepare())
    /// and reused across calls — only parameter values are updated. This eliminates per-call costs
    /// of DbCommand creation, DbParameter allocation, and SQL compilation (sqlite3_prepare_v2).
    /// </summary>
    internal sealed class CompiledQueryState
    {
        // Q1 fix: pool of prepared commands — concurrent callers each dequeue their own command.
        // Sequential callers (common case) reuse the same command from the pool with zero contention.
        public readonly System.Collections.Concurrent.ConcurrentQueue<System.Data.Common.DbCommand> CommandPool = new();
        public int FixedParamCount;
    }

    internal static class ExpressionCompiler
    {
        private static readonly ConcurrentDictionary<ExpressionFingerprint, Delegate> _compiledDelegateCache = new();

        public static Func<T, TResult> CompileExpression<T, TResult>(Expression<Func<T, TResult>> expr)
        {
            var key = ExpressionFingerprint.Compute(expr);

            if (_compiledDelegateCache.TryGetValue(key, out var cached))
                return (Func<T, TResult>)cached;

            var compiled = expr.Compile();
            _compiledDelegateCache.TryAdd(key, compiled);
            return compiled;
        }

        public static Func<TContext, TParam, Task<List<T>>> CompileQuery<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression)
            where TContext : DbContext
            where T : class
        {
            ExpressionUtils.ValidateExpression(queryExpression);
            var timeout = ExpressionUtils.GetCompilationTimeout(queryExpression);
            using var cts = new CancellationTokenSource(timeout);
            return CompileWithTimeout<TContext, TParam, T>(queryExpression, cts.Token);
        }

        private static Func<TContext, TParam, Task<List<T>>> CompileWithTimeout<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression, CancellationToken token)
            where TContext : DbContext
            where T : class
        {
            Func<TContext, TParam, Task<List<T>>>? result = null;
            Exception? compileException = null;

            var task = Task.Run(() =>
            {
                try
                {
                    result = CompileQueryInternal<TContext, TParam, T>(queryExpression);
                }
                catch (Exception ex)
                {
                    compileException = ex;
                }
            }, token);

            try
            {
                task.Wait(token);
            }
            catch (OperationCanceledException ex)
            {
                throw new TimeoutException("Expression compilation timed out", ex);
            }

            if (compileException != null)
                throw compileException;

            return result!;
        }

        private static Func<TContext, TParam, Task<List<T>>> CompileQueryInternal<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression)
            where TContext : DbContext
            where T : class
        {
            // SG-1/QP-1/SEC-1: Use per-context-shape cache keyed by a collision-resistant string
            // that encodes provider type, mappings, tenant ID, and global filter expressions.
            var plansByCtx = new ConcurrentDictionary<string, (QueryPlan Plan, IReadOnlyList<string> ParamNames, HashSet<string> CompiledParamSet, KeyValuePair<string, object>[]? FixedParams)>();
            string? cachedCtxKey = null;
            DbContext? cachedCtxKeyOwner = null;
            (QueryPlan Plan, IReadOnlyList<string> ParamNames, HashSet<string> CompiledParamSet, KeyValuePair<string, object>[]? FixedParams) cachedEntry = default;
            bool hasCachedEntry = false;
            // Q1/4.0-4.5 fix: per-context pooled state using ConditionalWeakTable.
            // Each DbContext gets its own CompiledQueryState (and CommandPool). This prevents
            // cross-connection command contamination when the same compiled delegate is called
            // from concurrent tasks each holding their own DbContext. Commands are bound to a
            // specific DbConnection; sharing across connections causes InvalidOperationException.
            // ConditionalWeakTable provides automatic GC-tracked lifetime: when a context is
            // collected after Dispose, its pooled commands are released automatically.
            var stateByCtx = new System.Runtime.CompilerServices.ConditionalWeakTable<DbContext, CompiledQueryState>();
            // PERF: Single-slot fast-path cache for same-context repeated calls (common case).
            // The fast-path vars are NOT synchronized — concurrent different-context callers
            // may race on them, causing a stale cache miss. That is safe: the only consequence
            // is a ConditionalWeakTable lookup instead of a direct field read (no correctness impact).
            DbContext? fastCtxOwner = null;
            CompiledQueryState? fastCtxState = null;
            // PERF: Cache the sync materializer delegate.
            Func<System.Data.Common.DbDataReader, object>? cachedMaterializer = null;

            return (ctx, value) =>
            {
                // PERF: Fast path — same context as last call (common in benchmarks and real apps).
                if (!hasCachedEntry || !ReferenceEquals(cachedCtxKeyOwner, ctx))
                {
                    string ctxKey;
                    if (ReferenceEquals(cachedCtxKeyOwner, ctx) && cachedCtxKey != null)
                    {
                        ctxKey = cachedCtxKey;
                    }
                    else
                    {
                        var tenantId = ctx.Options.TenantProvider?.GetCurrentTenantId();
                        ctxKey = string.Concat(
                            ctx.Provider.GetType().FullName, "|",
                            ctx.GetMappingHash().ToString(), "|",
                            tenantId?.ToString() ?? "", "|",
                            ctx.Options.GlobalFilters.Count > 0
                                ? string.Join(";", ctx.Options.GlobalFilters.SelectMany(kvp => kvp.Value.Select(f => f.ToString())))
                                : "");
                        cachedCtxKey = ctxKey;
                        cachedCtxKeyOwner = ctx;
                    }

                    if (!plansByCtx.TryGetValue(ctxKey, out var entry))
                    {
                        var ctxParam = queryExpression.Parameters[0];
                        var body = new ParameterReplacer(ctxParam, Expression.Constant(ctx)).Visit(queryExpression.Body)!;
                        body = new QueryCallEvaluator().Visit(body)!;
                        var p = ctx.GetQueryProvider().GetPlan(body, out _, out _);
                        var paramSet = new HashSet<string>(p.CompiledParameters, StringComparer.Ordinal);
                        KeyValuePair<string, object>[]? fixedParams = null;
                        if (paramSet.Count > 0)
                        {
                            var fpList = new List<KeyValuePair<string, object>>();
                            foreach (var kvp in p.Parameters)
                            {
                                if (!paramSet.Contains(kvp.Key))
                                    fpList.Add(kvp);
                            }
                            fixedParams = fpList.ToArray();
                        }
                        entry = (p, p.CompiledParameters, paramSet, fixedParams);
                        plansByCtx[ctxKey] = entry;
                    }
                    cachedEntry = entry;
                    hasCachedEntry = true;
                    cachedMaterializer = cachedEntry.Plan.SyncMaterializer;
                }

                var cachedPlan = cachedEntry.Plan;
                var paramNames = cachedEntry.ParamNames;

                // PERF: Reuse single-element array for the common single-param case
                object?[] args;
                if (paramNames != null && paramNames.Count > 0)
                {
                    if (value is System.Runtime.CompilerServices.ITuple tuple)
                    {
                        var arr = new object?[paramNames.Count];
                        var count = Math.Min(tuple.Length, paramNames.Count);
                        for (int i = 0; i < count; i++)
                            arr[i] = tuple[i];
                        args = arr;
                    }
                    else if (paramNames.Count == 1)
                    {
                        args = new object?[] { (object?)value };
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"Compiled query expects {paramNames.Count} parameters. " +
                            "Pass values as a ValueTuple, e.g. (value1, value2).");
                    }
                }
                else
                {
                    args = Array.Empty<object?>();
                }

                // Resolve per-context pooled state (fast-path: same ctx as last call).
                CompiledQueryState state;
                if (ReferenceEquals(fastCtxOwner, ctx) && fastCtxState != null)
                {
                    state = fastCtxState;
                }
                else
                {
                    state = stateByCtx.GetOrCreateValue(ctx);
                    fastCtxOwner = ctx;
                    fastCtxState = state;
                }

                // PERF: Inline pooled sync execution for providers without true async I/O (SQLite).
                // Bypasses the entire NormQueryProvider call chain (RetryPolicy, CacheProvider,
                // EnsureConnectionAsync, IsScalar dispatch) by inlining command reuse + sync read.
                // Pooled commands avoid per-call DbCommand/DbParameter allocation and SQL compilation.
                if (cachedPlan != null &&
                    ctx.Provider.PrefersSyncExecution &&
                    ctx.Options.RetryPolicy == null &&
                    ctx.Options.CacheProvider == null &&
                    ctx.Options.CommandInterceptors.Count == 0 &&
                    !cachedPlan.IsScalar)
                {
                    // Q1 fix: dequeue a prepared command from the per-context pool, or create new
                    if (!state.CommandPool.TryDequeue(out var cmd))
                    {
                        cmd = ctx.CreateCommand();
                        cmd.CommandText = cachedPlan.Sql;
                        var fixedParams = cachedEntry.FixedParams;
                        int fixedCount = 0;
                        if (fixedParams != null)
                        {
                            for (int i = 0; i < fixedParams.Length; i++)
                            {
                                var p = cmd.CreateParameter();
                                p.ParameterName = fixedParams[i].Key;
                                p.Value = fixedParams[i].Value;
                                cmd.Parameters.Add(p);
                            }
                            fixedCount = fixedParams.Length;
                        }
                        else
                        {
                            foreach (var kvp in cachedPlan.Parameters)
                            {
                                var p = cmd.CreateParameter();
                                p.ParameterName = kvp.Key;
                                p.Value = kvp.Value;
                                cmd.Parameters.Add(p);
                                fixedCount++;
                            }
                        }
                        var compiledParams2 = cachedPlan.CompiledParameters;
                        for (int i = 0; i < compiledParams2.Count; i++)
                        {
                            var p = cmd.CreateParameter();
                            p.ParameterName = compiledParams2[i];
                            p.Value = DBNull.Value;
                            cmd.Parameters.Add(p);
                        }
                        try { cmd.Prepare(); } catch { }
                        state.FixedParamCount = fixedCount;
                    }

                    // Update compiled parameter values (only these change per call)
                    var compiledParams = cachedPlan.CompiledParameters;
                    var compiledCount = Math.Min(compiledParams.Count, args.Length);
                    var fixedParamCount = state.FixedParamCount;
                    for (int i = 0; i < compiledCount; i++)
                        cmd.Parameters[fixedParamCount + i].Value = args[i] ?? DBNull.Value;

                    var materializer = cachedMaterializer!;
                    var capacity = cachedPlan.SingleResult ? 1 : (cachedPlan.Take ?? 16);
                    var list = new List<T>(capacity);

                    try
                    {
                        using var reader = cmd.ExecuteReader();
                        if (cachedPlan.SingleResult)
                        {
                            var maxRows = cachedPlan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                            for (int row = 0; row < maxRows; row++)
                            {
                                if (!reader.Read()) break;
                                list.Add((T)materializer(reader));
                            }
                        }
                        else
                        {
                            while (reader.Read())
                                list.Add((T)materializer(reader));
                        }
                    }
                    finally
                    {
                        // Return command to per-context pool for next caller on this context
                        state.CommandPool.Enqueue(cmd);
                    }

                    return Task.FromResult(list);
                }

                // Standard path for async providers or when advanced features are enabled
                return ctx.GetQueryProvider().ExecuteCompiledPooledAsync<List<T>>(
                    cachedPlan!, args, cachedEntry.FixedParams, state, default);
            };
        }

        internal static object? Evaluate(Expression expression)
        {
            ExpressionUtils.ValidateExpression(expression);
            var timeout = ExpressionUtils.GetCompilationTimeout(expression);
            using var cts = new CancellationTokenSource(timeout);
            var del = ExpressionUtils.CompileWithFallback(Expression.Lambda(expression), cts.Token);
            return del.DynamicInvoke();
        }

        private sealed class QueryCallEvaluator : ExpressionVisitor
        {
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.DeclaringType == typeof(NormQueryable) && node.Method.Name == "Query")
                {
                    var result = Evaluate(node);
                    return Expression.Constant(result, node.Type);
                }
                return base.VisitMethodCall(node);
            }
        }
    }
}
