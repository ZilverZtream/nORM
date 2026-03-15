using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
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

        // A1/X1 fix: cap concurrent compile operations so repeated hostile-timeout callers cannot
        // starve the thread pool. Each in-flight compile acquires one slot; the slot is released
        // when the compile task finishes (success or exception), NOT when the caller times out.
        // This provides the "bounded thread/task count" guarantee the audit requires.
        private static readonly int _compileSemaphoreCapacity = Math.Max(2, Environment.ProcessorCount);
        private static readonly SemaphoreSlim _compileSemaphore = new(_compileSemaphoreCapacity, _compileSemaphoreCapacity);
        // Exposed for deterministic test assertions (bounded-worker proof).
        internal static int CompileSemaphoreCurrentCount => _compileSemaphore.CurrentCount;
        internal static int CompileSemaphoreCapacity => _compileSemaphoreCapacity;

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

            // A1/X1 fix: acquire one compile slot before launching the task.
            // The slot is released inside the task's finally block (not when the caller times out),
            // so concurrent in-flight compiles never exceed _compileSemaphoreCapacity even under
            // repeated hostile-timeout pressure. Expression.Compile() is not interruptible, but
            // bounding concurrency prevents unbounded thread-pool growth.
            var task = Task.Run(() =>
            {
                _compileSemaphore.Wait(CancellationToken.None); // always release, even after caller timeout
                try
                {
                    result = CompileQueryInternal<TContext, TParam, T>(queryExpression);
                }
                catch (Exception ex)
                {
                    compileException = ex;
                }
                finally
                {
                    _compileSemaphore.Release();
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
            // C1 fix: cap at 256 entries via ConcurrentLruCache so long-lived processes with many
            // distinct tenant/filter/provider/model combinations don't grow this dictionary without bound.
            var plansByCtx = new ConcurrentLruCache<string, (QueryPlan Plan, IReadOnlyList<string> ParamNames, HashSet<string> CompiledParamSet, KeyValuePair<string, object>[]? FixedParams)>(256);
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
                        // SG1/X1 fix: use GetFilterKey() instead of f.ToString().
                        // f.ToString() is shape-only — same string regardless of captured closure value.
                        // ExpressionFingerprint alone also misses closure values: it hashes the closure
                        // object reference (whose ToString() is the type name), not the actual field values.
                        // GetFilterKey() combines the shape fingerprint with a recursive extraction of all
                        // closure-accessed field values so two lambdas with the same shape but different
                        // captured values (e.g., tenantId=1 vs tenantId=2) produce distinct cache keys.
                        ctxKey = string.Concat(
                            ctx.Provider.GetType().FullName, "|",
                            ctx.GetMappingHash().ToString(), "|",
                            tenantId?.ToString() ?? "", "|",
                            ctx.Options.GlobalFilters.Count > 0
                                ? string.Join(";", ctx.Options.GlobalFilters.SelectMany(kvp => kvp.Value.Select(GetFilterKey)))
                                : "");
                        cachedCtxKey = ctxKey;
                        cachedCtxKeyOwner = ctx;
                    }

                    // C1 fix: use GetOrAdd on the LRU cache so evicted entries are re-computed on
                    // demand and concurrent misses serialize through the factory (at most once per key).
                    var capturedCtx = ctx;
                    var capturedExpr = queryExpression;
                    var entry = plansByCtx.GetOrAdd(ctxKey, __ =>
                    {
                        var ctxParam = capturedExpr.Parameters[0];
                        var body = new ParameterReplacer(ctxParam, Expression.Constant(capturedCtx)).Visit(capturedExpr.Body)!;
                        body = new QueryCallEvaluator().Visit(body)!;
                        var p = capturedCtx.GetQueryProvider().GetPlan(body, out _, out _);
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
                        return (p, p.CompiledParameters, paramSet, fixedParams);
                    });
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
                    // P1 fix: use AssignValue (not direct .Value) so DbType and Size are reset
                    // on null values — prevents stale metadata carry-over on reused parameters.
                    var compiledParams = cachedPlan.CompiledParameters;
                    var compiledCount = Math.Min(compiledParams.Count, args.Length);
                    var fixedParamCount = state.FixedParamCount;
                    for (int i = 0; i < compiledCount; i++)
                        ParameterAssign.AssignValue(cmd.Parameters[fixedParamCount + i], args[i]);

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

        /// <summary>
        /// SG1/X1 fix: builds a cache key for a global filter expression that captures both
        /// expression shape and closure-captured runtime values.
        ///
        /// f.ToString() is shape-only (same string regardless of captured value).
        /// ExpressionFingerprint alone misses closure values because the captured closure object's
        /// AppendStableValue fallback uses value.ToString() which returns the type name, not field values.
        /// This method appends actual field values read via reflection so that two lambdas with the
        /// same structure but different captured variables (e.g. tenantId=1 vs tenantId=2) get
        /// distinct keys.
        /// </summary>
        internal static string GetFilterKey(LambdaExpression filter)
        {
            var shapeKey = ExpressionFingerprint.Compute(filter).ToString();
            var sb = new StringBuilder();
            AppendClosureValues(filter.Body, sb);
            return sb.Length == 0 ? shapeKey : string.Concat(shapeKey, "|CV:", sb.ToString());
        }

        private static void AppendClosureValues(Expression expr, StringBuilder sb)
        {
            // Closure field access pattern: member access on a constant (compiler-generated closure).
            // Reading the field gives the actual captured runtime value.
            if (expr is MemberExpression me && me.Expression is ConstantExpression ce)
            {
                try
                {
                    object? val = me.Member is FieldInfo fi ? fi.GetValue(ce.Value) :
                                  me.Member is PropertyInfo pi ? pi.GetValue(ce.Value) : null;
                    sb.Append(val?.ToString() ?? "null");
                    sb.Append(';');
                }
                catch { /* ignore reflection failures — treats the value as stable */ }
                return; // Don't recurse further into this constant node
            }

            switch (expr)
            {
                case BinaryExpression bin:
                    AppendClosureValues(bin.Left, sb);
                    AppendClosureValues(bin.Right, sb);
                    break;
                case UnaryExpression u:
                    AppendClosureValues(u.Operand, sb);
                    break;
                case MethodCallExpression mc:
                    if (mc.Object != null) AppendClosureValues(mc.Object, sb);
                    foreach (var a in mc.Arguments) AppendClosureValues(a, sb);
                    break;
                case LambdaExpression lam:
                    AppendClosureValues(lam.Body, sb);
                    break;
                case ConditionalExpression cond:
                    AppendClosureValues(cond.Test, sb);
                    AppendClosureValues(cond.IfTrue, sb);
                    AppendClosureValues(cond.IfFalse, sb);
                    break;
                case MemberExpression mem:
                    if (mem.Expression != null) AppendClosureValues(mem.Expression, sb);
                    break;
            }
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
