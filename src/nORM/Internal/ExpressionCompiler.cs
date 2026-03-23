using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
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
        // Per-context plan-cache key. Written once (lazily) on the first call from this context.
        // Stored here so concurrent different-context callers never share a key slot and cannot
        // overwrite each other's key — eliminating the shared-closure ctxKey race.
        public string? CachedCtxKey;
    }

    internal static class ExpressionCompiler
    {
        /// <summary>Maximum number of compiled delegate cache entries before LRU eviction.</summary>
        private const int DelegateCacheCapacity = 512;

        /// <summary>Maximum number of per-context query plan cache entries before LRU eviction.</summary>
        private const int PlanCacheCapacity = 256;

        // Bounded to prevent unbounded memory growth over long process lifetimes.
        // Covers the vast majority of real-app expression shapes (one per call-site per provider).
        // Internal for test assertions.
        internal static readonly ConcurrentLruCache<ExpressionFingerprint, Delegate> _compiledDelegateCache = new(DelegateCacheCapacity);

        // Cap concurrent compile operations so repeated hostile-timeout callers cannot starve
        // the thread pool. Each in-flight compile acquires one slot; the slot is released when
        // the compile finishes (success or exception), not when the caller times out.
        private static readonly int _compileSemaphoreCapacity = Math.Max(2, Environment.ProcessorCount);
        private static readonly SemaphoreSlim _compileSemaphore = new(_compileSemaphoreCapacity, _compileSemaphoreCapacity);
        // Exposed for deterministic test assertions (bounded-worker proof).
        internal static int CompileSemaphoreCurrentCount => _compileSemaphore.CurrentCount;
        internal static int CompileSemaphoreCapacity => _compileSemaphoreCapacity;

        public static Func<T, TResult> CompileExpression<T, TResult>(Expression<Func<T, TResult>> expr)
        {
            var key = ExpressionFingerprint.Compute(expr);

            if (_compiledDelegateCache.TryGet(key, out var cached))
                return (Func<T, TResult>)cached;

            var compiled = expr.Compile();
            _compiledDelegateCache.Set(key, compiled);
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

            // Acquire one compile slot before launching the task.
            // The slot is released inside the task's finally block (not when the caller times out),
            // so concurrent in-flight compiles never exceed _compileSemaphoreCapacity even under
            // repeated hostile-timeout pressure. Expression.Compile() is not interruptible, but
            // bounding concurrency prevents unbounded thread-pool growth.
            var task = Task.Run(() =>
            {
                _compileSemaphore.Wait(token); // cancellable wait; if cancelled, task faults without acquiring semaphore
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
            catch (AggregateException ae) when (ae.InnerException is OperationCanceledException oce)
            {
                // task.Wait throws AggregateException when the task itself is canceled or
                // faults with OperationCanceledException (e.g., _compileSemaphore.Wait(token)
                // inside Task.Run threw OCE while the caller token fired). Unwrap and rethrow
                // as TimeoutException to match the OperationCanceledException catch above.
                throw new TimeoutException("Expression compilation timed out", oce);
            }

            if (compileException != null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(compileException).Throw();

            return result!;
        }

        private static Func<TContext, TParam, Task<List<T>>> CompileQueryInternal<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression)
            where TContext : DbContext
            where T : class
        {
            // Use per-context-shape cache keyed by a collision-resistant string
            // that encodes provider type, mappings, tenant ID, and global filter expressions.
            // Cap entries via ConcurrentLruCache so long-lived processes with many
            // distinct tenant/filter/provider/model combinations don't grow this dictionary without bound.
            // Shared plan cache (thread-safe ConcurrentLruCache, keyed by context shape string).
            var plansByCtx = new ConcurrentLruCache<string, (QueryPlan Plan, IReadOnlyList<string> ParamNames, HashSet<string> CompiledParamSet, KeyValuePair<string, object>[]? FixedParams)>(PlanCacheCapacity);
            // Per-context pooled state via ConditionalWeakTable.
            // Each DbContext gets its own CompiledQueryState (CommandPool + per-ctx key cache).
            // Prevents cross-connection command contamination and eliminates shared-state key races.
            // ConditionalWeakTable provides automatic GC-tracked lifetime.
            var stateByCtx = new System.Runtime.CompilerServices.ConditionalWeakTable<DbContext, CompiledQueryState>();
            // Single-slot fast-path HINTS for the common case of repeated calls on the same ctx.
            // These vars are NOT synchronized — races cause a ConditionalWeakTable lookup (minor perf hit),
            // not a correctness failure, because correctness depends on per-ctx state (state.CachedCtxKey),
            // not on these shared hints.
            DbContext? fastCtxOwner = null;
            CompiledQueryState? fastCtxState = null;

            return (ctx, value) =>
            {
                // ── 1. Resolve per-context state (thread-safe via ConditionalWeakTable) ──────
                // State stores CachedCtxKey (per-ctx plan key) and CommandPool.
                // Fast-path: same ctx as last call → direct field read (unsynchronized hint).
                // Race consequence: stale miss → falls back to GetOrCreateValue (correct, O(1)).
                CompiledQueryState state;
                if (ReferenceEquals(fastCtxOwner, ctx) && fastCtxState != null)
                {
                    state = fastCtxState;
                }
                else
                {
                    state = stateByCtx.GetOrCreateValue(ctx);
                    fastCtxOwner = ctx;   // unsynchronized hint — safe (performance only)
                    fastCtxState = state; // unsynchronized hint — safe (performance only)
                }

                // ── 2. Get or compute plan-cache key (per-context, no sharing) ──────────────
                // CachedCtxKey is stored in state (per-ctx). Only written once (lazily on first
                // invocation for this ctx). No two contexts share a state object, so there is no
                // cross-context key race.
                string ctxKey;
                if (state.CachedCtxKey != null)
                {
                    ctxKey = state.CachedCtxKey;
                }
                else
                {
                    var tenantId = ctx.Options.TenantProvider?.GetCurrentTenantId();
                    // Use GetFilterKey() instead of f.ToString().
                    // f.ToString() is shape-only — same string regardless of captured closure value.
                    // ExpressionFingerprint alone also misses closure values: it hashes the closure
                    // object reference (whose ToString() is the type name), not the actual field values.
                    // GetFilterKey() combines the shape fingerprint with a recursive extraction of all
                    // closure-accessed field values so two lambdas with the same shape but different
                    // captured values (e.g., tenantId=1 vs tenantId=2) produce distinct cache keys.
                    // Tenant segment: "TENANT:NULL:" when provider is set but returns null,
                    // vs empty string when no provider — prevents cross-tenant plan sharing.
                    // X1: Include the runtime type in the key so objects of different types
                    // that produce the same ToString() (e.g. int 1 vs string "1") yield
                    // distinct cache keys and cannot cross-pollinate compiled plans.
                    var tenantSegment = ctx.Options.TenantProvider != null
                        ? (tenantId == null
                            ? "TENANT:NULL:"
                            : string.Concat("TENANT:", tenantId.GetType().FullName, ":", tenantId, ":"))
                        : "";
                    ctxKey = string.Concat(
                        ctx.Provider.GetType().FullName, "|",
                        ctx.GetMappingHash().ToString(), "|",
                        tenantSegment, "|",
                        ctx.Options.GlobalFilters.Count > 0
                            ? string.Join(";", ctx.Options.GlobalFilters.SelectMany(kvp => kvp.Value.Select(GetFilterKey)))
                            : "");
                    state.CachedCtxKey = ctxKey; // per-ctx: no sharing, no race
                }

                // ── 3. Look up or build the query plan (thread-safe ConcurrentLruCache) ──────
                // Use GetOrAdd so evicted entries are recomputed on demand; concurrent misses
                // serialize inside GetOrAdd (factory called at most once per key).
                var capturedCtx = ctx;
                var capturedExpr = queryExpression;
                var invEntry = plansByCtx.GetOrAdd(ctxKey, __ =>
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

                // invEntry is a LOCAL variable — thread-safe for this invocation.
                var cachedPlan = invEntry.Plan;
                var paramNames = invEntry.ParamNames;

                // Reuse single-element array for the common single-param case
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

                // Inline pooled sync execution for providers without true async I/O (SQLite).
                // Bypasses the entire NormQueryProvider call chain (RetryPolicy, CacheProvider,
                // EnsureConnectionAsync, IsScalar dispatch) by inlining command reuse + sync read.
                // Pooled commands avoid per-call DbCommand/DbParameter allocation and SQL compilation.
                // Guard on Connection.State == Open — closed connection falls through to the
                // standard path which calls EnsureConnectionAsync before executing.
                // Interceptors guard removed — the fast path now routes through
                // ExecuteReaderWithInterception so interceptors are honoured.
                if (cachedPlan != null &&
                    ctx.Provider.PrefersSyncExecution &&
                    ctx.Options.RetryPolicy == null &&
                    ctx.Options.CacheProvider == null &&
                    ctx.Connection.State == ConnectionState.Open &&
                    !cachedPlan.IsScalar)
                {
                    // Q1 fix: dequeue a prepared command from the per-context pool, or create new
                    if (!state.CommandPool.TryDequeue(out var cmd))
                    {
                        cmd = ctx.CreateCommand();
                        cmd.CommandText = cachedPlan.Sql;
                        var fixedParams = invEntry.FixedParams;
                        int fixedCount = 0;
                        if (fixedParams != null)
                        {
                            for (int i = 0; i < fixedParams.Length; i++)
                            {
                                var p = cmd.CreateParameter();
                                p.ParameterName = fixedParams[i].Key;
                                // P1 fix: use AssignValue so DbType/Size/Precision are set
                                // correctly for enum, DateOnly, TimeOnly, Guid etc.
                                ParameterAssign.AssignValue(p, fixedParams[i].Value);
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
                                // P1 fix: use AssignValue so DbType/Size/Precision are set
                                // correctly for enum, DateOnly, TimeOnly, Guid etc.
                                ParameterAssign.AssignValue(p, kvp.Value);
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
                        try { cmd.Prepare(); } catch (DbException) { /* Prepare is a performance optimization; failure is non-fatal */ }
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

                    var materializer = cachedPlan.SyncMaterializer;
                    var capacity = cachedPlan.SingleResult ? 1 : (cachedPlan.Take ?? 16);
                    var list = new List<T>(capacity);

                    try
                    {
                        // Rebind transaction on every use — the transaction may have changed since
                        // the command was created or last dequeued from the pool.
                        cmd.Transaction = ctx.CurrentTransaction;
                        // Route through ExecuteReaderWithInterception so registered
                        // CommandInterceptors are invoked even on the pooled sync fast path.
                        using var reader = cmd.ExecuteReaderWithInterception(ctx, CommandBehavior.Default);
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
                    cachedPlan!, args, invEntry.FixedParams, state, default);
            };
        }

        /// <summary>
        /// Builds a cache key for a global filter expression that captures both
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
                    // X1: Include runtime type so objects of different types with the same
                    // ToString() (e.g. int vs string) produce distinct cache key segments.
                    sb.Append(val == null ? "null" : string.Concat(val.GetType().FullName, ":", val));
                    sb.Append(';');
                }
                catch (Exception ex) when (ex is MemberAccessException or TargetInvocationException or InvalidOperationException or NotSupportedException)
                {
                    // Reflection failure reading closure value — treat as stable (no value appended).
                    // Only catch expected reflection exceptions; let unexpected failures propagate.
                }
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
