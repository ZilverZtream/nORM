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

        private sealed class CompiledQueryContextState
        {
            public string? StableCtxKey;
            public CompiledQueryState? StableState;
            public readonly ConcurrentLruCache<string, CompiledQueryState> DynamicStates = new(PlanCacheCapacity);
        }

        private readonly struct CompiledParameterValueSource
        {
            private readonly Expression? _expression;
            private readonly MemberInfo? _queryMember;
            private readonly bool _useQueryValue;

            private CompiledParameterValueSource(Expression? expression, MemberInfo? queryMember, bool useQueryValue)
            {
                _expression = expression;
                _queryMember = queryMember;
                _useQueryValue = useQueryValue;
            }

            public static CompiledParameterValueSource FromExpression(Expression expression)
                => new(expression, null, false);

            public static CompiledParameterValueSource FromQueryValue()
                => new(null, null, true);

            public static CompiledParameterValueSource FromQueryMember(MemberInfo member)
                => new(null, member, false);

            public object? GetValue(object? queryValue)
            {
                if (_useQueryValue)
                    return queryValue;

                if (_queryMember != null)
                {
                    if (queryValue == null)
                        throw new InvalidOperationException(
                            $"Compiled query parameter object cannot be null when binding member '{_queryMember.Name}'.");

                    return _queryMember is FieldInfo fi ? fi.GetValue(queryValue) :
                           _queryMember is PropertyInfo pi ? pi.GetValue(queryValue) :
                           null;
                }

                if (_expression != null && QueryTranslator.TryGetConstantValue(_expression, out var value))
                    return value;

                return null;
            }
        }

        private sealed class CompiledParameterValueSourceCollector : ExpressionVisitor
        {
            private readonly ParameterExpression _queryParameter;
            private readonly int _expectedCount;
            private readonly List<CompiledParameterValueSource> _sources;

            public CompiledParameterValueSourceCollector(ParameterExpression queryParameter, int expectedCount)
            {
                _queryParameter = queryParameter;
                _expectedCount = expectedCount;
                _sources = new List<CompiledParameterValueSource>(expectedCount);
            }

            public CompiledParameterValueSource[] Collect(Expression expression)
            {
                Visit(expression);
                return _sources.ToArray();
            }

            protected override Expression VisitConstant(ConstantExpression node) => node;

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (_sources.Count < _expectedCount && ReferenceEquals(node, _queryParameter))
                {
                    _sources.Add(CompiledParameterValueSource.FromQueryValue());
                    return node;
                }

                return base.VisitParameter(node);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (_sources.Count >= _expectedCount)
                    return node;

                if (ReferenceEquals(node.Expression, _queryParameter))
                {
                    _sources.Add(CompiledParameterValueSource.FromQueryMember(node.Member));
                    return node;
                }

                if (QueryTranslator.TryGetConstantValue(node, out _))
                {
                    _sources.Add(CompiledParameterValueSource.FromExpression(node));
                    return node;
                }

                return base.VisitMember(node);
            }
        }

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
            ValidateNoTopLevelQueryBranch(queryExpression.Body);
            var timeout = ExpressionUtils.GetCompilationTimeout(queryExpression);
            using var cts = new CancellationTokenSource(timeout);
            return CompileWithTimeout<TContext, TParam, T>(queryExpression, cts.Token);
        }

        /// <summary>
        /// CompileQuery bakes a single SQL plan per delegate, so a top-level ternary that
        /// selects between two different query shapes (e.g. <c>asc ? q.OrderBy(k) : q.OrderByDescending(k)</c>)
        /// cannot be honored at call time -- the translator would either silently pick one
        /// branch or crash with a cryptic BCL <c>ArgumentException</c> when reconstructing
        /// the ConditionalExpression with rewritten branches. Surface a clear error pointing
        /// at the two-delegate workaround.
        /// </summary>
        private static void ValidateNoTopLevelQueryBranch(Expression body)
        {
            var e = body;
            while (e is UnaryExpression { NodeType: ExpressionType.Convert } ue) e = ue.Operand;
            if (e is ConditionalExpression ce
                && typeof(IQueryable).IsAssignableFrom(ce.IfTrue.Type)
                && typeof(IQueryable).IsAssignableFrom(ce.IfFalse.Type))
            {
                throw new NormUnsupportedFeatureException(
                    "CompileQuery does not support a top-level conditional (ternary) that selects between " +
                    "two different query shapes (e.g. `asc ? q.OrderBy(k) : q.OrderByDescending(k)`). The compiled " +
                    "SQL plan is decided once and cannot switch query structure at call time. " +
                    "Workaround: compile each branch into its own delegate and dispatch at the call site:\n" +
                    "    var asc  = Norm.CompileQuery((MyCtx ctx, int _) => ctx.Query<T>().OrderBy(k));\n" +
                    "    var desc = Norm.CompileQuery((MyCtx ctx, int _) => ctx.Query<T>().OrderByDescending(k));\n" +
                    "    var rows = sortAsc ? await asc(ctx, 0) : await desc(ctx, 0);");
            }
        }

        private static Func<TContext, TParam, Task<List<T>>> CompileWithTimeout<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression, CancellationToken token)
            where TContext : DbContext
            where T : class
        {
            _compileSemaphore.Wait();
            try
            {
                token.ThrowIfCancellationRequested();
                return CompileQueryInternal<TContext, TParam, T>(queryExpression);
            }
            finally
            {
                _compileSemaphore.Release();
            }
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
            var plansByCtx = new ConcurrentLruCache<string, (QueryPlan Plan, IReadOnlyList<string> ParamNames, HashSet<string> CompiledParamSet, KeyValuePair<string, object>[]? FixedParams, CompiledParameterValueSource[] ValueSources)>(PlanCacheCapacity);
            // Per-context pooled state via ConditionalWeakTable.
            // Each DbContext gets its own context-state container. Stable contexts use a single
            // cached command pool; tenant/global-filter contexts keep one command pool per computed
            // plan key so changed filter values cannot reuse commands prepared for an older plan.
            // ConditionalWeakTable provides automatic GC-tracked lifetime.
            var stateByCtx = new System.Runtime.CompilerServices.ConditionalWeakTable<DbContext, CompiledQueryContextState>();
            // Single-slot fast-path HINTS for the common case of repeated calls on the same ctx.
            // These vars are NOT synchronized — races cause a ConditionalWeakTable lookup (minor perf hit),
            // not a correctness failure, because correctness depends on the per-ctx context state.
            DbContext? fastCtxOwner = null;
            CompiledQueryContextState? fastCtxState = null;
            var expressionKeyCanChange = HasClosureValues(queryExpression.Body);

            return (ctx, value) =>
            {
                // ── 1. Resolve per-context state (thread-safe via ConditionalWeakTable) ──────
                // Fast-path: same ctx as last call → direct field read (unsynchronized hint).
                // Race consequence: stale miss → falls back to GetOrCreateValue (correct, O(1)).
                CompiledQueryContextState contextState;
                if (ReferenceEquals(fastCtxOwner, ctx) && fastCtxState != null)
                {
                    contextState = fastCtxState;
                }
                else
                {
                    contextState = stateByCtx.GetOrCreateValue(ctx);
                    fastCtxOwner = ctx;   // unsynchronized hint — safe (performance only)
                    fastCtxState = contextState; // unsynchronized hint — safe (performance only)
                }

                // ── 2. Get plan-cache key and matching command-pool state ───────────────────
                // Contexts without tenant/global filters keep the old single cached key/state for
                // the hot benchmark path. Contexts with dynamic filter inputs recompute the key on
                // every call and select a matching command pool, preventing stale tenant/filter
                // values and stale prepared commands when the provider value changes in-place.
                var expressionKey = expressionKeyCanChange ? GetFilterKey(queryExpression) : null;
                var (ctxKey, state) = ResolveContextPlanState(ctx, contextState, expressionKey);

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
                    var p = capturedCtx.GetQueryProvider().GetPlan(body, out var filtered, out _);
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
                    var valueSources = BuildCompiledParameterValueSources(
                        filtered, capturedExpr.Parameters[1], p.CompiledParameters.Count);
                    return (p, p.CompiledParameters, paramSet, fixedParams, valueSources);
                });

                // invEntry is a LOCAL variable — thread-safe for this invocation.
                var cachedPlan = invEntry.Plan;
                var paramNames = invEntry.ParamNames;

                var args = BuildCompiledParameterValues(invEntry.ValueSources, value, paramNames);

                // Inline pooled sync execution for providers whose compiled-query hot path is
                // faster synchronously.
                // Bypasses the entire NormQueryProvider call chain (RetryPolicy, CacheProvider,
                // EnsureConnectionAsync, IsScalar dispatch) by inlining command reuse + sync read.
                // Pooled commands avoid per-call DbCommand/DbParameter allocation and SQL compilation.
                // Guard on Connection.State == Open — closed connection falls through to the
                // standard path which calls EnsureConnectionAsync before executing.
                // Interceptors guard removed — the fast path now routes through
                // ExecuteReaderWithInterception so interceptors are honoured.
                if (cachedPlan != null &&
                    ctx.Provider.PrefersSyncCompiledQueryExecution &&
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
                            ParameterAssign.AssignValue(p, i < args.Length ? args[i] : DBNull.Value);
                            cmd.Parameters.Add(p);
                        }
                        ApplyPreparedParameterSizeHints(cmd);
                        try { cmd.Prepare(); } catch (Exception) { /* Prepare is a performance optimization; failure is non-fatal */ }
                        state.FixedParamCount = fixedCount;
                    }

                    // Update compiled parameter values (only these change per call)
                    // P1 fix: use AssignValue (not direct .Value) so DbType and Size are reset
                    // on null values — prevents stale metadata carry-over on reused parameters.
                    var compiledParams = cachedPlan.CompiledParameters;
                    var compiledCount = Math.Min(compiledParams.Count, args.Length);
                    var fixedParamCount = state.FixedParamCount;
                    for (int i = 0; i < compiledCount; i++)
                    {
                        ParameterAssign.AssignValue(cmd.Parameters[fixedParamCount + i], args[i]);
                        ApplyPreparedParameterSizeHint(cmd, cmd.Parameters[fixedParamCount + i]);
                    }

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

                    if (cachedPlan.PostReverse) Query.QueryExecutor.ReverseListInPlace(list);
                    if (cachedPlan.PostMaterializeTransform != null)
                    {
                        var transformed = cachedPlan.PostMaterializeTransform(list);
                        var rebuilt = new List<T>(transformed.Count);
                        foreach (var item in transformed) rebuilt.Add((T)item!);
                        list = rebuilt;
                    }
                    return Task.FromResult(list);
                }

                // Standard path for async providers or when advanced features are enabled
                return ctx.GetQueryProvider().ExecuteCompiledPooledAsync<List<T>>(
                    cachedPlan!, args, invEntry.FixedParams, state, default);
            };
        }

        private static void ApplyPreparedParameterSizeHints(DbCommand cmd)
        {
            foreach (DbParameter parameter in cmd.Parameters)
                ApplyPreparedParameterSizeHint(cmd, parameter);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static void ApplyPreparedParameterSizeHint(DbCommand cmd, DbParameter parameter)
        {
            if (parameter.DbType is DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength)
                parameter.Size = ParameterOptimizer.MaxInlineStringSize;
            else if (parameter.DbType == DbType.Binary)
                parameter.Size = -1;
            else if (cmd.GetType().FullName == "Microsoft.Data.SqlClient.SqlCommand" && parameter.Size == 0)
                parameter.Size = 1;
        }

        private static CompiledParameterValueSource[] BuildCompiledParameterValueSources(
            Expression filteredExpression, ParameterExpression queryParameter, int expectedCount)
        {
            if (expectedCount == 0)
                return Array.Empty<CompiledParameterValueSource>();

            return new CompiledParameterValueSourceCollector(queryParameter, expectedCount)
                .Collect(filteredExpression);
        }

        private static object?[] BuildCompiledParameterValues<TParam>(
            CompiledParameterValueSource[] valueSources, TParam value, IReadOnlyList<string>? paramNames)
        {
            if (paramNames == null || paramNames.Count == 0)
                return Array.Empty<object?>();

            if (valueSources.Length == paramNames.Count)
            {
                var values = new object?[valueSources.Length];
                object? boxed = value;
                for (int i = 0; i < valueSources.Length; i++)
                    values[i] = valueSources[i].GetValue(boxed);
                return values;
            }

            // Fallback for rare translator-produced parameters not represented by the expression
            // collector. This preserves the legacy explicit-argument behavior for unsupported shapes.
            if (value is System.Runtime.CompilerServices.ITuple tuple)
            {
                var arr = new object?[paramNames.Count];
                var count = Math.Min(tuple.Length, paramNames.Count);
                for (int i = 0; i < count; i++)
                    arr[i] = tuple[i];
                return arr;
            }

            if (paramNames.Count == 1)
                return new object?[] { (object?)value };

            throw new InvalidOperationException(
                $"Compiled query expects {paramNames.Count} parameters. " +
                "Pass values as a ValueTuple, e.g. (value1, value2).");
        }

        private static (string CtxKey, CompiledQueryState State) ResolveContextPlanState(
            DbContext ctx, CompiledQueryContextState contextState, string? expressionKey)
        {
            if (ctx.Options.TenantProvider == null && ctx.Options.GlobalFilters.Count == 0 && expressionKey == null)
            {
                var stableKey = contextState.StableCtxKey;
                var stableState = contextState.StableState;
                if (stableKey != null && stableState != null)
                    return (stableKey, stableState);

                lock (contextState)
                {
                    stableKey = contextState.StableCtxKey;
                    stableState = contextState.StableState;
                    if (stableKey == null || stableState == null)
                    {
                        stableKey = BuildContextPlanKey(ctx);
                        stableState = new CompiledQueryState();
                        contextState.StableCtxKey = stableKey;
                        contextState.StableState = stableState;
                    }

                    return (stableKey, stableState);
                }
            }

            var ctxKey = BuildContextPlanKey(ctx);
            if (expressionKey != null)
                ctxKey = string.Concat(ctxKey, "|EXPR:", expressionKey);
            return (ctxKey, contextState.DynamicStates.GetOrAdd(ctxKey, _ => new CompiledQueryState()));
        }

        private static string BuildContextPlanKey(DbContext ctx)
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

            return string.Concat(
                ctx.Provider.GetType().FullName, "|",
                ctx.GetMappingHash().ToString(), "|",
                tenantSegment, "|",
                ctx.Options.GlobalFilters.Count > 0
                    ? string.Join(";", ctx.Options.GlobalFilters.SelectMany(kvp => kvp.Value.Select(GetFilterKey)))
                    : "");
        }

        private static bool HasClosureValues(Expression expression)
        {
            var detector = new ClosureValueDetector();
            detector.Visit(expression);
            return detector.Found;
        }

        private sealed class ClosureValueDetector : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (Found)
                    return node;

                if (HasConstantRoot(node) && QueryTranslator.TryGetConstantValue(node, out _))
                {
                    Found = true;
                    return node;
                }

                return base.VisitMember(node);
            }
        }

        private static bool HasConstantRoot(MemberExpression node)
        {
            Expression? current = node.Expression;
            while (current is MemberExpression member)
                current = member.Expression;
            return current is ConstantExpression;
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
            if (expr is MemberExpression me && QueryTranslator.TryGetConstantValue(me, out var evaluated))
            {
                // Include runtime type so objects of different types with the same ToString()
                // (e.g. int vs string) produce distinct cache key segments. Evaluating the full
                // member expression covers common holder patterns such as tenant.CurrentId.
                AppendClosureValue(evaluated, sb);
                sb.Append(';');
                return;
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

        private static void AppendClosureValue(object? value, StringBuilder sb)
        {
            if (value == null)
            {
                sb.Append("null");
                return;
            }

            sb.Append(value.GetType().FullName).Append(':');
            if (value is System.Collections.IEnumerable enumerable &&
                value is not string &&
                value is not byte[] &&
                value is not IQueryable)
            {
                sb.Append('[');
                var first = true;
                foreach (var item in enumerable)
                {
                    if (!first) sb.Append(',');
                    AppendClosureValue(item, sb);
                    first = false;
                }
                sb.Append(']');
                return;
            }

            sb.Append(value);
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
