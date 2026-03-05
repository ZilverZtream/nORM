using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Query;

#nullable enable

namespace nORM.Internal
{
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
            // Using a string key avoids the 32-bit HashCode.Combine collisions that occur when
            // two filter lambdas differ only by a constant value (e.g. TenantKey==1 vs TenantKey==2).
            var plansByCtx = new ConcurrentDictionary<string, (QueryPlan Plan, IReadOnlyList<string> ParamNames)>();

            return async (ctx, value) =>
            {
                // Build a deterministic, collision-resistant cache key from all query-shape dimensions.
                var keyBuilder = new System.Text.StringBuilder();
                keyBuilder.Append(ctx.Provider.GetType().FullName);
                keyBuilder.Append('|');
                foreach (var m in ctx.GetAllMappings())
                {
                    keyBuilder.Append(m.TableName);
                    keyBuilder.Append(',');
                    keyBuilder.Append(m.Type.FullName);
                    keyBuilder.Append(';');
                }
                keyBuilder.Append('|');
                // QP-1/SEC-1: Include tenant dimension so plans are not reused across tenant contexts.
                var tenantId = ctx.Options.TenantProvider?.GetCurrentTenantId();
                keyBuilder.Append(tenantId?.ToString() ?? "");
                keyBuilder.Append('|');
                // QP-1/SEC-1: Include global filter expressions (their ToString() embeds constant values)
                // so contexts with different filters always get distinct plan cache entries.
                if (ctx.Options.GlobalFilters.Count > 0)
                {
                    foreach (var kvp in ctx.Options.GlobalFilters)
                        foreach (var filterExpr in kvp.Value)
                        {
                            keyBuilder.Append(filterExpr.ToString());
                            keyBuilder.Append(';');
                        }
                }
                string ctxKey = keyBuilder.ToString();


                if (!plansByCtx.TryGetValue(ctxKey, out var entry))
                {
                    var ctxParam = queryExpression.Parameters[0];
                    var body = new ParameterReplacer(ctxParam, Expression.Constant(ctx)).Visit(queryExpression.Body)!;
                    body = new QueryCallEvaluator().Visit(body)!;
                    var p = new NormQueryProvider(ctx).GetPlan(body, out _);
                    entry = (p, p.CompiledParameters);
                    plansByCtx[ctxKey] = entry;
                }

                var cachedPlan = entry.Plan;
                var paramNames = entry.ParamNames;

                // PERFORMANCE FIX: Use array instead of dictionary to avoid allocation
                object?[] args;

                if (paramNames != null && paramNames.Count > 0)
                {
                    args = new object?[paramNames.Count];
                    if (value is System.Runtime.CompilerServices.ITuple tuple)
                    {
                        var count = Math.Min(tuple.Length, paramNames.Count);
                        for (int i = 0; i < count; i++)
                            args[i] = tuple[i];
                    }
                    else if (args.Length == 1)
                    {
                        // Single parameter case - map value to first argument
                        args[0] = value;
                    }
                    else
                    {
                        // PC-1: Multiple parameters require a ValueTuple; single non-tuple is ambiguous.
                        throw new InvalidOperationException(
                            $"Compiled query expects {paramNames.Count} parameters. " +
                            "Pass values as a ValueTuple, e.g. (value1, value2).");
                    }
                }
                else
                {
                    args = Array.Empty<object?>();
                }

                var execProvider = new NormQueryProvider(ctx);
                // Call the optimized array overload
                return await execProvider.ExecuteCompiledAsync<List<T>>(cachedPlan, args, default).ConfigureAwait(false);
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
