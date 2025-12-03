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
            QueryPlan? cachedPlan = null;
            IReadOnlyList<string>? paramNames = null;

            return async (ctx, value) =>
            {
                if (cachedPlan == null)
                {
                    var ctxParam = queryExpression.Parameters[0];
                    var body = new ParameterReplacer(ctxParam, Expression.Constant(ctx)).Visit(queryExpression.Body)!;
                    body = new QueryCallEvaluator().Visit(body)!;
                    var provider = new NormQueryProvider(ctx);
                    cachedPlan = provider.GetPlan(body, out _);
                    paramNames = cachedPlan.CompiledParameters;
                }

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
                    else
                    {
                        // Single parameter case - map value to first argument
                        if (args.Length == 1)
                        {
                            args[0] = value;
                        }
                        else
                        {
                            // For multiple parameters without tuple, use value for all (rare case)
                            for (int i = 0; i < paramNames.Count; i++)
                            {
                                args[i] = value;
                            }
                        }
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
