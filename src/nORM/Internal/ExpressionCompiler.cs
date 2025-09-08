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
        private static readonly ConcurrentDictionary<int, Delegate> _compiledDelegateCache = new();

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

                var parameters = cachedPlan.Parameters.ToDictionary(k => k.Key, v => v.Value);
                if (paramNames != null)
                {
                    if (value is System.Runtime.CompilerServices.ITuple tuple)
                    {
                        var count = Math.Min(tuple.Length, paramNames.Count);
                        for (int i = 0; i < count; i++)
                            parameters[paramNames[i]] = tuple[i]!;
                    }
                    else
                    {
                        foreach (var name in paramNames)
                            parameters[name] = value!;
                    }
                }

                var execProvider = new NormQueryProvider(ctx);
                return await execProvider.ExecuteCompiledAsync<List<T>>(cachedPlan, parameters, default).ConfigureAwait(false);
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
