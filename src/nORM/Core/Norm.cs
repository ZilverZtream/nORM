using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Query;

#nullable enable

namespace nORM.Core
{
    public static class Norm
    {
        public static Func<TContext, TParam, Task<List<T>>> CompileQuery<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression)
            where TContext : DbContext
            where T : class
        {
            if (queryExpression == null) throw new ArgumentNullException(nameof(queryExpression));

            ExpressionUtils.ValidateExpression(queryExpression);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
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
                    foreach (var name in paramNames)
                        parameters[name] = value!;

                var execProvider = new NormQueryProvider(ctx);
                return await execProvider.ExecuteCompiledAsync<List<T>>(cachedPlan, parameters, default).ConfigureAwait(false);
            };
        }

        private sealed class QueryCallEvaluator : ExpressionVisitor
        {
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.DeclaringType == typeof(NormQueryable) && node.Method.Name == "Query")
                {
                    ExpressionUtils.ValidateExpression(node);
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    var del = ExpressionUtils.CompileWithTimeout(Expression.Lambda(node), cts.Token);
                    var result = del.DynamicInvoke();
                    return Expression.Constant(result, node.Type);
                }
                return base.VisitMethodCall(node);
            }
        }
    }
}

