using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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
                    var result = Expression.Lambda(node).Compile().DynamicInvoke();
                    return Expression.Constant(result, node.Type);
                }
                return base.VisitMethodCall(node);
            }
        }
    }
}

