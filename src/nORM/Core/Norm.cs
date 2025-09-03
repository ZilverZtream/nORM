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
        {
            if (queryExpression == null) throw new ArgumentNullException(nameof(queryExpression));

            var ctxParameter = queryExpression.Parameters[0];
            var valueParameter = queryExpression.Parameters[1];

            QueryPlan? cachedPlan = null;
            string? paramName = null;

            return async (ctx, value) =>
            {
                if (cachedPlan == null)
                {
                    var body = new ParameterReplacer(ctxParameter, Expression.Constant(ctx)).Visit(queryExpression.Body)!;
                    body = new ParameterReplacer(valueParameter, Expression.Constant(value, typeof(TParam))).Visit(body)!;
                    var provider = new NormQueryProvider(ctx);
                    cachedPlan = provider.GetPlan(body, out _);
                    paramName = cachedPlan.Parameters.Keys.FirstOrDefault();
                }

                var parameters = cachedPlan.Parameters.ToDictionary(k => k.Key, v => v.Value);
                if (paramName != null)
                    parameters[paramName] = value!;

                var execProvider = new NormQueryProvider(ctx);
                return await execProvider.ExecuteCompiledAsync<List<T>>(cachedPlan, parameters, default);
            };
        }
    }
}

