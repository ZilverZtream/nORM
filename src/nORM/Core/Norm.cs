using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
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

            var compiled = queryExpression.Compile();
            QueryPlan? cachedPlan = null;
            string? paramName = null;

            return async (ctx, value) =>
            {
                if (cachedPlan == null)
                {
                    var query = compiled(ctx, value);
                    var provider = new NormQueryProvider(ctx);
                    cachedPlan = provider.GetPlan(query.Expression, out _);
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

