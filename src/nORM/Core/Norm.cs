using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using nORM.Internal;

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

            return ExpressionCompiler.CompileQuery<TContext, TParam, T>(queryExpression);
        }
    }
}

