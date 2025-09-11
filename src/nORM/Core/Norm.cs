using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using nORM.Internal;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Provides helpers for compiling query expressions into high-performance delegates.
    /// </summary>
    public static class Norm
    {
        /// <summary>
        /// Compiles the supplied query expression into a reusable delegate that executes the query asynchronously.
        /// </summary>
        /// <typeparam name="TContext">Type of the <see cref="DbContext"/>.</typeparam>
        /// <typeparam name="TParam">Type of the parameter passed to the query.</typeparam>
        /// <typeparam name="T">Element type returned by the query.</typeparam>
        /// <param name="queryExpression">Expression describing the query to compile.</param>
        /// <returns>A delegate that executes the query and returns the results as a <see cref="List{T}"/>.</returns>
        public static Func<TContext, TParam, Task<List<T>>> CompileQuery<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression)
            where TContext : DbContext
            where T : class
        {
            if (queryExpression == null) throw new ArgumentNullException(nameof(queryExpression));

            return ExpressionCompiler.CompileQuery<TContext, TParam, T>(queryExpression);
        }
    }
}

