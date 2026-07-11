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
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Compiled queries translate expression trees and build delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Compiled queries reflect over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public static Func<TContext, TParam, Task<List<T>>> CompileQuery<TContext, TParam, T>(Expression<Func<TContext, TParam, IQueryable<T>>> queryExpression)
            where TContext : DbContext
            where T : class
        {
            if (queryExpression == null) throw new ArgumentNullException(nameof(queryExpression));

            return ExpressionCompiler.CompileQuery<TContext, TParam, T>(queryExpression);
        }

        /// <summary>
        /// Compiles a query whose body ends in a terminal operator (<c>First</c>, <c>Single</c>,
        /// <c>Count</c>, <c>Any</c>, <c>Sum</c>, <c>Min</c>, <c>Max</c>, <c>Average</c>, and their
        /// <c>OrDefault</c>/<c>Long</c> variants) into a reusable delegate returning the terminal
        /// result. Terminal semantics match the non-compiled runtime exactly: <c>First</c> on an
        /// empty sequence throws <see cref="InvalidOperationException"/>, <c>Single</c> enforces
        /// cardinality, and <c>FirstOrDefault</c> returns <c>null</c>/<c>default</c>.
        /// </summary>
        /// <typeparam name="TContext">Type of the <see cref="DbContext"/>.</typeparam>
        /// <typeparam name="TParam">Type of the parameter passed to the query.</typeparam>
        /// <typeparam name="TResult">Result type produced by the terminal operator.</typeparam>
        /// <param name="queryExpression">Expression describing the query to compile, ending in a terminal operator.</param>
        /// <returns>A delegate that executes the query and returns the terminal result.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Compiled queries translate expression trees and build delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Compiled queries reflect over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public static Func<TContext, TParam, Task<TResult>> CompileTerminalQuery<TContext, TParam, TResult>(
            Expression<Func<TContext, TParam, TResult>> queryExpression)
            where TContext : DbContext
        {
            if (queryExpression == null) throw new ArgumentNullException(nameof(queryExpression));

            return ExpressionCompiler.CompileTerminalQuery<TContext, TParam, TResult>(queryExpression);
        }
    }
}

