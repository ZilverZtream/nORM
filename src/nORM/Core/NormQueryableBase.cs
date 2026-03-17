using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Query;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Provides shared queryable behavior for nORM query implementations.
    /// </summary>
    internal abstract class NormQueryableBase<T> : IOrderedQueryable<T>
    {
        public Expression Expression { get; }
        public Type ElementType => typeof(T);
        public IQueryProvider Provider { get; }

        protected NormQueryableBase(DbContext ctx)
        {
            Expression = Expression.Constant(this);
            // Reuse cached NormQueryProvider instead of creating new one per Query<T>() call.
            // Saves 4 heap allocations (provider, executor, include processor, CUD builder).
            Provider = ctx.GetQueryProvider();
        }

        protected NormQueryableBase(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        /// <summary>
        /// Executes the expression tree and returns an enumerator for the resulting sequence.
        /// </summary>
        /// <returns>An enumerator that iterates through the query results.</returns>
        public IEnumerator<T> GetEnumerator() => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Returns the SQL that would be generated for this query without executing it.
        /// Useful for debugging and tests that inspect query shape.
        /// </summary>
        public override string ToString()
        {
            if (Provider is NormQueryProvider qp)
            {
                try
                {
                    var plan = qp.GetPlan(Expression, out _, out _);
                    return plan.Sql;
                }
                catch (NormQueryException)
                {
                    throw; // Deliberate validation failures (e.g. invalid JSON path) must propagate.
                }
                catch
                {
                    // Fall through to default if translation fails (e.g. unsupported expression).
                }
            }
            return base.ToString() ?? string.Empty;
        }
    }
}

