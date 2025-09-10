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
            Provider = new NormQueryProvider(ctx);
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
    }
}

