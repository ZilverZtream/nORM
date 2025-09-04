using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace nORM.Core
{
    /// <summary>
    /// Extension methods to mark queries as cacheable.
    /// </summary>
    public static class CacheableExtensions
    {
        /// <summary>
        /// Marks a query's results to be cached.
        /// </summary>
        /// <param name="source">The source query.</param>
        /// <param name="absoluteExpiration">The absolute expiration time.</param>
        public static IQueryable<T> Cacheable<T>(this IQueryable<T> source, TimeSpan absoluteExpiration)
        {
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!)
                .MakeGenericMethod(typeof(T));

            var call = Expression.Call(null, method, source.Expression, Expression.Constant(absoluteExpiration));
            return source.Provider.CreateQuery<T>(call);
        }
    }
}
