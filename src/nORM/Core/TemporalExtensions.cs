using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace nORM.Core
{
    /// <summary>
    /// Provides helper methods for querying temporal tables at specific points in time.
    /// </summary>
    [RequiresDynamicCode("nORM temporal extensions emit reflection-built LINQ expressions; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [RequiresUnreferencedCode("nORM temporal extensions reflect over LINQ method metadata; trimming may remove the required members.")]
    public static class TemporalExtensions
    {
        /// <summary>
        /// Creates a query that returns entity states as of the specified timestamp.
        /// </summary>
        /// <typeparam name="T">Type of the entity.</typeparam>
        /// <param name="source">Queryable source representing a temporal table.</param>
        /// <param name="timestamp">Point in time to query.</param>
        /// <returns>A queryable that yields entity values at the given timestamp.</returns>
        public static IQueryable<T> AsOf<T>(this IQueryable<T> source, DateTime timestamp)
        {
            ArgumentNullException.ThrowIfNull(source);
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(timestamp));
            return source.Provider.CreateQuery<T>(call);
        }

        /// <summary>
        /// Creates a query that uses a temporal history table tag to filter results.
        /// </summary>
        /// <typeparam name="T">Type of the entity.</typeparam>
        /// <param name="source">Queryable source representing a temporal table.</param>
        /// <param name="tagName">Name of the history tag to apply.</param>
        /// <returns>A queryable filtered by the specified temporal tag.</returns>
        public static IQueryable<T> AsOf<T>(this IQueryable<T> source, string tagName)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentException.ThrowIfNullOrWhiteSpace(tagName);
            var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(tagName));
            return source.Provider.CreateQuery<T>(call);
        }
    }
}
