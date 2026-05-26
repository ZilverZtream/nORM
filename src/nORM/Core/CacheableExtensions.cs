using System;
using System.Diagnostics.CodeAnalysis;
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
        // Pre-resolved open generic MethodInfo for Cacheable<T>. The static field is
        // populated once and reused across all calls. Using GetMethod avoids capturing
        // the current-method reflective call that triggers IL2026/IL2060/IL3050.
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(CacheableExtensions))]
        private static readonly MethodInfo _cacheableOpenMethod =
            typeof(CacheableExtensions).GetMethod(nameof(Cacheable), BindingFlags.Public | BindingFlags.Static)!;

        /// <summary>
        /// Marks a query's results to be cached.
        /// </summary>
        /// <remarks>
        /// This method builds a LINQ expression call using reflection and is not compatible
        /// with NativeAOT or aggressive trimming. Annotate consuming APIs accordingly.
        /// </remarks>
        /// <param name="source">The source query.</param>
        /// <param name="absoluteExpiration">The absolute expiration time.</param>
        [RequiresDynamicCode("Cacheable<T> calls MakeGenericMethod to construct a LINQ call expression; not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("Cacheable<T> uses reflection to resolve the open generic method; trimming may remove it.")]
        public static IQueryable<T> Cacheable<T>(this IQueryable<T> source, TimeSpan absoluteExpiration)
        {
            ArgumentNullException.ThrowIfNull(source);
            if (absoluteExpiration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(absoluteExpiration), absoluteExpiration, "Cache expiration must be a positive duration.");

            var method = _cacheableOpenMethod.MakeGenericMethod(typeof(T));
            var call = Expression.Call(null, method, source.Expression, Expression.Constant(absoluteExpiration));
            return source.Provider.CreateQuery<T>(call);
        }
    }
}
