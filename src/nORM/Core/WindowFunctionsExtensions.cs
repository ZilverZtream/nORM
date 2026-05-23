using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace nORM.Core
{
    /// <summary>
    /// Provides LINQ extension methods for SQL window functions.
    /// </summary>
    [RequiresDynamicCode("nORM window functions emit MakeGenericMethod-built expressions; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [RequiresUnreferencedCode("nORM window functions reflect over LINQ method metadata; trimming may remove the required members.")]
    public static class WindowFunctionsExtensions
    {
        /// <summary>
        /// Adds a ROW_NUMBER() column to the query projection.
        /// </summary>
        public static IQueryable<TResult> WithRowNumber<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, TResult>> resultSelector)
            where TSource : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(resultSelector);
            if (source.Provider is Query.NormQueryProvider provider)
            {
                var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(TSource), typeof(TResult));
                var call = Expression.Call(null, method, source.Expression, Expression.Quote(resultSelector));
                return provider.CreateQuery<TResult>(call);
            }

            throw new NormUsageException("WithRowNumber extension can only be used with nORM queries.");
        }

        /// <summary>
        /// Adds a RANK() column to the query projection.
        /// </summary>
        public static IQueryable<TResult> WithRank<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, TResult>> resultSelector)
            where TSource : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(resultSelector);
            if (source.Provider is Query.NormQueryProvider provider)
            {
                var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(TSource), typeof(TResult));
                var call = Expression.Call(null, method, source.Expression, Expression.Quote(resultSelector));
                return provider.CreateQuery<TResult>(call);
            }

            throw new NormUsageException("WithRank extension can only be used with nORM queries.");
        }

        /// <summary>
        /// Adds a DENSE_RANK() column to the query projection.
        /// </summary>
        public static IQueryable<TResult> WithDenseRank<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, TResult>> resultSelector)
            where TSource : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(resultSelector);
            if (source.Provider is Query.NormQueryProvider provider)
            {
                var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(TSource), typeof(TResult));
                var call = Expression.Call(null, method, source.Expression, Expression.Quote(resultSelector));
                return provider.CreateQuery<TResult>(call);
            }

            throw new NormUsageException("WithDenseRank extension can only be used with nORM queries.");
        }

        /// <summary>
        /// Adds a LAG() column to the query projection.
        /// </summary>
        public static IQueryable<TResult> WithLag<TSource, TValue, TResult>(
            this IQueryable<TSource> source,
            Expression<Func<TSource, TValue>> valueSelector,
            int offset,
            Expression<Func<TSource, TValue?, TResult>> resultSelector,
            Expression<Func<TSource, TValue>>? defaultValue = null)
            where TSource : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(valueSelector);
            ArgumentNullException.ThrowIfNull(resultSelector);
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), offset, "LAG offset must be non-negative.");
            if (source.Provider is Query.NormQueryProvider provider)
            {
                var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(TSource), typeof(TValue), typeof(TResult));
                var call = Expression.Call(
                    null,
                    method,
                    source.Expression,
                    Expression.Quote(valueSelector),
                    Expression.Constant(offset),
                    Expression.Quote(resultSelector),
                    defaultValue != null ? Expression.Quote(defaultValue) : Expression.Constant(null, typeof(Expression<Func<TSource, TValue>>))
                );
                return provider.CreateQuery<TResult>(call);
            }

            throw new NormUsageException("WithLag extension can only be used with nORM queries.");
        }

        /// <summary>
        /// Adds a LEAD() column to the query projection.
        /// </summary>
        public static IQueryable<TResult> WithLead<TSource, TValue, TResult>(
            this IQueryable<TSource> source,
            Expression<Func<TSource, TValue>> valueSelector,
            int offset,
            Expression<Func<TSource, TValue?, TResult>> resultSelector,
            Expression<Func<TSource, TValue>>? defaultValue = null)
            where TSource : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(valueSelector);
            ArgumentNullException.ThrowIfNull(resultSelector);
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), offset, "LEAD offset must be non-negative.");
            if (source.Provider is Query.NormQueryProvider provider)
            {
                var method = ((MethodInfo)MethodBase.GetCurrentMethod()!).MakeGenericMethod(typeof(TSource), typeof(TValue), typeof(TResult));
                var call = Expression.Call(
                    null,
                    method,
                    source.Expression,
                    Expression.Quote(valueSelector),
                    Expression.Constant(offset),
                    Expression.Quote(resultSelector),
                    defaultValue != null ? Expression.Quote(defaultValue) : Expression.Constant(null, typeof(Expression<Func<TSource, TValue>>))
                );
                return provider.CreateQuery<TResult>(call);
            }

            throw new NormUsageException("WithLead extension can only be used with nORM queries.");
        }
    }
}
