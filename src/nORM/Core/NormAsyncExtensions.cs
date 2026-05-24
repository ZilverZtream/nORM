using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    /// <summary>
    /// High-performance extension methods for nORM async operations
    /// These are specifically designed for nORM queries to avoid conflicts with other ORMs
    /// </summary>
    public static class NormAsyncExtensions
    {
        /// <summary>
        /// Streams nORM query results asynchronously - only works with nORM queries
        /// </summary>
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                return normProvider.AsAsyncEnumerable<T>(source.Expression, ct);
            }

            throw new NormUsageException(
                "AsAsyncEnumerable extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>().");
        }

        /// <summary>
        /// Converts nORM query to List asynchronously - only works with nORM queries
        /// </summary>
        public static Task<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            // Only works with nORM provider - this avoids conflicts with EF Core
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                return normProvider.ExecuteAsync<List<T>>(source.Expression, ct);
            }

            throw new NormUsageException(
                "ToListAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToListAsync().");
        }

        /// <summary>
        /// Materializes an nORM query into a dictionary keyed by the supplied selector.
        /// Server executes the query once; the dictionary is built on the client side, which is
        /// the only place a CLR-only key selector can run.
        /// </summary>
        public static async Task<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
            this IQueryable<T> source,
            Func<T, TKey> keySelector,
            CancellationToken ct = default)
            where T : class
            where TKey : notnull
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            var list = await ToListAsync(source, ct).ConfigureAwait(false);
            var result = new Dictionary<TKey, T>(list.Count);
            foreach (var item in list)
                result.Add(keySelector(item), item);
            return result;
        }

        /// <summary>
        /// Materializes an nORM query into a dictionary keyed and valued by the supplied
        /// selectors. Server executes the query once; client materializes the dictionary.
        /// </summary>
        public static async Task<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(
            this IQueryable<T> source,
            Func<T, TKey> keySelector,
            Func<T, TValue> valueSelector,
            CancellationToken ct = default)
            where T : class
            where TKey : notnull
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(valueSelector);
            var list = await ToListAsync(source, ct).ConfigureAwait(false);
            var result = new Dictionary<TKey, TValue>(list.Count);
            foreach (var item in list)
                result.Add(keySelector(item), valueSelector(item));
            return result;
        }

        /// <summary>
        /// Materializes an nORM query into a <see cref="HashSet{T}"/>. Server executes the query
        /// once; the hash set is built on the client side using the default equality comparer.
        /// </summary>
        public static async Task<HashSet<T>> ToHashSetAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            var list = await ToListAsync(source, ct).ConfigureAwait(false);
            return new HashSet<T>(list);
        }

        /// <summary>
        /// Counts nORM query results asynchronously - only works with nORM queries
        /// </summary>
        public static Task<int> CountAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                // Try direct count path first — avoids Expression.Call + Type[] allocation
                // and re-parsing the Count expression in TryGetCountQuery.
                if (normProvider.TryDirectCountAsync(source.Expression, ct, out var directResult))
                    return directResult;

                // Fallback to expression-based path for complex predicates / global filters
                var countExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.Count),
                    new[] { typeof(T) },
                    source.Expression);
                return normProvider.ExecuteAsync<int>(countExpression, ct);
            }
            
            throw new NormUsageException(
                "CountAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.CountAsync().");
        }

        /// <summary>
        /// Predicate-overload of <see cref="CountAsync{T}(IQueryable{T}, CancellationToken)"/> -- parity with
        /// EF Core's <c>CountAsync(predicate)</c> spelling so users don't have to chain
        /// <c>.Where(predicate).CountAsync()</c>. Lowered to the same Where + Count pipeline
        /// internally so the e438a53 SelectMany+Count silent-wrongness fix applies uniformly.
        /// </summary>
        public static Task<int> CountAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(predicate);
            return CountAsync(source.Where(predicate), ct);
        }

        /// <summary>
        /// Counts rows in a nORM query asynchronously and returns a 64-bit total — use when the
        /// expected row count may exceed Int32.MaxValue.
        /// </summary>
        public static Task<long> LongCountAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var longCountExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.LongCount),
                    new[] { typeof(T) },
                    source.Expression);
                return normProvider.ExecuteAsync<long>(longCountExpression, ct);
            }

            throw new NormUsageException(
                "LongCountAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.LongCountAsync().");
        }

        /// <summary>
        /// Converts nORM query to List synchronously - only works with nORM queries
        /// </summary>
        public static List<T> ToListSync<T>(this IQueryable<T> source) where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                return normProvider.ExecuteSync<List<T>>(source.Expression);
            }

            throw new NormUsageException("ToListSync can only be used with nORM queries.");
        }

        /// <summary>
        /// Counts nORM query results synchronously - only works with nORM queries
        /// </summary>
        public static int CountSync<T>(this IQueryable<T> source) where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var countExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.Count),
                    new[] { typeof(T) },
                    source.Expression);
                return normProvider.ExecuteSync<int>(countExpression);
            }

            throw new NormUsageException("CountSync can only be used with nORM queries.");
        }

        /// <summary>
        /// Converts nORM query to Array asynchronously - only works with nORM queries  
        /// </summary>
        public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider)
            {
                var list = await source.ToListAsync(ct).ConfigureAwait(false);
                return list.ToArray();
            }
            
            throw new NormUsageException(
                "ToArrayAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToArrayAsync().");
        }

        /// <summary>
        /// Checks if nORM query has any results asynchronously - only works with nORM queries
        /// </summary>
        public static async Task<bool> AnyAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var anyExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.Any),
                    new[] { typeof(T) },
                    source.Expression);
                return await normProvider.ExecuteAsync<bool>(anyExpression, ct).ConfigureAwait(false);
            }
            
            throw new NormUsageException(
                "AnyAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AnyAsync().");
        }

        /// <summary>
        /// Predicate-overload of <see cref="AnyAsync{T}(IQueryable{T}, CancellationToken)"/> -- parity with
        /// EF Core's <c>AnyAsync(predicate)</c> spelling so users don't have to chain
        /// <c>.Where(predicate).AnyAsync()</c>. Lowered to the same Where + Any pipeline
        /// internally.
        /// </summary>
        public static Task<bool> AnyAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(predicate);
            return AnyAsync(source.Where(predicate), ct);
        }

        /// <summary>
        /// Gets first result from nORM query asynchronously - only works with nORM queries
        /// </summary>
        public static Task<T> FirstAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var firstExpression = Expression.Call(
                    typeof(Queryable), 
                    nameof(Queryable.First), 
                    new[] { typeof(T) }, 
                    source.Expression);
                return normProvider.ExecuteAsync<T>(firstExpression, ct);
            }
            
            throw new NormUsageException(
                "FirstAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.FirstAsync().");
        }

        /// <summary>
        /// Gets first result or default from nORM query asynchronously - only works with nORM queries
        /// </summary>
        public static Task<T?> FirstOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var firstOrDefaultExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.FirstOrDefault),
                    new[] { typeof(T) },
                    source.Expression);
                return normProvider.ExecuteAsync<T?>(firstOrDefaultExpression, ct);
            }

            throw new NormUsageException(
                "FirstOrDefaultAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.FirstOrDefaultAsync().");
        }

        /// <summary>
        /// Gets the last result from nORM query asynchronously. Requires an OrderBy to be
        /// deterministic; the underlying Last translator flips the ORDER BY direction so the
        /// SQL still reads only one row.
        /// </summary>
        public static Task<T> LastAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var lastExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.Last),
                    new[] { typeof(T) },
                    source.Expression);
                return normProvider.ExecuteAsync<T>(lastExpression, ct);
            }

            throw new NormUsageException(
                "LastAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.LastAsync().");
        }

        /// <summary>
        /// Gets the last result or default from nORM query asynchronously.
        /// </summary>
        public static Task<T?> LastOrDefaultAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var lastOrDefaultExpression = Expression.Call(
                    typeof(Queryable),
                    nameof(Queryable.LastOrDefault),
                    new[] { typeof(T) },
                    source.Expression);
                return normProvider.ExecuteAsync<T?>(lastOrDefaultExpression, ct);
            }

            throw new NormUsageException(
                "LastOrDefaultAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.LastOrDefaultAsync().");
        }

        /// <summary>
        /// Executes a bulk delete for the entities matching the query.
        /// </summary>
        /// <typeparam name="T">Type of the entity.</typeparam>
        /// <param name="source">Query identifying entities to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The number of affected rows.</returns>
        public static Task<int> ExecuteDeleteAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                return normProvider.ExecuteDeleteAsync(source.Expression, ct);
            }

            throw new NormUsageException(
                "ExecuteDeleteAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>().");
        }

        /// <summary>
        /// Executes a bulk update for entities matching the query using the provided property assignments.
        /// </summary>
        /// <typeparam name="T">Type of the entity.</typeparam>
        /// <param name="source">Query identifying entities to update.</param>
        /// <param name="set">Delegate specifying property assignments.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The number of affected rows.</returns>
        public static Task<int> ExecuteUpdateAsync<T>(this IQueryable<T> source, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default)
            where T : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                return normProvider.ExecuteUpdateAsync(source.Expression, set, ct);
            }

            throw new NormUsageException(
                "ExecuteUpdateAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>().");
        }

        // Join operations for nORM - these don't conflict since they return IQueryable
        /// <summary>
        /// Performs an inner join between two queryable sources.
        /// </summary>
        public static IQueryable<TResult> Join<TOuter, TInner, TKey, TResult>(
            this IQueryable<TOuter> outer,
            IQueryable<TInner> inner,
            Expression<Func<TOuter, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<TOuter, TInner, TResult>> resultSelector)
        {
            return Queryable.Join(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }

        /// <summary>
        /// Performs a group join between two queryable sources.
        /// </summary>
        public static IQueryable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(
            this IQueryable<TOuter> outer,
            IQueryable<TInner> inner,
            Expression<Func<TOuter, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<TOuter, IEnumerable<TInner>, TResult>> resultSelector)
        {
            return Queryable.GroupJoin(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }
    }
}
