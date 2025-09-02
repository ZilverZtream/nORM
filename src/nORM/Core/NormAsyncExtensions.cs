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
        /// Converts nORM query to List asynchronously - only works with nORM queries
        /// </summary>
        public static Task<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            // Only works with nORM provider - this avoids conflicts with EF Core
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                return normProvider.ExecuteAsync<List<T>>(source.Expression, ct);
            }
            
            throw new InvalidOperationException(
                "ToListAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToListAsync().");
        }

        /// <summary>
        /// Counts nORM query results asynchronously - only works with nORM queries
        /// </summary>
        public static Task<int> CountAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var countExpression = Expression.Call(
                    typeof(Queryable), 
                    nameof(Queryable.Count), 
                    new[] { typeof(T) }, 
                    source.Expression);
                return normProvider.ExecuteAsync<int>(countExpression, ct);
            }
            
            throw new InvalidOperationException(
                "CountAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.CountAsync().");
        }

        /// <summary>
        /// Converts nORM query to Array asynchronously - only works with nORM queries  
        /// </summary>
        public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            if (source.Provider is Query.NormQueryProvider)
            {
                var list = await source.ToListAsync(ct);
                return list.ToArray();
            }
            
            throw new InvalidOperationException(
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
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var anyExpression = Expression.Call(
                    typeof(Queryable), 
                    nameof(Queryable.Any), 
                    new[] { typeof(T) }, 
                    source.Expression);
                var result = await normProvider.ExecuteAsync<int>(anyExpression, ct);
                return result > 0;
            }
            
            throw new InvalidOperationException(
                "AnyAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AnyAsync().");
        }

        /// <summary>
        /// Gets first result from nORM query asynchronously - only works with nORM queries
        /// </summary>
        public static Task<T> FirstAsync<T>(this IQueryable<T> source, CancellationToken ct = default)
            where T : class
        {
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var firstExpression = Expression.Call(
                    typeof(Queryable), 
                    nameof(Queryable.First), 
                    new[] { typeof(T) }, 
                    source.Expression);
                return normProvider.ExecuteAsync<T>(firstExpression, ct);
            }
            
            throw new InvalidOperationException(
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
            if (source.Provider is Query.NormQueryProvider normProvider)
            {
                var firstOrDefaultExpression = Expression.Call(
                    typeof(Queryable), 
                    nameof(Queryable.FirstOrDefault), 
                    new[] { typeof(T) }, 
                    source.Expression);
                return normProvider.ExecuteAsync<T?>(firstOrDefaultExpression, ct);
            }
            
            throw new InvalidOperationException(
                "FirstOrDefaultAsync extension can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.FirstOrDefaultAsync().");
        }
        
        // Join operations for nORM - these don't conflict since they return IQueryable
        public static IQueryable<TResult> Join<TOuter, TInner, TKey, TResult>(
            this IQueryable<TOuter> outer,
            IQueryable<TInner> inner,
            Expression<Func<TOuter, TKey>> outerKeySelector,
            Expression<Func<TInner, TKey>> innerKeySelector,
            Expression<Func<TOuter, TInner, TResult>> resultSelector)
        {
            return Queryable.Join(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }
        
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
