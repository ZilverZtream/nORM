using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Query;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Provides the entry point to query the database using LINQ.
    /// </summary>
    public static class NormQueryable
    {
        /// <summary>
        /// Creates a queryable source for the specified entity type backed by the provided context.
        /// </summary>
        /// <typeparam name="T">The entity type to query.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> that provides access to the database.</param>
        /// <returns>An <see cref="IQueryable{T}"/> to compose and execute queries.</returns>
        public static IQueryable<T> Query<T>(this DbContext ctx) where T : class
            => typeof(T).GetConstructor(Type.EmptyTypes) != null
                ? (IQueryable<T>)Activator.CreateInstance(typeof(NormQueryableImpl<>).MakeGenericType(typeof(T)), ctx)!
                : new NormQueryableImplUnconstrained<T>(ctx);
    }

    /// <summary>
    /// Defines a queryable data source with extended functionality like Include.
    /// </summary>
    /// <summary>
    /// Defines a queryable data source with extended functionality like eager-loading and
    /// asynchronous evaluation.
    /// </summary>
    /// <typeparam name="T">The entity type returned by the query.</typeparam>
    public interface INormQueryable<T> : IOrderedQueryable<T>
    {
        /// <summary>
        /// Specifies a navigation property to include in the query results.
        /// </summary>
        /// <typeparam name="TProperty">Type of the navigation property.</typeparam>
        /// <param name="navigationPropertyPath">Expression identifying the navigation to include.</param>
        INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> navigationPropertyPath);

        /// <summary>Disables change tracking for the query results.</summary>
        INormQueryable<T> AsNoTracking();

        /// <summary>
        /// Splits collection navigations into multiple queries to reduce result set inflation.
        /// </summary>
        INormQueryable<T> AsSplitQuery();

        /// <summary>Executes the query as an asynchronous stream of entities.</summary>
        IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default);

        /// <summary>Asynchronously materializes the query results into a list.</summary>
        Task<List<T>> ToListAsync(CancellationToken ct = default);

        /// <summary>Asynchronously materializes the query results into an array.</summary>
        Task<T[]> ToArrayAsync(CancellationToken ct = default);

        /// <summary>Asynchronously counts the number of results.</summary>
        Task<int> CountAsync(CancellationToken ct = default);

        /// <summary>Determines asynchronously whether any results exist.</summary>
        Task<bool> AnyAsync(CancellationToken ct = default);

        /// <summary>Returns the first element of the sequence, throwing if none exists.</summary>
        Task<T> FirstAsync(CancellationToken ct = default);

        /// <summary>Returns the first element of the sequence or <c>null</c> if none exists.</summary>
        Task<T?> FirstOrDefaultAsync(CancellationToken ct = default);

        /// <summary>Returns the single element of the sequence, throwing if the sequence does not contain exactly one element.</summary>
        Task<T> SingleAsync(CancellationToken ct = default);

        /// <summary>Returns the single element of the sequence or <c>null</c> if no elements exist.</summary>
        Task<T?> SingleOrDefaultAsync(CancellationToken ct = default);

        /// <summary>Executes a bulk delete based on the query.</summary>
        Task<int> ExecuteDeleteAsync(CancellationToken ct = default);

        /// <summary>Executes a bulk update using the provided property setter expression.</summary>
        Task<int> ExecuteUpdateAsync(Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default);
    }

    /// <summary>
    /// Represents a queryable that supports inclusion of navigation properties.
    /// </summary>
    public interface INormIncludableQueryable<TEntity, out TProperty> : INormQueryable<TEntity>
    {
    }

    // Base interface for internal use without constraints
    internal interface INormQueryableInternal<T> : IOrderedQueryable<T>
    {
        // No async methods here to avoid constraint conflicts
    }

    internal sealed class NormQueryableImpl<T> : NormQueryableBase<T>, INormQueryable<T> where T : class, new()
    {
        public NormQueryableImpl(DbContext ctx) : base(ctx)
        {
        }

        public NormQueryableImpl(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }

        public INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> path)
        {
            var method = typeof(INormQueryable<>).MakeGenericType(typeof(T))
                .GetMethods()
                .Single(m => m.Name == nameof(Include) && m.IsGenericMethod);
            var generic = method.MakeGenericMethod(typeof(TProperty));
            var expression = Expression.Call(Expression, generic, Expression.Quote(path));
            return new NormIncludableQueryable<T, TProperty>(Provider, expression);
        }

        /// <summary>
        /// Returns a new query that will not track the resulting entities in the <see cref="DbContext"/>.
        /// </summary>
        /// <returns>An untracked query.</returns>
        public INormQueryable<T> AsNoTracking()
        {
            var expression = Expression.Call(
                typeof(INormQueryable<>).MakeGenericType(typeof(T)),
                nameof(AsNoTracking),
                Type.EmptyTypes,
                Expression
            );
            return new NormQueryableImpl<T>(Provider, expression);
        }

        /// <summary>
        /// Configures the query to execute related collection loads as separate database queries.
        /// </summary>
        /// <returns>A query configured for split query execution.</returns>
        public INormQueryable<T> AsSplitQuery()
        {
            var expression = Expression.Call(
                typeof(INormQueryable<>).MakeGenericType(typeof(T)),
                nameof(AsSplitQuery),
                Type.EmptyTypes,
                Expression
            );
            return new NormQueryableImpl<T>(Provider, expression);
        }

        /// <summary>
        /// Executes the query and exposes the results as an asynchronous stream.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous iteration.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> representing the query results.</returns>
        public IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).AsAsyncEnumerable<T>(Expression, ct);

        /// <summary>
        /// Executes the query and materializes the results into a <see cref="List{T}"/> asynchronously.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task containing the resulting list of entities.</returns>
        public Task<List<T>> ToListAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<List<T>>(Expression, ct);
        public async Task<T[]> ToArrayAsync(CancellationToken ct = default) => (await ToListAsync(ct).ConfigureAwait(false)).ToArray();
        public Task<int> CountAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<int>(Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(T) }, Expression), ct);
        public async Task<bool> AnyAsync(CancellationToken ct = default) => await ((NormQueryProvider)Provider).ExecuteAsync<bool>(Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(T) }, Expression), ct).ConfigureAwait(false);
        public Task<T> FirstAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T>(Expression.Call(typeof(Queryable), nameof(Queryable.First), new[] { typeof(T) }, Expression), ct);
        public Task<T?> FirstOrDefaultAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T?>(Expression.Call(typeof(Queryable), nameof(Queryable.FirstOrDefault), new[] { typeof(T) }, Expression), ct);
        public Task<T> SingleAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T>(Expression.Call(typeof(Queryable), nameof(Queryable.Single), new[] { typeof(T) }, Expression), ct);
        public Task<T?> SingleOrDefaultAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T?>(Expression.Call(typeof(Queryable), nameof(Queryable.SingleOrDefault), new[] { typeof(T) }, Expression), ct);
        public Task<int> ExecuteDeleteAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteDeleteAsync(Expression, ct);
        public Task<int> ExecuteUpdateAsync(Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteUpdateAsync(Expression, set, ct);
    }

    /// <summary>
    /// Internal queryable implementation without constraints, used for intermediate query operations
    /// like Join that produce anonymous types or other types without parameterless constructors.
    /// </summary>
    internal sealed class NormQueryableImplUnconstrained<T> : NormQueryableBase<T>, INormQueryableInternal<T>
    {
        public NormQueryableImplUnconstrained(DbContext ctx) : base(ctx)
        {
        }

        public NormQueryableImplUnconstrained(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }
    }

    internal sealed class NormIncludableQueryable<T, TProperty> : NormQueryableBase<T>, INormIncludableQueryable<T, TProperty> where T : class, new()
    {
        public NormIncludableQueryable(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }

        public INormIncludableQueryable<T, TProperty2> Include<TProperty2>(Expression<Func<T, TProperty2>> path)
        {
            var method = typeof(INormQueryable<>).MakeGenericType(typeof(T))
                .GetMethods()
                .Single(m => m.Name == nameof(INormQueryable<T>.Include) && m.IsGenericMethod);
            var generic = method.MakeGenericMethod(typeof(TProperty2));
            var expression = Expression.Call(Expression, generic, Expression.Quote(path));
            return new NormIncludableQueryable<T, TProperty2>(Provider, expression);
        }

        /// <summary>
        /// Returns a new query that will not track the resulting entities in the <see cref="DbContext"/>.
        /// </summary>
        /// <returns>An untracked query.</returns>
        public INormQueryable<T> AsNoTracking()
        {
            var expression = Expression.Call(
                typeof(INormQueryable<>).MakeGenericType(typeof(T)),
                nameof(INormQueryable<T>.AsNoTracking),
                Type.EmptyTypes,
                Expression
            );
            return new NormQueryableImpl<T>(Provider, expression);
        }

        /// <summary>
        /// Configures the query to execute related collection loads as separate database queries.
        /// </summary>
        /// <returns>A query configured for split query execution.</returns>
        public INormQueryable<T> AsSplitQuery()
        {
            var expression = Expression.Call(
                typeof(INormQueryable<>).MakeGenericType(typeof(T)),
                nameof(INormQueryable<T>.AsSplitQuery),
                Type.EmptyTypes,
                Expression
            );
            return new NormQueryableImpl<T>(Provider, expression);
        }

        /// <summary>
        /// Executes the query and exposes the results as an asynchronous stream.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous iteration.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> representing the query results.</returns>
        public IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).AsAsyncEnumerable<T>(Expression, ct);

        public Task<List<T>> ToListAsync(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteAsync<List<T>>(Expression, ct);
        public async Task<T[]> ToArrayAsync(CancellationToken ct = default) => (await ToListAsync(ct).ConfigureAwait(false)).ToArray();
        public Task<int> CountAsync(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteAsync<int>(Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(T) }, Expression), ct);
        public async Task<bool> AnyAsync(CancellationToken ct = default)
            => await ((NormQueryProvider)Provider).ExecuteAsync<bool>(Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(T) }, Expression), ct).ConfigureAwait(false);
        public Task<T> FirstAsync(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteAsync<T>(Expression.Call(typeof(Queryable), nameof(Queryable.First), new[] { typeof(T) }, Expression), ct);
        public Task<T?> FirstOrDefaultAsync(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteAsync<T?>(Expression.Call(typeof(Queryable), nameof(Queryable.FirstOrDefault), new[] { typeof(T) }, Expression), ct);
        public Task<T> SingleAsync(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteAsync<T>(Expression.Call(typeof(Queryable), nameof(Queryable.Single), new[] { typeof(T) }, Expression), ct);
        public Task<T?> SingleOrDefaultAsync(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteAsync<T?>(Expression.Call(typeof(Queryable), nameof(Queryable.SingleOrDefault), new[] { typeof(T) }, Expression), ct);
        public Task<int> ExecuteDeleteAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteDeleteAsync(Expression, ct);
        public Task<int> ExecuteUpdateAsync(Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default)
            => ((NormQueryProvider)Provider).ExecuteUpdateAsync(Expression, set, ct);
    }

    /// <summary>
    /// Extension methods for chaining <c>Include</c> calls on queryables that already include
    /// a navigation property.
    /// </summary>
    public static class NormIncludableQueryableExtensions
    {
        /// <summary>
        /// Specifies an additional navigation property to include after a previous <c>Include</c> call.
        /// </summary>
        /// <typeparam name="TEntity">Root entity type of the query.</typeparam>
        /// <typeparam name="TPreviousProperty">Type of the previously included navigation.</typeparam>
        /// <typeparam name="TProperty">Type of the navigation to include.</typeparam>
        /// <param name="source">The source query with an already included navigation.</param>
        /// <param name="path">Expression identifying the additional navigation to include.</param>
        /// <returns>A new queryable with the navigation included.</returns>
        public static INormIncludableQueryable<TEntity, TProperty> ThenInclude<TEntity, TPreviousProperty, TProperty>(
            this INormIncludableQueryable<TEntity, TPreviousProperty> source,
            Expression<Func<TPreviousProperty, TProperty>> path)
            where TEntity : class, new()
        {
            var method = ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty));
            var expression = Expression.Call(method, ((IQueryable<TEntity>)source).Expression, Expression.Quote(path));
            return new NormIncludableQueryable<TEntity, TProperty>(((IQueryable<TEntity>)source).Provider, expression);
        }

        /// <summary>
        /// Specifies an additional navigation property to include after a previous <c>Include</c> call
        /// where the previous navigation was a collection.
        /// </summary>
        /// <typeparam name="TEntity">Root entity type of the query.</typeparam>
        /// <typeparam name="TPreviousProperty">Type of the elements in the previously included collection.</typeparam>
        /// <typeparam name="TProperty">Type of the navigation to include.</typeparam>
        /// <param name="source">The source query with an already included collection navigation.</param>
        /// <param name="path">Expression identifying the additional navigation to include.</param>
        /// <returns>A new queryable with the navigation included.</returns>
        public static INormIncludableQueryable<TEntity, TProperty> ThenInclude<TEntity, TPreviousProperty, TProperty>(
            this INormIncludableQueryable<TEntity, IEnumerable<TPreviousProperty>> source,
            Expression<Func<TPreviousProperty, TProperty>> path)
            where TEntity : class, new()
        {
            var method = ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty));
            var expression = Expression.Call(method, ((IQueryable<TEntity>)source).Expression, Expression.Quote(path));
            return new NormIncludableQueryable<TEntity, TProperty>(((IQueryable<TEntity>)source).Provider, expression);
        }
    }
}