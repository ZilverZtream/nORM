using System;
using System.Collections;
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
        public static INormQueryable<T> Query<T>(this DbContext ctx) where T : class, new()
            => new NormQueryableImpl<T>(ctx);
    }

    /// <summary>
    /// Defines a queryable data source with extended functionality like Include.
    /// </summary>
    public interface INormQueryable<T> : IOrderedQueryable<T>
    {
        INormQueryable<T> Include(Expression<Func<T, object>> navigationPropertyPath);
        INormQueryable<T> AsNoTracking();
        Task<List<T>> ToListAsync(CancellationToken ct = default);
        Task<T[]> ToArrayAsync(CancellationToken ct = default);
        Task<int> CountAsync(CancellationToken ct = default);
        Task<bool> AnyAsync(CancellationToken ct = default);
        Task<T> FirstAsync(CancellationToken ct = default);
        Task<T?> FirstOrDefaultAsync(CancellationToken ct = default);
        Task<T> SingleAsync(CancellationToken ct = default);
        Task<T?> SingleOrDefaultAsync(CancellationToken ct = default);
    }

    // Base interface for internal use without constraints
    internal interface INormQueryableInternal<T> : IOrderedQueryable<T>
    {
        // No async methods here to avoid constraint conflicts
    }

    internal sealed class NormQueryableImpl<T> : INormQueryable<T> where T : class, new()
    {
        public Expression Expression { get; }
        public Type ElementType => typeof(T);
        public IQueryProvider Provider { get; }

        public NormQueryableImpl(DbContext ctx)
        {
            Expression = Expression.Constant(this);
            Provider = new NormQueryProvider(ctx);
        }

        public NormQueryableImpl(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public IEnumerator<T> GetEnumerator() => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public INormQueryable<T> Include(Expression<Func<T, object>> path)
        {
            var expression = Expression.Call(
                typeof(INormQueryable<>).MakeGenericType(typeof(T)),
                nameof(Include),
                Type.EmptyTypes,
                Expression,
                path
            );
            return new NormQueryableImpl<T>(Provider, expression);
        }

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

        public Task<List<T>> ToListAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<List<T>>(Expression, ct);
        public async Task<T[]> ToArrayAsync(CancellationToken ct = default) => (await ToListAsync(ct)).ToArray();
        public Task<int> CountAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<int>(Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(T) }, Expression), ct);
        public async Task<bool> AnyAsync(CancellationToken ct = default) => await ((NormQueryProvider)Provider).ExecuteAsync<bool>(Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(T) }, Expression), ct);
        public Task<T> FirstAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T>(Expression.Call(typeof(Queryable), nameof(Queryable.First), new[] { typeof(T) }, Expression), ct);
        public Task<T?> FirstOrDefaultAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T?>(Expression.Call(typeof(Queryable), nameof(Queryable.FirstOrDefault), new[] { typeof(T) }, Expression), ct);
        public Task<T> SingleAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T>(Expression.Call(typeof(Queryable), nameof(Queryable.Single), new[] { typeof(T) }, Expression), ct);
        public Task<T?> SingleOrDefaultAsync(CancellationToken ct = default) => ((NormQueryProvider)Provider).ExecuteAsync<T?>(Expression.Call(typeof(Queryable), nameof(Queryable.SingleOrDefault), new[] { typeof(T) }, Expression), ct);
    }

    /// <summary>
    /// Internal queryable implementation without constraints, used for intermediate query operations
    /// like Join that produce anonymous types or other types without parameterless constructors.
    /// </summary>
    internal sealed class NormQueryableImplUnconstrained<T> : INormQueryableInternal<T>
    {
        public Expression Expression { get; }
        public Type ElementType => typeof(T);
        public IQueryProvider Provider { get; }

        public NormQueryableImplUnconstrained(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public IEnumerator<T> GetEnumerator() => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}