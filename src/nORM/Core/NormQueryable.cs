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
        public static IQueryable<T> Query<T>(this DbContext ctx) where T : class
            => typeof(T).GetConstructor(Type.EmptyTypes) != null
                ? (IQueryable<T>)Activator.CreateInstance(typeof(NormQueryableImpl<>).MakeGenericType(typeof(T)), ctx)!
                : new NormQueryableImplUnconstrained<T>(ctx);
    }

    /// <summary>
    /// Defines a queryable data source with extended functionality like Include.
    /// </summary>
    public interface INormQueryable<T> : IOrderedQueryable<T>
    {
        INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> navigationPropertyPath);
        INormQueryable<T> AsNoTracking();
        INormQueryable<T> AsSplitQuery();
        IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default);
        Task<List<T>> ToListAsync(CancellationToken ct = default);
        Task<T[]> ToArrayAsync(CancellationToken ct = default);
        Task<int> CountAsync(CancellationToken ct = default);
        Task<bool> AnyAsync(CancellationToken ct = default);
        Task<T> FirstAsync(CancellationToken ct = default);
        Task<T?> FirstOrDefaultAsync(CancellationToken ct = default);
        Task<T> SingleAsync(CancellationToken ct = default);
        Task<T?> SingleOrDefaultAsync(CancellationToken ct = default);
        Task<int> ExecuteDeleteAsync(CancellationToken ct = default);
        Task<int> ExecuteUpdateAsync(Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default);
    }

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

        public IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default)
            => ((NormQueryProvider)Provider).AsAsyncEnumerable<T>(Expression, ct);

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

    public static class NormIncludableQueryableExtensions
    {
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