using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
    [RequiresDynamicCode("nORM LINQ uses Expression<T>.MakeGenericMethod / MakeGenericType to lift queries onto entity types and is not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [RequiresUnreferencedCode("nORM LINQ reflects over entity property metadata to build SQL; trimming may remove the required members. Annotate consumer call sites accordingly.")]
    public static class NormQueryable
    {
        /// <summary>
        /// Cached factory delegates per entity type to avoid GetConstructor + Activator.CreateInstance
        /// reflection on every Query&lt;T&gt;() call. Saves ~2 allocations and reflection per call.
        /// </summary>
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<DbContext, object>> _queryFactoryCache = new();

        /// <summary>
        /// Rejects <c>Include</c>/<c>ThenInclude</c> composed over a <c>FromSqlRaw</c>/<c>FromSqlInterpolated</c>
        /// root. Eager loading rebuilds the root query from the mapped table's <c>FROM</c> clause, which would
        /// silently discard the raw SQL (and any filter it carries) and read the whole table — so this fails
        /// loud at composition time rather than returning wrong rows. Callers should materialise the raw query
        /// first (<c>ToList</c>) and load related data with a follow-up query, or express the join in the SQL.
        /// </summary>
        internal static void ThrowIfRawSqlRoot(Expression source, string op)
        {
            if (QueryTranslator.FindRootRawSource(source) is not null)
                throw new NormUnsupportedFeatureException(
                    $"'{op}' can't be composed onto a FromSqlRaw/FromSqlInterpolated query: eager loading rebuilds " +
                    "the root query from the mapped table and would silently ignore the raw SQL and its filter. " +
                    "Materialise the raw query first (ToList) and load related data with a follow-up query, or " +
                    "express the join in the raw SQL itself.");
        }

        /// <summary>
        /// Returns the SQL this query would execute, without running it — the equivalent of Entity Framework
        /// Core's <c>ToQueryString()</c>. Each parameter is listed as a leading comment (<c>-- @p0 = value</c>)
        /// so the statement is self-describing. Useful for debugging, logging, and comparing against
        /// hand-written SQL. The query is translated but never sent to the database.
        /// </summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">A nORM query (from <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>/<c>FromSqlRaw</c>).</param>
        /// <returns>The generated SQL, preceded by one comment line per parameter.</returns>
        public static string ToQueryString<T>(this IQueryable<T> source)
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is not NormQueryProvider provider)
                throw new NormUsageException(
                    "ToQueryString can only be used with nORM queries. Start with context.Query<T>()/Set<T>() " +
                    "or context.FromSqlRaw<T>(...). For Entity Framework queries, use " +
                    "Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToQueryString().");
            return provider.BuildQueryString(source.Expression);
        }

        /// <summary>
        /// Creates a queryable source for the specified entity type backed by the provided context.
        /// </summary>
        /// <typeparam name="T">The entity type to query.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> that provides access to the database.</param>
        /// <returns>An <see cref="IQueryable{T}"/> to compose and execute queries.</returns>
        public static IQueryable<T> Query<T>(this DbContext ctx) where T : class
        {
            // Register T as a query-root entity so IsMapped returns true for it.
            ctx.RegisterEntityType(typeof(T));
            var factory = _queryFactoryCache.GetOrAdd(typeof(T), static t =>
            {
                if (t.GetConstructor(Type.EmptyTypes) != null)
                {
                    var implType = typeof(NormQueryableImpl<>).MakeGenericType(t);
                    var ctor = implType.GetConstructor(new[] { typeof(DbContext) })!;
                    var ctxParam = System.Linq.Expressions.Expression.Parameter(typeof(DbContext), "ctx");
                    var newExpr = System.Linq.Expressions.Expression.New(ctor, ctxParam);
                    var lambda = System.Linq.Expressions.Expression.Lambda<Func<DbContext, object>>(newExpr, ctxParam);
                    return lambda.Compile();
                }
                else
                {
                    return static ctx => new NormQueryableImplUnconstrained<T>(ctx);
                }
            });
            return (IQueryable<T>)factory(ctx);
        }

        /// <summary>
        /// Returns the <see cref="DbSet{T}"/> for the specified entity type — an <see cref="IQueryable{T}"/>
        /// that also exposes the change-tracking write verbs — matching the Entity Framework Core
        /// <c>DbContext.Set&lt;T&gt;()</c> entry point so existing muscle memory works unchanged. Expose it as
        /// a context property (<c>public DbSet&lt;User&gt; Users =&gt; this.Set&lt;User&gt;();</c>) for
        /// <c>context.Users</c>-style access. Composing LINQ still yields the same query as
        /// <see cref="Query{T}(DbContext)"/>.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> that provides access to the database.</param>
        /// <returns>A <see cref="DbSet{T}"/> to query and to register add/update/remove intent.</returns>
        public static DbSet<T> Set<T>(this DbContext ctx) where T : class => new(ctx);
    }

    /// <summary>
    /// Defines a queryable data source with extended functionality like eager-loading and
    /// asynchronous evaluation.
    /// </summary>
    /// <typeparam name="T">The entity type returned by the query.</typeparam>
    /// <remarks>
    /// IMPORTANT: This interface no longer requires the new() constraint to support
    /// entities with parameterized constructors (e.g., records, immutable classes).
    /// </remarks>
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
        /// Disables change tracking but still resolves entity identity: a root entity whose key appears more
        /// than once in one result set (e.g. from <c>Concat</c> or a self-join/SelectMany flatten) is returned
        /// as a single shared instance. Matches EF Core's <c>AsNoTrackingWithIdentityResolution</c>.
        /// </summary>
        INormQueryable<T> AsNoTrackingWithIdentityResolution();

        /// <summary>
        /// Accepted for EF Core compatibility and has no additional effect:
        /// <see cref="Include{TProperty}"/> always eager-loads its navigation paths
        /// through coordinated follow-up queries.
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

    [RequiresDynamicCode("nORM LINQ uses Expression<T>.MakeGenericMethod / MakeGenericType to lift queries onto entity types and is not NativeAOT-compatible.")]
    [RequiresUnreferencedCode("nORM LINQ reflects over entity property metadata to build SQL; trimming may remove the required members.")]
    internal sealed class NormQueryableImpl<T> : NormQueryableBase<T>, INormQueryable<T> where T : class
    {
        public NormQueryableImpl(DbContext ctx) : base(ctx)
        {
        }

        public NormQueryableImpl(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }

        public INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> path)
        {
            NormQueryable.ThrowIfRawSqlRoot(Expression, nameof(Include));
            var method = typeof(INormQueryable<>).MakeGenericType(typeof(T))
                .GetMethods()
                .Single(m => m.Name == nameof(Include) && m.IsGenericMethod);
            var generic = method.MakeGenericMethod(typeof(TProperty));
            // The source expression's static type after standard Queryable operators
            // (Where/OrderBy/Take) is IQueryable<T>, not INormQueryable<T>; an instance
            // call against it throws ArgumentException at expression-build time. Convert
            // to the interface first — the translator unwraps the Convert when visiting.
            var expression = Expression.Call(
                Expression.Convert(Expression, typeof(INormQueryable<>).MakeGenericType(typeof(T))),
                generic, Expression.Quote(path));
            return new NormIncludableQueryable<T, TProperty>(Provider, expression);
        }

        /// <summary>
        /// Returns a new query that will not track the resulting entities in the <see cref="DbContext"/>.
        /// </summary>
        /// <returns>An untracked query.</returns>
        public INormQueryable<T> AsNoTracking()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTracking))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImpl<T>(Provider, expression);
        }

        public INormQueryable<T> AsNoTrackingWithIdentityResolution()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTrackingWithIdentityResolution))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImpl<T>(Provider, expression);
        }

        /// <summary>
        /// Accepted for EF Core compatibility and has no additional effect: Include
        /// always eager-loads its navigation paths.
        /// </summary>
        /// <returns>The same query; eager loading is driven by Include alone.</returns>
        public INormQueryable<T> AsSplitQuery()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsSplitQuery))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
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
    /// Now fully supports async LINQ methods for all entity types.
    /// </summary>
    [RequiresDynamicCode("nORM LINQ uses Expression<T>.MakeGenericMethod / MakeGenericType to lift queries onto entity types and is not NativeAOT-compatible.")]
    [RequiresUnreferencedCode("nORM LINQ reflects over entity property metadata to build SQL; trimming may remove the required members.")]
    internal sealed class NormQueryableImplUnconstrained<T> : NormQueryableBase<T>, INormQueryable<T>
    {
        public NormQueryableImplUnconstrained(DbContext ctx) : base(ctx)
        {
        }

        public NormQueryableImplUnconstrained(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }

        public INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> path)
        {
            NormQueryable.ThrowIfRawSqlRoot(Expression, nameof(Include));
            var method = typeof(INormQueryable<>).MakeGenericType(typeof(T))
                .GetMethods()
                .Single(m => m.Name == nameof(Include) && m.IsGenericMethod);
            var generic = method.MakeGenericMethod(typeof(TProperty));
            // The source expression's static type after standard Queryable operators
            // (Where/OrderBy/Take) is IQueryable<T>, not INormQueryable<T>; an instance
            // call against it throws ArgumentException at expression-build time. Convert
            // to the interface first — the translator unwraps the Convert when visiting.
            var expression = Expression.Call(
                Expression.Convert(Expression, typeof(INormQueryable<>).MakeGenericType(typeof(T))),
                generic, Expression.Quote(path));
            return new NormIncludableQueryableUnconstrained<T, TProperty>(Provider, expression);
        }

        public INormQueryable<T> AsNoTracking()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTracking))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImplUnconstrained<T>(Provider, expression);
        }

        public INormQueryable<T> AsNoTrackingWithIdentityResolution()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTrackingWithIdentityResolution))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImplUnconstrained<T>(Provider, expression);
        }

        public INormQueryable<T> AsSplitQuery()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsSplitQuery))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImplUnconstrained<T>(Provider, expression);
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

    [RequiresDynamicCode("nORM Include uses Expression<T>.MakeGenericMethod to compose navigation includes; not NativeAOT-compatible.")]
    [RequiresUnreferencedCode("nORM Include reflects over navigation property metadata; trimming may remove the required members.")]
    internal sealed class NormIncludableQueryableUnconstrained<T, TProperty> : NormQueryableBase<T>, INormIncludableQueryable<T, TProperty>
    {
        public NormIncludableQueryableUnconstrained(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }

        public INormIncludableQueryable<T, TProperty2> Include<TProperty2>(Expression<Func<T, TProperty2>> path)
        {
            NormQueryable.ThrowIfRawSqlRoot(Expression, nameof(Include));
            var method = typeof(INormQueryable<>).MakeGenericType(typeof(T))
                .GetMethods()
                .Single(m => m.Name == nameof(INormQueryable<T>.Include) && m.IsGenericMethod);
            var generic = method.MakeGenericMethod(typeof(TProperty2));
            // The source expression's static type after standard Queryable operators
            // (Where/OrderBy/Take) is IQueryable<T>, not INormQueryable<T>; an instance
            // call against it throws ArgumentException at expression-build time. Convert
            // to the interface first — the translator unwraps the Convert when visiting.
            var expression = Expression.Call(
                Expression.Convert(Expression, typeof(INormQueryable<>).MakeGenericType(typeof(T))),
                generic, Expression.Quote(path));
            return new NormIncludableQueryableUnconstrained<T, TProperty2>(Provider, expression);
        }

        public INormQueryable<T> AsNoTracking()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTracking))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImplUnconstrained<T>(Provider, expression);
        }

        public INormQueryable<T> AsNoTrackingWithIdentityResolution()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTrackingWithIdentityResolution))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImplUnconstrained<T>(Provider, expression);
        }

        public INormQueryable<T> AsSplitQuery()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsSplitQuery))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImplUnconstrained<T>(Provider, expression);
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

    [RequiresDynamicCode("nORM Include uses Expression<T>.MakeGenericMethod to compose navigation includes; not NativeAOT-compatible.")]
    [RequiresUnreferencedCode("nORM Include reflects over navigation property metadata; trimming may remove the required members.")]
    internal sealed class NormIncludableQueryable<T, TProperty> : NormQueryableBase<T>, INormIncludableQueryable<T, TProperty> where T : class
    {
        public NormIncludableQueryable(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
        }

        public INormIncludableQueryable<T, TProperty2> Include<TProperty2>(Expression<Func<T, TProperty2>> path)
        {
            NormQueryable.ThrowIfRawSqlRoot(Expression, nameof(Include));
            var method = typeof(INormQueryable<>).MakeGenericType(typeof(T))
                .GetMethods()
                .Single(m => m.Name == nameof(INormQueryable<T>.Include) && m.IsGenericMethod);
            var generic = method.MakeGenericMethod(typeof(TProperty2));
            // The source expression's static type after standard Queryable operators
            // (Where/OrderBy/Take) is IQueryable<T>, not INormQueryable<T>; an instance
            // call against it throws ArgumentException at expression-build time. Convert
            // to the interface first — the translator unwraps the Convert when visiting.
            var expression = Expression.Call(
                Expression.Convert(Expression, typeof(INormQueryable<>).MakeGenericType(typeof(T))),
                generic, Expression.Quote(path));
            return new NormIncludableQueryable<T, TProperty2>(Provider, expression);
        }

        /// <summary>
        /// Returns a new query that will not track the resulting entities in the <see cref="DbContext"/>.
        /// </summary>
        /// <returns>An untracked query.</returns>
        public INormQueryable<T> AsNoTracking()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTracking))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImpl<T>(Provider, expression);
        }

        public INormQueryable<T> AsNoTrackingWithIdentityResolution()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsNoTrackingWithIdentityResolution))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
            return new NormQueryableImpl<T>(Provider, expression);
        }

        /// <summary>
        /// Accepted for EF Core compatibility and has no additional effect: Include
        /// always eager-loads its navigation paths.
        /// </summary>
        /// <returns>The same query; eager loading is driven by Include alone.</returns>
        public INormQueryable<T> AsSplitQuery()
        {
            var normType = typeof(INormQueryable<>).MakeGenericType(typeof(T));
            var method = normType.GetMethod(nameof(INormQueryable<T>.AsSplitQuery))!;
            var expression = Expression.Call(Expression.Convert(Expression, normType), method);
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
    [RequiresDynamicCode("nORM Include extension methods emit MakeGenericMethod expressions; not NativeAOT-compatible.")]
    [RequiresUnreferencedCode("nORM Include extension methods reflect over navigation property metadata; trimming may remove the required members.")]
    public static class NormIncludableQueryableExtensions
    {
        /// <summary>
        /// Specifies a navigation property to eager-load, matching Entity Framework Core's
        /// <c>Include</c> surface. This extension lets <c>Include</c> chain directly off the
        /// <see cref="IQueryable{T}"/> returned by <see cref="NormQueryable.Query{T}"/> /
        /// <see cref="NormQueryable.Set{T}"/> and off standard operators such as
        /// <c>Where</c>/<c>OrderBy</c>, whose static result type is <see cref="IQueryable{T}"/>
        /// even though the underlying object is a nORM queryable — so no cast to
        /// <see cref="INormQueryable{T}"/> is required. When the static type already is
        /// <see cref="INormQueryable{T}"/>, the instance <c>Include</c> is preferred by the
        /// compiler and this extension is not involved.
        /// </summary>
        /// <typeparam name="TEntity">The root entity type of the query.</typeparam>
        /// <typeparam name="TProperty">Type of the navigation property.</typeparam>
        /// <param name="source">A nORM query started with <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>.</param>
        /// <param name="navigationPropertyPath">Expression identifying the navigation to include.</param>
        /// <returns>A query with the navigation included, chainable with <c>ThenInclude</c>.</returns>
        public static INormIncludableQueryable<TEntity, TProperty> Include<TEntity, TProperty>(
            this IQueryable<TEntity> source,
            Expression<Func<TEntity, TProperty>> navigationPropertyPath)
            where TEntity : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(navigationPropertyPath);

            // Query<T>()/Set<T>() and every standard operator over them return objects that
            // implement INormQueryable<T> at runtime (see NormQueryProvider.CreateQueryInternal);
            // only the compile-time type is erased to IQueryable<T>. Delegate to the instance
            // Include so the constrained/unconstrained impl choice and the Convert-unwrapping the
            // translator expects are preserved exactly — this is the same path the cast workaround
            // (INormQueryable<T>)q).Include(...) already exercises.
            if (source is INormQueryable<TEntity> normQueryable)
                return normQueryable.Include(navigationPropertyPath);

            throw new NormUsageException(
                "Include can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>() or context.Set<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.Include().");
        }

        /// <summary>
        /// Disables change tracking for the query results, matching Entity Framework Core's
        /// <c>AsNoTracking</c>. Like <see cref="Include{TEntity,TProperty}"/> this extension lets
        /// <c>AsNoTracking</c> chain off the <see cref="IQueryable{T}"/> returned by
        /// <see cref="NormQueryable.Query{T}"/>/<see cref="NormQueryable.Set{T}"/> and off standard
        /// operators without a cast to <see cref="INormQueryable{T}"/>; when the static type already
        /// is <see cref="INormQueryable{T}"/> the instance method is preferred by the compiler.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">A nORM query started with <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>.</param>
        /// <returns>An untracked query.</returns>
        public static INormQueryable<T> AsNoTracking<T>(this IQueryable<T> source)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source is INormQueryable<T> normQueryable)
                return normQueryable.AsNoTracking();

            throw new NormUsageException(
                "AsNoTracking can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>() or context.Set<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AsNoTracking().");
        }

        /// <summary>
        /// The <see cref="IQueryable{T}"/> form of <see cref="INormQueryable{T}.AsNoTrackingWithIdentityResolution"/>
        /// for chains typed as <see cref="IQueryable{T}"/>; when the static type already is
        /// <see cref="INormQueryable{T}"/> the instance method is preferred by the compiler.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">A nORM query started with <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>.</param>
        /// <returns>An untracked query that resolves entity identity.</returns>
        public static INormQueryable<T> AsNoTrackingWithIdentityResolution<T>(this IQueryable<T> source)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source is INormQueryable<T> normQueryable)
                return normQueryable.AsNoTrackingWithIdentityResolution();

            throw new NormUsageException(
                "AsNoTrackingWithIdentityResolution can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>() or context.Set<T>(). " +
                "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution().");
        }

        /// <summary>
        /// Forces the query's results to be change-tracked, matching Entity Framework Core's
        /// <c>AsTracking</c>. This is the per-query override of a context whose
        /// <see cref="nORM.Configuration.DbContextOptions.DefaultTrackingBehavior"/> is
        /// <see cref="QueryTrackingBehavior.NoTracking"/> — the results are tracked even though the context
        /// would otherwise return them detached. When composed with <c>AsNoTracking</c> the outermost
        /// (last-written) call wins. Has no effect on the default (already tracking) context.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">A nORM query started with <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>.</param>
        /// <returns>A query whose results are tracked.</returns>
        [RequiresDynamicCode("nORM AsTracking emits a MakeGenericMethod marker call; not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("nORM AsTracking uses reflection to resolve the open generic marker method; trimming may remove it.")]
        public static IQueryable<T> AsTracking<T>(this IQueryable<T> source)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is not nORM.Query.NormQueryProvider)
                throw new NormUsageException(
                    "AsTracking can only be used with nORM queries. " +
                    "Make sure you started with context.Query<T>() or context.Set<T>(). " +
                    "For Entity Framework queries, use Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.AsTracking().");

            // Self-referential marker call (like IgnoreQueryFilters/TagWith): a translator sets the
            // force-tracking flag and strips the marker. No separate marker method is needed.
            var method = ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(typeof(T));
            var call = Expression.Call(method, source.Expression);
            return source.Provider.CreateQuery<T>(call);
        }

        /// <summary>
        /// Accepted for Entity Framework Core compatibility; nORM always eager-loads
        /// <c>Include</c> paths through coordinated follow-up queries, so this has no additional
        /// effect. Exposed on <see cref="IQueryable{T}"/> so it composes without a cast, mirroring
        /// <see cref="AsNoTracking{T}"/>.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">A nORM query started with <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>.</param>
        /// <returns>The same query; eager loading is driven by Include alone.</returns>
        public static INormQueryable<T> AsSplitQuery<T>(this IQueryable<T> source)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source is INormQueryable<T> normQueryable)
                return normQueryable.AsSplitQuery();

            throw new NormUsageException(
                "AsSplitQuery can only be used with nORM queries. " +
                "Make sure you started with context.Query<T>() or context.Set<T>().");
        }

        /// <summary>
        /// Bypasses the user-defined global query filters (registered via
        /// <c>DbContextOptions.AddGlobalFilter</c>, e.g. soft-delete) for this query — the escape
        /// hatch for admin "show deleted", restore, and audit views, matching Entity Framework
        /// Core's <c>IgnoreQueryFilters</c>. It applies to the root query's filters.
        /// <para>
        /// IMPORTANT: it does NOT bypass tenant isolation. In nORM the tenant predicate is a
        /// security boundary applied independently of global filters, so it always remains in
        /// force; a query can never read another tenant's rows via IgnoreQueryFilters.
        /// </para>
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">A nORM query started with <c>context.Query&lt;T&gt;()</c>/<c>Set&lt;T&gt;()</c>.</param>
        /// <returns>A query whose user global filters are suppressed.</returns>
        [RequiresDynamicCode("nORM IgnoreQueryFilters emits a MakeGenericMethod marker call; not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("nORM IgnoreQueryFilters uses reflection to resolve the open generic marker method; trimming may remove it.")]
        public static IQueryable<T> IgnoreQueryFilters<T>(this IQueryable<T> source)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            if (source.Provider is not nORM.Query.NormQueryProvider)
                throw new NormUsageException(
                    "IgnoreQueryFilters can only be used with nORM queries. " +
                    "Make sure you started with context.Query<T>() or context.Set<T>().");

            // Marker operator in the expression tree: the provider detects it before translation to
            // skip user global filters, and a pass-through translator strips it. Self-referential
            // like ThenInclude so no separate marker method is needed.
            var method = ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(typeof(T));
            var call = Expression.Call(method, source.Expression);
            return source.Provider.CreateQuery<T>(call);
        }

        /// <summary>
        /// Adds a comment tag to the generated SQL for this query, matching EF Core's <c>TagWith</c>. The
        /// tag is emitted as a leading SQL line comment (injection-safe: each line is prefixed with
        /// <c>-- </c>), which is invaluable for correlating a query in database logs, an APM trace, or a
        /// slow-query report back to the source. Multiple <c>TagWith</c> calls accumulate. Has no effect on
        /// results.
        /// </summary>
        /// <typeparam name="T">The query element type.</typeparam>
        /// <param name="source">The source query.</param>
        /// <param name="tag">The comment to embed in the generated SQL.</param>
        /// <returns>A query that emits the tag comment in its SQL.</returns>
        [RequiresDynamicCode("nORM TagWith emits a MakeGenericMethod marker call; not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("nORM TagWith uses reflection to resolve the open generic marker method; trimming may remove it.")]
        public static IQueryable<T> TagWith<T>(this IQueryable<T> source, string tag)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(tag);
            if (source.Provider is not nORM.Query.NormQueryProvider)
                throw new NormUsageException(
                    "TagWith can only be used with nORM queries. " +
                    "Make sure you started with context.Query<T>() or context.Set<T>().");

            // Self-referential marker call (like IgnoreQueryFilters): a pass-through translator captures
            // the tag and prepends it to the SQL as a line comment, then strips the marker.
            var method = ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(typeof(T));
            var call = Expression.Call(method, source.Expression, Expression.Constant(tag));
            return source.Provider.CreateQuery<T>(call);
        }

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
            where TEntity : class
        {
            NormQueryable.ThrowIfRawSqlRoot(((IQueryable<TEntity>)source).Expression, nameof(ThenInclude));
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
            where TEntity : class
        {
            NormQueryable.ThrowIfRawSqlRoot(((IQueryable<TEntity>)source).Expression, nameof(ThenInclude));
            var method = ((System.Reflection.MethodInfo)System.Reflection.MethodBase.GetCurrentMethod()!)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(typeof(TEntity), typeof(TPreviousProperty), typeof(TProperty));
            var expression = Expression.Call(method, ((IQueryable<TEntity>)source).Expression, Expression.Quote(path));
            return new NormIncludableQueryable<TEntity, TProperty>(((IQueryable<TEntity>)source).Provider, expression);
        }
    }
}