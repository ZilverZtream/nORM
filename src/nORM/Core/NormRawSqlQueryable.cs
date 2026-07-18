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
    /// A composable query whose source is a raw SQL statement rather than a mapped table. The SQL is wrapped
    /// as a derived table (<c>FROM (&lt;sql&gt;) AS t</c>) so ordinary LINQ operators — <c>Where</c>,
    /// <c>OrderBy</c>, <c>Skip</c>/<c>Take</c>, projections, <c>Count</c> — compose on top, and the entity's
    /// global/tenant filters are applied to the outer query (the raw SQL must expose the entity's columns).
    /// Produced by <see cref="DbContext.FromSqlRaw{T}(string, object[])"/> and
    /// <see cref="DbContext.FromSqlInterpolated{T}(FormattableString)"/>; the query translator recognises this
    /// root and substitutes the derived table for the mapped table's <c>FROM</c> clause.
    /// </summary>
    /// <remarks>
    /// It implements <see cref="INormQueryable{T}"/> so the nORM-specific operators (<c>Include</c>,
    /// <c>AsNoTracking</c>, the async terminals) compose directly off a raw query — each delegates to a
    /// standard query wrapping this instance's root expression, which the translator still recognises as a
    /// raw source (via <see cref="INormRawSqlSource"/>).
    /// </remarks>
    internal sealed class NormRawSqlQueryable<T> : NormQueryableBase<T>, INormQueryable<T>, INormRawSqlSource where T : class
    {
        /// <summary>The raw SQL statement that produces the source rows.</summary>
        public string RawSql { get; }

        /// <summary>The parameter values referenced positionally by <see cref="RawSql"/>.</summary>
        public object?[] RawParameters { get; }

        public NormRawSqlQueryable(DbContext ctx, string rawSql, object?[] rawParameters)
            : base(ctx)
        {
            RawSql = rawSql ?? throw new ArgumentNullException(nameof(rawSql));
            RawParameters = rawParameters ?? Array.Empty<object?>();
        }

        // A standard nORM query over this instance's root expression (Constant(this)). It carries the full
        // INormQueryable operator surface, and the translator still finds the raw source at the root.
        private INormQueryable<T> Composable() => (INormQueryable<T>)Provider.CreateQuery<T>(Expression);

        public INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> navigationPropertyPath)
            => Composable().Include(navigationPropertyPath);

        public INormQueryable<T> AsNoTracking() => Composable().AsNoTracking();
        public INormQueryable<T> AsSplitQuery() => Composable().AsSplitQuery();
        public IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default) => Composable().AsAsyncEnumerable(ct);
        public Task<List<T>> ToListAsync(CancellationToken ct = default) => Composable().ToListAsync(ct);
        public Task<T[]> ToArrayAsync(CancellationToken ct = default) => Composable().ToArrayAsync(ct);
        public Task<int> CountAsync(CancellationToken ct = default) => Composable().CountAsync(ct);
        public Task<bool> AnyAsync(CancellationToken ct = default) => Composable().AnyAsync(ct);
        public Task<T> FirstAsync(CancellationToken ct = default) => Composable().FirstAsync(ct);
        public Task<T?> FirstOrDefaultAsync(CancellationToken ct = default) => Composable().FirstOrDefaultAsync(ct);
        public Task<T> SingleAsync(CancellationToken ct = default) => Composable().SingleAsync(ct);
        public Task<T?> SingleOrDefaultAsync(CancellationToken ct = default) => Composable().SingleOrDefaultAsync(ct);
        public Task<int> ExecuteDeleteAsync(CancellationToken ct = default) => Composable().ExecuteDeleteAsync(ct);
        public Task<int> ExecuteUpdateAsync(Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default)
            => Composable().ExecuteUpdateAsync(set, ct);
    }
}
