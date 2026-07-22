using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// A query over an entity type that also exposes the change-tracking write verbs
    /// (<see cref="Add"/> / <see cref="Remove"/> / <see cref="Update"/> / <see cref="Attach"/> /
    /// <see cref="Find"/>), matching Entity Framework Core's <c>DbSet&lt;TEntity&gt;</c>. Obtain one from
    /// <c>context.Set&lt;T&gt;()</c>, or expose it as a context property for EF-style access:
    /// <code>
    /// public sealed class AppContext : DbContext
    /// {
    ///     public AppContext(DbConnection cn, DatabaseProvider p) : base(cn, p) { }
    ///     public DbSet&lt;User&gt; Users =&gt; this.Set&lt;User&gt;();   // context.Users.Where(...) / context.Users.Add(...)
    /// }
    /// </code>
    /// The computed-property form needs no reflection or auto-population, so it is trimming/NativeAOT-safe.
    /// It is a full <see cref="INormQueryable{T}"/>, so <c>Include</c>/<c>AsNoTracking</c>/<c>AsSplitQuery</c>
    /// and the async terminal operators compose off it without a cast, exactly as off <c>context.Query&lt;T&gt;()</c>;
    /// the write verbs delegate to the context's change tracker and apply on the next <c>SaveChangesAsync</c>.
    /// </summary>
    /// <typeparam name="T">The entity CLR type.</typeparam>
    public sealed class DbSet<T> : INormQueryable<T> where T : class
    {
        private readonly DbContext _context;
        private readonly INormQueryable<T> _query;

        internal DbSet(DbContext context)
        {
            _context = context;
            _query = (INormQueryable<T>)context.Query<T>();
        }

        // ── IQueryable<T> / IOrderedQueryable<T> ─────────────────────────────────────────────
        /// <inheritdoc />
        public Type ElementType => _query.ElementType;
        /// <inheritdoc />
        public Expression Expression => _query.Expression;
        /// <inheritdoc />
        public IQueryProvider Provider => _query.Provider;
        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator() => _query.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)_query).GetEnumerator();

        // ── INormQueryable<T> (delegate to the underlying query so front-door composition works) ──
        /// <inheritdoc />
        public INormIncludableQueryable<T, TProperty> Include<TProperty>(Expression<Func<T, TProperty>> navigationPropertyPath) => _query.Include(navigationPropertyPath);
        /// <inheritdoc />
        public INormQueryable<T> AsNoTracking() => _query.AsNoTracking();
        /// <inheritdoc />
        public INormQueryable<T> AsNoTrackingWithIdentityResolution() => _query.AsNoTrackingWithIdentityResolution();
        /// <inheritdoc />
        public INormQueryable<T> AsSplitQuery() => _query.AsSplitQuery();
        /// <inheritdoc />
        public IAsyncEnumerable<T> AsAsyncEnumerable(CancellationToken ct = default) => _query.AsAsyncEnumerable(ct);
        /// <inheritdoc />
        public Task<List<T>> ToListAsync(CancellationToken ct = default) => _query.ToListAsync(ct);
        /// <inheritdoc />
        public Task<T[]> ToArrayAsync(CancellationToken ct = default) => _query.ToArrayAsync(ct);
        /// <inheritdoc />
        public Task<int> CountAsync(CancellationToken ct = default) => _query.CountAsync(ct);
        /// <inheritdoc />
        public Task<bool> AnyAsync(CancellationToken ct = default) => _query.AnyAsync(ct);
        /// <inheritdoc />
        public Task<T> FirstAsync(CancellationToken ct = default) => _query.FirstAsync(ct);
        /// <inheritdoc />
        public Task<T?> FirstOrDefaultAsync(CancellationToken ct = default) => _query.FirstOrDefaultAsync(ct);
        /// <inheritdoc />
        public Task<T> SingleAsync(CancellationToken ct = default) => _query.SingleAsync(ct);
        /// <inheritdoc />
        public Task<T?> SingleOrDefaultAsync(CancellationToken ct = default) => _query.SingleOrDefaultAsync(ct);
        /// <inheritdoc />
        public Task<int> ExecuteDeleteAsync(CancellationToken ct = default) => _query.ExecuteDeleteAsync(ct);
        /// <inheritdoc />
        public Task<int> ExecuteUpdateAsync(Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct = default) => _query.ExecuteUpdateAsync(set, ct);

        // ── Change-tracking write verbs (EF Core DbSet parity) ───────────────────────────────
        /// <summary>Begins tracking <paramref name="entity"/> in the Added state (EF Core <c>Add</c>).</summary>
        public EntityEntry Add(T entity) => _context.Add(entity);
        /// <summary>Async form of <see cref="Add"/>; completes synchronously (keys are assigned at SaveChanges).</summary>
        public ValueTask<EntityEntry> AddAsync(T entity, CancellationToken ct = default) => _context.AddAsync(entity, ct);
        /// <summary>Begins tracking every entity in <paramref name="entities"/> in the Added state.</summary>
        public void AddRange(IEnumerable<T> entities) => _context.AddRange(entities);
        /// <summary>Begins tracking every entity in <paramref name="entities"/> in the Added state.</summary>
        public void AddRange(params T[] entities) => _context.AddRange(entities);
        /// <summary>Begins tracking <paramref name="entity"/> in the Unchanged state (EF Core <c>Attach</c>).</summary>
        public EntityEntry Attach(T entity) => _context.Attach(entity);
        /// <summary>Marks <paramref name="entity"/> Modified so all columns persist on the next save (EF Core <c>Update</c>).</summary>
        public EntityEntry Update(T entity) => _context.Update(entity);
        /// <summary>Marks every entity in <paramref name="entities"/> Modified.</summary>
        public void UpdateRange(IEnumerable<T> entities) => _context.UpdateRange(entities);
        /// <summary>Marks <paramref name="entity"/> Deleted so it is removed on the next save (EF Core <c>Remove</c>).</summary>
        public EntityEntry Remove(T entity) => _context.Remove(entity);
        /// <summary>Marks every entity in <paramref name="entities"/> Deleted.</summary>
        public void RemoveRange(IEnumerable<T> entities) => _context.RemoveRange(entities);
        /// <summary>Marks every entity in <paramref name="entities"/> Deleted.</summary>
        public void RemoveRange(params T[] entities) => _context.RemoveRange(entities);
        /// <summary>Finds a tracked or stored entity by primary key, or <c>null</c> (EF Core <c>Find</c>).</summary>
        public T? Find(params object?[] keyValues) => _context.Find<T>(keyValues);
        /// <summary>Async form of <see cref="Find"/>.</summary>
        public ValueTask<T?> FindAsync(params object?[] keyValues) => _context.FindAsync<T>(keyValues);
        /// <summary>Async form of <see cref="Find"/> with cancellation.</summary>
        public ValueTask<T?> FindAsync(object?[] keyValues, CancellationToken ct) => _context.FindAsync<T>(keyValues, ct);
    }
}
