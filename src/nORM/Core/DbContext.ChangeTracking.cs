using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Begins tracking the given entity in the <see cref="ChangeTracker"/> in the
        /// <see cref="EntityState.Added"/> state. The entity will be inserted into the
        /// database when <c>SaveChanges</c> is called.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity instance to add.</param>
        /// <returns>An <see cref="EntityEntry"/> representing the tracked entity.</returns>
        public EntityEntry Add<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            NormValidator.ValidateEntity(entity);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            var map = GetMapping(typeof(T));
            EnsureWritableMapping(map, "Add");
            return ChangeTracker.Track(entity, EntityState.Added, map);
        }

        /// <summary>
        /// Asynchronously begins tracking the entity in the <see cref="EntityState.Added"/> state,
        /// matching EF Core's <c>AddAsync</c>. nORM assigns database-generated keys at <c>SaveChanges</c>
        /// (not at add time), so this completes synchronously — the async signature exists for source
        /// compatibility with EF code.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity instance to add.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>An <see cref="EntityEntry"/> representing the tracked entity.</returns>
        public ValueTask<EntityEntry> AddAsync<T>(T entity, CancellationToken ct = default) where T : class
        {
            ct.ThrowIfCancellationRequested();
            return new ValueTask<EntityEntry>(Add(entity));
        }

        /// <summary>
        /// Starts tracking the entity without modifying its state. Existing values are
        /// assumed to match those in the database and no update will be sent unless
        /// changes are detected.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to attach.</param>
        /// <returns>An <see cref="EntityEntry"/> for the attached entity.</returns>
        public EntityEntry Attach<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            NormValidator.ValidateEntity(entity);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return ChangeTracker.Track(entity, EntityState.Unchanged, GetMapping(typeof(T)));
        }

        /// <summary>
        /// Marks the entity as <see cref="EntityState.Modified"/> so that all of its
        /// properties are treated as modified and will be persisted during
        /// <c>SaveChanges</c>.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to update.</param>
        /// <returns>An <see cref="EntityEntry"/> for the updated entity.</returns>
        public EntityEntry Update<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            NormValidator.ValidateEntity(entity);
            var map = GetMapping(typeof(T));
            EnsureWritableMapping(map, "Update");
            return ChangeTracker.Track(entity, EntityState.Modified, map);
        }

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be removed from the
        /// database when <c>SaveChanges</c> is executed.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity instance to remove.</param>
        /// <returns>An <see cref="EntityEntry"/> for the removed entity.</returns>
        public EntityEntry Remove<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            NormValidator.ValidateEntity(entity);
            var map = GetMapping(typeof(T));
            EnsureWritableMapping(map, "Remove");
            return ChangeTracker.Track(entity, EntityState.Deleted, map);
        }
        /// <summary>
        /// Returns the <see cref="EntityEntry"/> for the supplied entity if it is already being tracked.
        /// If the entity is not tracked, an exception is thrown instructing the caller to use
        /// <see cref="Attach{T}"/> explicitly. Auto-attaching is deliberately not supported because
        /// it is a dangerous side-effect that can silently modify tracking state.
        /// </summary>
        /// <param name="entity">The entity whose tracking entry is requested.</param>
        /// <returns>An <see cref="EntityEntry"/> representing the entity's tracking information.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="entity"/> is null or invalid.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity is not currently tracked.</exception>
        public EntityEntry Entry(object entity)
        {
            ThrowIfDisposed();
            NormValidator.ValidateEntity(entity, nameof(entity));

            // Check if entity is already tracked before returning entry.
            // Auto-attaching untracked entities is dangerous - it silently modifies tracking state.
            // Uses O(1) identity-map lookup via _entriesByReference dictionary.
            var existingEntry = ChangeTracker.GetEntryOrDefault(entity);
            if (existingEntry == null)
            {
                throw new InvalidOperationException(
                    $"The entity of type '{entity.GetType().Name}' is not being tracked by the context. " +
                    "Use context.Attach() to explicitly attach the entity before calling Entry().");
            }

            // Ensure lazy loading is enabled for the tracked entity (cached MethodInfo avoids repeated reflection)
            try
            {
                s_enableLazyLoadingMethod.MakeGenericMethod(entity.GetType()).Invoke(null, new object[] { entity, this });
            }
            catch (System.Reflection.TargetInvocationException tie) when (tie.InnerException != null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
            }

            return existingEntry;
        }
    }
}
