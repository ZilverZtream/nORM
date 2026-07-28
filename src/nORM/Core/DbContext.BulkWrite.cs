using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        #region Bulk Operations

        // Bulk, stored-procedure, and temporal writes are non-idempotent and expose no commit barrier the
        // retry strategy can observe: their commit happens inside the provider (bulk) or is a single
        // auto-commit statement (proc/temporal). Passing this as isCommitAttempted makes the retry strategy
        // treat every failure as possibly-committed and NEVER retry them, so a transient fault at or after
        // commit cannot re-run the operation and duplicate rows. Idempotent reads and the tracked
        // single-entity / SaveChanges paths — which thread a real commit-attempted flag — are unaffected.
        private static readonly Func<bool> s_nonIdempotentNoRetry = static () => true;

        /// <summary>
        /// True when <paramref name="entity"/> carries populated nORM-managed children — a non-empty owned
        /// collection or a non-empty many-to-many navigation — that the columns-only bulk insert/update fast
        /// path cannot persist (they need the owner's key and per-owner child sync, which only SaveChanges
        /// performs). Empty collections return false so an aggregate with no children keeps the fast path.
        /// Silently dropping such children is data loss, so the bulk entry points refuse loudly instead.
        /// </summary>
        private static bool HasPopulatedNormManagedChildren(object entity, TableMapping map)
        {
            foreach (var oc in map.OwnedCollections)
                if (oc.CollectionGetter(entity) is System.Collections.IEnumerable owned && HasAnyItem(owned))
                    return true;
            foreach (var jtm in map.ManyToManyJoins)
                if (jtm.LeftCollectionGetter(entity) is { Count: > 0 })
                    return true;
            return false;

            static bool HasAnyItem(System.Collections.IEnumerable e)
            {
                foreach (var _ in e) return true;
                return false;
            }
        }

        private static void GuardBulkAggregateChildren(object entity, TableMapping map, string method)
        {
            if ((map.OwnedCollections.Count > 0 || map.ManyToManyJoins.Count > 0)
                && HasPopulatedNormManagedChildren(entity, map))
                throw new NormUnsupportedFeatureException(
                    $"{method} writes only the owner's own columns and cannot persist owned-collection children " +
                    "or many-to-many relationships (they need the owner's key and per-owner child sync). Use " +
                    "SaveChanges for aggregates with populated owned/related children.",
                    NormUnsupportedReason.BulkAggregateChildrenUnsupported);
        }

        /// <summary>
        /// Efficiently inserts a collection of entities using provider specific bulk
        /// techniques. Validation and tenant checks are applied to each entity before
        /// execution.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of inserted rows.</returns>
        public Task<int> BulkInsertAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                if (entities == null) throw new ArgumentNullException(nameof(entities));
                var entityList = entities.ToList();                         // single enumeration
                NormValidator.ValidateBulkOperation(entityList, "insert");
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var map = GetMapping(typeof(T));
                EnsureWritableMapping(map, "BulkInsertAsync");
                foreach (var entity in entityList)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Insert);
                    GuardBulkAggregateChildren(entity, map, "BulkInsertAsync");
                    // Stamp the TPH discriminator from the entity's RUNTIME type (not the compile-time typeof(T)):
                    // a base-typed batch (List<Base> { new Derived(), ... }) must stamp each row's own subtype
                    // discriminator, else the base mapping's no-op ApplyDiscriminator leaves discriminator=0 and
                    // strands the subtype. ResolveWriteMapping mirrors the tracked write path. The base `map`
                    // stays the bind target below — its merged columns (TPH-base-safe getters) bind each row's
                    // own subtype columns (null for siblings) plus the now-stamped discriminator.
                    ResolveWriteMapping(entity).ApplyDiscriminator(entity);
                }
                return await _p.BulkInsertAsync(ctx, map, entityList, token).ConfigureAwait(false);
            }, s_nonIdempotentNoRetry, ct);
        }

        /// <summary>
        /// Performs a set based update of the provided entities using the provider's
        /// bulk update facilities.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">Entities to update.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of updated rows.</returns>
        public Task<int> BulkUpdateAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                if (entities == null) throw new ArgumentNullException(nameof(entities));
                var entityList = entities.ToList();                         // single enumeration
                NormValidator.ValidateBulkOperation(entityList, "update");
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var map = GetMapping(typeof(T));
                EnsureWritableMapping(map, "BulkUpdateAsync");
                foreach (var entity in entityList)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Update);
                    GuardBulkAggregateChildren(entity, map, "BulkUpdateAsync");
                }
                return await _p.BulkUpdateAsync(ctx, map, entityList, token).ConfigureAwait(false);
            }, s_nonIdempotentNoRetry, ct);
        }

        /// <summary>
        /// Removes a collection of entities from the database using bulk delete
        /// operations.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">Entities to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of deleted rows.</returns>
        public Task<int> BulkDeleteAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            return _executionStrategy.ExecuteAsync(async (ctx, token) =>
            {
                if (entities == null) throw new ArgumentNullException(nameof(entities));
                var entityList = entities.ToList();                         // single enumeration
                NormValidator.ValidateBulkOperation(entityList, "delete");
                await ctx.EnsureConnectionAsync(token).ConfigureAwait(false);
                var map = GetMapping(typeof(T));
                EnsureWritableMapping(map, "BulkDeleteAsync");
                foreach (var entity in entityList)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Delete);
                }

                // Mappings with no nORM-managed children keep the fast direct path.
                if (map.OwnedCollections.Count == 0 && map.ManyToManyJoins.Count == 0)
                    return await _p.BulkDeleteAsync(ctx, map, entityList, token).ConfigureAwait(false);

                // Owner rows + nORM-managed children (owned-collection children, m2m join rows) must delete
                // atomically, mirroring the tracked SaveChanges Deleted branch — otherwise bulk delete orphans
                // the children. Establish a transaction when the caller isn't managing one so the per-entity
                // child cleanup and the provider bulk delete (which participates in CurrentTransaction) commit
                // together; the children hold the FK so they are removed before the owner rows.
                var ownTx = ctx.CurrentTransaction == null;
                DbTransaction? bulkTx = null;
                if (ownTx)
                {
                    bulkTx = await ctx.RawConnection.BeginTransactionAsync(token).ConfigureAwait(false);
                    ctx.CurrentTransaction = bulkTx;
                }
                try
                {
                    foreach (var entity in entityList)
                        await ctx.CleanupNormManagedChildrenOnDeleteAsync(entity, map, ctx.CurrentTransaction, token).ConfigureAwait(false);
                    var deleted = await _p.BulkDeleteAsync(ctx, map, entityList, token).ConfigureAwait(false);
                    if (bulkTx != null) await bulkTx.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                    return deleted;
                }
                catch
                {
                    if (bulkTx != null) await bulkTx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    throw;
                }
                finally
                {
                    if (bulkTx != null)
                    {
                        ctx.CurrentTransaction = null;
                        await bulkTx.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }, s_nonIdempotentNoRetry, ct);
        }
        #endregion
    }
}
