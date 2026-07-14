using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
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
        /// Configures the context to automatically retry failed <c>SaveChanges</c>
        /// operations when the database reports a deadlock (SQL Server error 1205).
        /// A default retry policy with exponential backoff is applied.
        /// </summary>
        /// <returns>The current <see cref="DbContextOptions"/> instance for fluent configuration.</returns>
        public DbContextOptions UseDeadlockResilientSaveChanges()
        {
            ThrowIfDisposed();
            Options.RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromSeconds(1),
                ShouldRetry = ex =>
                {
                    if (ex is DbException dbEx)
                    {
                        var prop = s_numberPropertyCache.GetOrAdd(dbEx.GetType(), t => t.GetProperty("Number"));
                        return (int?)prop?.GetValue(dbEx) == SqlServerDeadlockErrorNumber;
                    }
                    return false;
                }
            };
            return Options;
        }
        /// <summary>
        /// Persists all tracked changes to the database. The operation is executed
        /// using the configured retry policy which transparently retries transient
        /// failures such as deadlocks.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        /// <remarks>
        /// <para>
        /// Automatically calls DetectChanges() which performs snapshot-based comparison of ALL
        /// tracked entities. Avoid calling SaveChanges() in tight loops with many tracked entities.
        /// For bulk operations, use InsertBulkAsync() or UpdateBulkAsync() instead. For read-only
        /// queries, use AsNoTracking() to avoid change tracking overhead entirely.
        /// </para>
        /// <para>
        /// <b>Async-first:</b> nORM does not provide a synchronous <c>SaveChanges()</c> method.
        /// Always use <c>await ctx.SaveChangesAsync()</c>. Blocking with
        /// <c>.GetAwaiter().GetResult()</c> can cause deadlocks in synchronization contexts
        /// such as ASP.NET classic or WPF.
        /// </para>
        /// </remarks>
        public Task<int> SaveChangesAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            return SaveChangesWithRetryAsync(detectChanges: true, ct);
        }

        /// <summary>
        /// Persists all tracked changes to the database. The operation is executed
        /// using the configured retry policy which transparently retries transient
        /// failures such as deadlocks.
        /// </summary>
        /// <param name="detectChanges">
        /// If true, automatically calls <see cref="ChangeTracker.DetectChanges"/> before saving.
        /// If false, assumes changes have been manually tracked or detected.
        /// Set to false for performance in scenarios with many tracked entities where you've
        /// manually set entity states using <c>context.Entry(entity).State = EntityState.Modified</c>.
        /// </param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        public Task<int> SaveChangesAsync(bool detectChanges, CancellationToken ct = default)
        {
            ThrowIfDisposed();
            return SaveChangesWithRetryAsync(detectChanges, ct);
        }

        /// <summary>
        /// Invokes <see cref="SaveChangesInternalAsync"/> using the configured retry policy to
        /// transparently retry transient failures such as deadlocks.
        /// Exceptions thrown during or after commit are not retried because the commit outcome is unknown.
        /// </summary>
        /// <param name="detectChanges">If true, calls ChangeTracker.DetectChanges before saving.</param>
        /// <param name="ct">Token used to cancel the save operation.</param>
        /// <returns>The number of state entries written to the database.</returns>
        private async Task<int> SaveChangesWithRetryAsync(bool detectChanges, CancellationToken ct)
        {
            var policy = Options.RetryPolicy;
            var maxRetries = policy?.MaxRetries ?? 1;
            var baseDelay = policy?.BaseDelay ?? TimeSpan.Zero;
            var rand = Random.Shared;

            // TX-2: Retrying under a non-owned transaction replays writes inside an external
            // transaction whose rollback path is a no-op here, risking duplicate effects.
            // If an explicit or ambient transaction controls the scope, execute exactly once.
            var hasExternalTransaction = CurrentTransaction != null
                || System.Transactions.Transaction.Current != null;
            if (hasExternalTransaction)
                return await SaveChangesInternalAsync(detectChanges, ct).ConfigureAwait(false);

            for (var attempt = 0; ; attempt++)
            {
                var commitAttempted = false;
                try
                {
                    return await SaveChangesInternalAsync(detectChanges, ct, onCommitAttempted: () => commitAttempted = true).ConfigureAwait(false);
                }
                catch (Exception ex) when (!commitAttempted && attempt < maxRetries - 1 && IsRetryableException(ex))
                {
                    // Only retry pre-commit transient failures - if commit was attempted, the outcome
                    // is unknown and retrying could produce duplicate rows.
                    var backoffMs = baseDelay.TotalMilliseconds * Math.Pow(2, attempt);
                    var jitter = 1 + (rand.NextDouble() * 2 * RetryJitterRange - RetryJitterRange);
                    var delay = TimeSpan.FromMilliseconds(backoffMs * jitter);
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Persists all tracked entity changes to the database within a single transaction.
        /// </summary>
        /// <param name="detectChanges">
        /// If true, calls ChangeTracker.DetectChanges before saving. DetectChanges iterates all
        /// tracked entities and compares current values to original values, which can be expensive
        /// for contexts tracking thousands of entities.
        /// </param>
        /// <param name="ct">Token used to cancel the save operation.</param>
        /// <param name="onCommitAttempted">Optional callback invoked immediately before CommitAsync is called, used by retry logic to detect commit attempts.</param>
        /// <returns>The total number of state entries written to the database.</returns>
        private async Task<int> SaveChangesInternalAsync(bool detectChanges, CancellationToken ct, Action? onCommitAttempted = null)
        {
            // Only detect changes if requested - DetectChanges is O(entities x properties).
            if (detectChanges)
            {
                // Fixup first: the reference direction assigns FK scalars on already-tracked
                // dependents, and detection must see those assignments to mark the rows Modified.
                FixupNavigationChildren();
                CascadeMarkDeletedDependents();
                ChangeTracker.DetectAllChanges();
            }
            var changedEntries = ChangeTracker.Entries
                .Where(e => e.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
                .ToList();
            if (changedEntries.Count == 0)
                return 0;

            var saveInterceptors = Options.SaveChangesInterceptors;
            if (saveInterceptors.Count > 0)
            {
                foreach (var interceptor in saveInterceptors)
                    await interceptor.SavingChangesAsync(this, changedEntries, ct).ConfigureAwait(false);

                // Recompute changedEntries AFTER interceptors run: interceptors may call context.Add()
                // or modify tracked entities during SavingChangesAsync. Re-reading the change tracker
                // ensures those additions and modifications are included in the current save operation.
                // Also re-run DetectAllChanges so that property-level mutations made to previously-Unchanged
                // entities (e.g. audit stamping) are picked up even if entries were not explicitly marked Modified.
                ChangeTracker.DetectAllChanges();
                changedEntries = ChangeTracker.Entries
                    .Where(e => e.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
                    .ToList();
                if (changedEntries.Count == 0)
                    return 0;
            }

            // Snapshot the DB-generated key values of Added entities before the attempt runs. A
            // rolled-back INSERT still leaves the key it assigned on the entity; capturing the
            // pre-attempt value lets the rollback path restore it so the retry re-inserts the row
            // instead of skipping it as already-persisted (silent data loss).
            List<(object Entity, TableMapping Map, object?[] Keys)>? addedKeySnapshot = null;
            foreach (var entry in changedEntries)
            {
                if (entry.State == EntityState.Added
                    && entry.Entity is { } addedEntity
                    && entry.Mapping.KeyColumns.Any(k => k.IsDbGenerated))
                {
                    var m = entry.Mapping;
                    var keys = new object?[m.KeyColumns.Length];
                    for (int i = 0; i < m.KeyColumns.Length; i++)
                        keys[i] = m.KeyColumns[i].Getter(addedEntity);
                    (addedKeySnapshot ??= new()).Add((addedEntity, m, keys));
                }
            }

            await using var transactionManager = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
            ct = transactionManager.Token;
            var transaction = transactionManager.Transaction;

            var totalAffected = 0;
            try
            {
                var allGroups = changedEntries.GroupBy(e => (e.State, e.Mapping))
                    .Select(g => (IGrouping<(EntityState State, TableMapping Mapping), EntityEntry>)new SaveChangesEntryGroup(g.Key, g))
                    .ToList();
                var addedGroups = allGroups.Where(g => g.Key.State == EntityState.Added).ToList();
                var modifiedGroups = allGroups.Where(g => g.Key.State == EntityState.Modified).ToList();
                var deletedGroups = allGroups.Where(g => g.Key.State == EntityState.Deleted).ToList();

                // Replace-in-place: a key that is both Deleted and re-Added in this
                // batch must DELETE before the insert or the primary key's unique
                // constraint fires mid-batch (inserts run first globally). Hoist
                // exactly those deletes into a pre-pass; all other deletes keep
                // their position after inserts and updates, which the FK-repoint
                // scenarios rely on.
                var preDeleteGroups = new List<IGrouping<(EntityState State, TableMapping Mapping), EntityEntry>>();
                if (deletedGroups.Count > 0 && addedGroups.Count > 0)
                {
                    for (var gi = 0; gi < deletedGroups.Count; gi++)
                    {
                        var delGroup = deletedGroups[gi];
                        var map = delGroup.Key.Mapping;
                        if (map.KeyColumns.Length == 0)
                            continue;
                        var addedSame = addedGroups.FirstOrDefault(g => g.Key.Mapping == map);
                        if (addedSame == null)
                            continue;
                        var addedKeys = new HashSet<nORM.Query.GroupJoinOuterIdentity>(
                            addedSame.Select(e => new nORM.Query.GroupJoinOuterIdentity(
                                Array.ConvertAll(map.KeyColumns, k => k.Getter(e.Entity!)))));
                        var replaced = delGroup
                            .Where(e => addedKeys.Contains(new nORM.Query.GroupJoinOuterIdentity(
                                Array.ConvertAll(map.KeyColumns, k => k.Getter(e.Entity!)))))
                            .ToList();
                        if (replaced.Count == 0)
                            continue;
                        var remaining = delGroup.Except(replaced).ToList();
                        preDeleteGroups.Add(new SaveChangesEntryGroup(delGroup.Key, replaced));
                        deletedGroups[gi] = new SaveChangesEntryGroup(delGroup.Key, remaining);
                    }
                    if (preDeleteGroups.Count > 1)
                    {
                        // Children-first, matching the regular delete ordering.
                        var sortedPre = TopologicalSortMappings(preDeleteGroups.Select(g => g.Key.Mapping)).Reverse().ToList();
                        preDeleteGroups = sortedPre.Select(m => preDeleteGroups.First(g => g.Key.Mapping == m)).ToList();
                    }
                }

                var sortedAddedMappings = TopologicalSortMappings(addedGroups.Select(g => g.Key.Mapping)).ToList();
                // Gate D fix: Apply the same topological sort to modified groups so that FK
                // constraints do not fire when a dependent row is updated before its principal.
                // Inserts already follow principal-first order; updates must do the same.
                var sortedModifiedMappings = TopologicalSortMappings(modifiedGroups.Select(g => g.Key.Mapping)).ToList();
                var sortedDeletedMappings = TopologicalSortMappings(deletedGroups.Select(g => g.Key.Mapping)).Reverse().ToList();

                var orderedAddedGroups = sortedAddedMappings.Select(m => addedGroups.First(g => g.Key.Mapping == m));
                var orderedModifiedGroups = sortedModifiedMappings.Select(m => modifiedGroups.First(g => g.Key.Mapping == m));
                var orderedDeletedGroups = sortedDeletedMappings.Select(m => deletedGroups.First(g => g.Key.Mapping == m));
                var orderedGroups = preDeleteGroups.Concat(orderedAddedGroups).Concat(orderedModifiedGroups).Concat(orderedDeletedGroups);

                foreach (var group in orderedGroups)
                {
                    var entries = group.ToList();
                    if (entries.Count == 0)
                        continue;
                    var map = group.Key.Mapping;
                    var state = group.Key.State;
                    if (state is EntityState.Added or EntityState.Modified or EntityState.Deleted)
                        EnsureWritableMapping(map, $"SaveChanges {state}");

                    // Guard against re-inserting entities whose DB-generated key was already
                    // assigned by a previous SaveChanges call (e.g. inside a committed external
                    // transaction where AcceptChanges was not invoked). If the entity is in Added
                    // state but its DB-generated key is already non-default, the INSERT already
                    // committed; skip to avoid duplicate rows.
                    if (state == EntityState.Added && map.KeyColumns.Any(k => k.IsDbGenerated))
                    {
                        entries = entries.Where(e => e.Entity is not null && IsDefaultDbGeneratedKey(e.Entity, map)).ToList();
                        if (entries.Count == 0)
                            continue;
                    }

                    // A self-referential table (Category→Parent, Employee→Manager) is a single
                    // mapping, so TopologicalSortMappings cannot separate parent rows from child
                    // rows. Order the rows within the group so a parent is inserted before the
                    // child that references it — and deleted after — or an in-batch write violates
                    // the self-foreign-key. No-ops for the common non-self-referential mapping.
                    if (state == EntityState.Added)
                        entries = OrderSelfReferentialRows(entries, map, childrenFirst: false);
                    else if (state == EntityState.Deleted)
                        entries = OrderSelfReferentialRows(entries, map, childrenFirst: true);

                    await using var commandScope = new CommandScope(RawConnection, transaction);

                    // Include tenant param in per-entity count for Modified/Deleted so that
                    // batch sizing does not overflow MaxParameters on bounded providers.
                    var tenantParamCount = (Options.TenantProvider != null && map.TenantColumn != null) ? 1 : 0;
                    var paramsPerEntity = state switch
                    {
                        EntityState.Added    => _p.GetInsertColumns(map).Length,
                        EntityState.Modified => map.UpdateColumns.Length + map.KeyColumns.Length + (map.TimestampColumn != null ? 1 : 0) + tenantParamCount,
                        EntityState.Deleted  => map.KeyColumns.Length + (map.TimestampColumn != null ? 1 : 0) + tenantParamCount,
                        _ => 0
                    };
                    var batchSize = CalculateBatchSize(entries.Count, paramsPerEntity);
                    var templateLength = EstimateTemplateLength(state, map);

                    // Reuse DbCommand and StringBuilder across batches: create ONE of each and
                    // clear/reset between batches rather than allocating per-batch.
                    await using var cmd = commandScope.CreateCommand();
                    var sql = new StringBuilder(templateLength * batchSize);

                    // Owned/M2M timing asymmetry:
                    // - DELETE: owned collections and M2M join rows must be removed BEFORE the owner
                    //   entity is deleted, because the child rows hold FK references to the owner.
                    //   Deleting the owner first would violate FK constraints.
                    // - INSERT/UPDATE: owned collections and M2M join rows are saved AFTER the owner
                    //   entity is persisted, because the child rows need the owner's (possibly
                    //   DB-generated) primary key value to populate their FK columns.
                    if (state == EntityState.Deleted)
                    {
                        foreach (var entry in entries)
                        {
                            if (entry.Entity != null)
                            {
                                if (map.OwnedCollections.Count > 0)
                                    await SaveOwnedCollectionsAsync(entry.Entity, map, state, transaction, ct).ConfigureAwait(false);
                                if (map.ManyToManyJoins.Count > 0)
                                    await ExecuteJoinTableSyncAsync(entry.Entity, entry, transaction, ct).ConfigureAwait(false);
                            }
                        }
                    }

                    for (int start = 0; start < entries.Count; start += batchSize)
                    {
                        var batchCount = Math.Min(batchSize, entries.Count - start);
                        var batch = entries.GetRange(start, batchCount);
                        // Clear for reuse
                        sql.Clear();
                        cmd.Parameters.Clear();

                        switch (state)
                        {
                            case EntityState.Added:
                                totalAffected += await ExecuteInsertBatch(cmd, map, batch, sql, 0, ct).ConfigureAwait(false);
                                break;
                            case EntityState.Modified:
                                totalAffected += await ExecuteUpdateBatch(cmd, map, batch, sql, 0, ct).ConfigureAwait(false);
                                break;
                            case EntityState.Deleted:
                                totalAffected += await ExecuteDeleteBatch(cmd, map, batch, sql, 0, ct).ConfigureAwait(false);
                                break;
                        }
                    }

                    // For Added/Modified: save owned collections and M2M join rows AFTER the owner is persisted
                    if (state != EntityState.Deleted)
                    {
                        foreach (var entry in entries)
                        {
                            if (entry.Entity != null)
                            {
                                if (map.OwnedCollections.Count > 0)
                                    await SaveOwnedCollectionsAsync(entry.Entity, map, state, transaction, ct).ConfigureAwait(false);
                                if (map.ManyToManyJoins.Count > 0)
                                    await ExecuteJoinTableSyncAsync(entry.Entity, entry, transaction, ct).ConfigureAwait(false);
                            }
                        }
                    }
                }
                onCommitAttempted?.Invoke();
                await transactionManager.CommitAsync().ConfigureAwait(false);

                // X1 fix: accept tracker state whenever writes committed independently.
                // This covers: owned tx (OwnsTransaction=true), ambient Ignore policy (enlistment
                // intentionally skipped), and ambient BestEffort where enlistment failed (writes
                // committed outside scope). For external explicit transactions or successfully-enlisted
                // ambient scopes, ShouldAcceptChanges=false and the caller controls durability.
                if (transactionManager.ShouldAcceptChanges)
                {
                    // Capture the deleted instances BEFORE detaching them — Remove
                    // resets the entry state, and the navigation cleanup below needs
                    // to know which instances just left the database.
                    List<object>? deletedInstances = null;
                    foreach (var entry in changedEntries)
                    {
                        if (entry.State == EntityState.Deleted)
                        {
                            // Remove deleted entities from the ChangeTracker
                            if (entry.Entity is { } entityToRemove)
                            {
                                (deletedInstances ??= new List<object>()).Add(entityToRemove);
                                ChangeTracker.Remove(entityToRemove, true);
                            }
                        }
                        else
                        {
                            // Mark Added/Modified entities as Unchanged
                            entry.AcceptChanges();
                        }
                    }

                    if (deletedInstances != null)
                        RemoveDeletedInstancesFromTrackedNavigations(deletedInstances);
                }
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                // Without this guard, a rollback failure replaces originalEx entirely,
                // making it impossible to diagnose the root write failure.
                try
                {
                    await transactionManager.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception rollbackEx)
                {
                    throw new AggregateException(
                        "SaveChanges failed and rollback also failed. See inner exceptions for details.",
                        originalEx, rollbackEx);
                }

                // The rollback discarded every row inserted this attempt. Reset the in-memory
                // DB-generated keys those inserts stamped so a retry (or a caller re-saving) treats
                // the entities as never-inserted; otherwise the "skip already-inserted" guard drops
                // them and the rows are silently lost. Only keys mutated during this attempt are
                // reset - an entity carrying a non-default key from a prior committed save is left
                // untouched.
                if (addedKeySnapshot != null)
                {
                    foreach (var (entity, map, keys) in addedKeySnapshot)
                    {
                        var changed = false;
                        for (int i = 0; i < map.KeyColumns.Length; i++)
                        {
                            if (!Equals(map.KeyColumns[i].Getter(entity), keys[i]))
                            {
                                changed = true;
                                break;
                            }
                        }
                        if (changed)
                            ChangeTracker.RollbackGeneratedKeyAssignment(entity, map, keys);
                    }
                }

                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable - satisfies compiler
            }

            // Fire SavedChangesAsync AFTER CommitAsync and AcceptChanges, and OUTSIDE the try/catch
            // block so an interceptor exception does not attempt to roll back an already-committed
            // transaction. Uses CancellationToken.None because ct was replaced by transactionManager.Token
            // (linked to caller token), so a cancellation arriving here would surface as
            // OperationCanceledException even though the DB commit already succeeded.
            // Post-commit interceptors are best-effort notifications and must never surface as a false
            // SaveChanges failure. Each interceptor is wrapped individually so that one failure does
            // not prevent subsequent interceptors from running; failures are logged and suppressed.
            if (saveInterceptors.Count > 0)
            {
                foreach (var interceptor in saveInterceptors)
                {
                    try
                    {
                        await interceptor.SavedChangesAsync(this, changedEntries, totalAffected, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception interceptorEx)
                    {
                        Options.Logger?.LogWarning(interceptorEx,
                            "Interceptor {InterceptorType}.SavedChangesAsync threw after a successful commit. " +
                            "The database changes are committed. The interceptor exception is logged and suppressed " +
                            "to prevent false failure reports and duplicate-retry side effects.",
                            interceptor.GetType().Name);
                    }
                }
            }

            var cache = Options.CacheProvider;
            if (cache != null)
            {
                var tags = new HashSet<string>();
                foreach (var entry in changedEntries)
                {
                    if (entry.Entity is { } entity)
                    {
                        var map = GetMapping(entity.GetType());
                        tags.Add(map.TableName);
                    }
                }
                foreach (var tag in tags)
                    cache.InvalidateTag(tag);
            }
            return totalAffected;
        }

        /// <summary>
        /// Discovers dependents reachable through the collection navigations of tracked, non-deleted
        /// entities that are not yet tracked themselves, tracks each as <see cref="EntityState.Added"/>,
        /// and populates its foreign key from the principal's primary key — the documented
        /// <c>principal.Children.Add(child)</c> relationship-fixup contract. A work queue visits a newly
        /// added child's own collections too, so a deep object graph is added in one SaveChanges. When
        /// the principal's key is DB-generated (still default here), the foreign key is re-propagated
        /// after the principal is inserted (see <see cref="ChangeTracker.PropagateGeneratedKeyToChildren"/>).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship fixup reads navigation properties and resolves dependent mappings via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void FixupNavigationChildren()
        {
            ChangeTracker.ClearPendingReferenceKeyFixups();
            var queue = new Queue<EntityEntry>();
            foreach (var e in ChangeTracker.Entries)
                if (e.Entity != null && e.State != EntityState.Deleted
                    && (e.Mapping.Relations.Count > 0 || e.Mapping.ReferenceNavigations.Length > 0))
                    queue.Enqueue(e);
            if (queue.Count == 0)
                return;

            while (queue.Count > 0)
            {
                var entry = queue.Dequeue();
                var principal = entry.Entity;
                if (principal == null)
                    continue;

                foreach (var relation in entry.Mapping.Relations.Values)
                {
                    if (relation.NavProp.GetValue(principal) is not System.Collections.IEnumerable collection || collection is string)
                        continue;

                    TableMapping? childMapping = null;
                    foreach (var child in collection)
                    {
                        if (child == null || ChangeTracker.GetEntryOrDefault(child) != null)
                            continue;

                        childMapping ??= GetMapping(relation.DependentType);
                        var childEntry = ChangeTracker.Track(child, EntityState.Added, childMapping);

                        // Set the FK from the principal's PK. Correct immediately when the principal's key
                        // is already assigned; re-propagated after insert for DB-generated principal keys.
                        for (int i = 0; i < relation.ForeignKeys.Count && i < relation.PrincipalKeys.Count; i++)
                            relation.ForeignKeys[i].Setter(child, relation.PrincipalKeys[i].Getter(principal));

                        if (childEntry.Mapping.Relations.Count > 0 || childEntry.Mapping.ReferenceNavigations.Length > 0)
                            queue.Enqueue(childEntry);
                    }
                }

                FixupReferenceNavigations(entry, queue);
            }
        }

        /// <summary>
        /// The reference direction of relationship fixup: <c>dependent.Principal = entity</c>.
        /// An untracked principal is discovered and tracked as Added, and the dependent's FK
        /// scalar is aligned with the principal's primary key so the assignment persists. A
        /// null navigation is a no-op — a plain query leaves navigations null while the FK
        /// holds a value, so null cannot mean "sever" without a navigation snapshot; clearing
        /// the FK scalar is the severing gesture. A deliberately edited FK scalar outranks a
        /// stale navigation reference (the navigation may still point at the previously
        /// included principal).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship fixup reads navigation properties and resolves principal mappings via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void FixupReferenceNavigations(EntityEntry entry, Queue<EntityEntry> queue)
        {
            var navProps = entry.Mapping.ReferenceNavigations;
            if (navProps.Length == 0)
                return;
            var dependent = entry.Entity!;

            foreach (var navProp in navProps)
            {
                object? principal;
                try
                {
                    principal = navProp.GetValue(dependent);
                }
                catch
                {
                    continue;
                }
                if (principal == null)
                    continue;

                TableMapping principalMap;
                try
                {
                    principalMap = GetMapping(navProp.PropertyType);
                }
                catch
                {
                    continue;
                }
                if (principalMap.KeyColumns.Length != 1)
                    continue;
                var fk = global::nORM.Query.ExpressionToSqlVisitor.FindReferenceNavForeignKey(
                    entry.Mapping, navProp.Name, navProp.PropertyType, principalMap);
                if (fk == null)
                    continue;

                var principalEntry = ChangeTracker.GetEntryOrDefault(principal);
                if (principalEntry == null)
                {
                    principalEntry = ChangeTracker.Track(principal, EntityState.Added, principalMap);
                    if (principalEntry.Mapping.Relations.Count > 0 || principalEntry.Mapping.ReferenceNavigations.Length > 0)
                        queue.Enqueue(principalEntry);
                }
                else if (principalEntry.State == EntityState.Deleted)
                {
                    continue;
                }

                var pkColumn = principalMap.KeyColumns[0];
                if (pkColumn.IsDbGenerated && principalEntry.State == EntityState.Added
                    && IsDefaultDbGeneratedKey(principal, principalMap))
                {
                    // The key does not exist yet; link after the principal's INSERT hydrates it.
                    ChangeTracker.RegisterPendingReferenceKeyFixup(principal, dependent, fk, pkColumn);
                    if (entry.State is EntityState.Unchanged or EntityState.Modified)
                    {
                        // The FK write lands during SaveChanges itself (after the principal's
                        // insert), which is after change detection — mark the row now or its
                        // UPDATE is skipped and the link is silently lost.
                        entry.State = EntityState.Modified;
                        entry.MarkExplicitlyModified();
                    }
                    continue;
                }

                var pkValue = pkColumn.Getter(principal);
                var fkValue = fk.Getter(dependent);
                if (Equals(fkValue, pkValue))
                    continue;
                if (entry.State != EntityState.Added)
                {
                    if (fk.IsKey)
                        continue;
                    if (entry.HasColumnValueChanged(fk))
                    {
                        // The deliberately edited FK outranks the stale navigation —
                        // and the navigation must be reconciled NOW: left pointing at
                        // the old principal, it would silently re-assert itself on
                        // the NEXT save, where the accepted baseline equals the
                        // edited FK and the edit is no longer visible. Point it at
                        // the tracked principal the FK now references, or null it.
                        var editedFk = fk.Getter(dependent);
                        object? editedPrincipal = editedFk != null
                            ? ChangeTracker.GetEntryByKey(principalMap.Type, editedFk)?.Entity
                            : null;
                        try { navProp.SetValue(dependent, editedPrincipal); }
                        catch { /* read-only navigation — leave the stale reference */ }
                        continue;
                    }
                    entry.State = EntityState.Modified;
                    entry.MarkExplicitlyModified();
                }
                fk.Setter(dependent, pkValue);
            }
        }

        private int CalculateBatchSize(int totalEntries, int paramsPerEntity)
        {
            var batchSize = totalEntries;
            if (_p.MaxParameters != int.MaxValue)
            {
                var maxParams = Math.Max(1, _p.MaxParameters - ParameterBudgetReserve);
                batchSize = Math.Max(1, maxParams / Math.Max(1, paramsPerEntity));
            }
            return batchSize;
        }

        private int EstimateTemplateLength(EntityState state, TableMapping map)
            => state switch
            {
                EntityState.Added => BuildInsertBatch(map, 0).Length + 1,
                EntityState.Modified => BuildUpdateBatch(map, 0).Length + 1,
                EntityState.Deleted => BuildDeleteBatch(map, 0).Length + 1,
                _ => 0
            };


        /// <summary>
        /// <see cref="TimeoutException"/> is intentionally NOT retried by default because a
        /// timed-out write operation may have already been partially applied by the database and
        /// retrying it could produce duplicate rows. Callers that want to retry on timeout must
        /// wrap the timeout condition inside their <see cref="DbContextOptions.RetryPolicy"/>
        /// by mapping the relevant <see cref="DbException"/> to a positive <c>ShouldRetry</c>
        /// result.
        /// </summary>
        private bool IsRetryableException(Exception ex)
        {
            if (ex is DbException dbEx && Options.RetryPolicy != null)
                return Options.RetryPolicy.ShouldRetry(dbEx);
            return false;   // TimeoutException is excluded - retrying a timed-out write can duplicate data.
        }

        /// <summary>
        /// Client-side cascade delete: marks TRACKED dependents of Deleted
        /// principals as Deleted, transitively, for relations configured with
        /// <c>CascadeDelete</c>. Dependents are matched by foreign key value, so
        /// loaded children cascade whether or not they sit in the navigation
        /// collection. Added dependents were never persisted and detach instead.
        /// Unloaded dependents are the database referential action's
        /// responsibility (migrations emit ON DELETE CASCADE for these relations).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Cascade marking reads relation metadata via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Cascade marking reflects over relations; trimming may remove the required members.")]
        private void CascadeMarkDeletedDependents()
        {
            Queue<EntityEntry>? queue = null;
            foreach (var e in ChangeTracker.Entries)
            {
                if (e.State == EntityState.Deleted && e.Mapping.Relations.Count > 0)
                    (queue ??= new Queue<EntityEntry>()).Enqueue(e);
            }
            if (queue == null)
                return;

            var trackedByType = ChangeTracker.Entries
                .GroupBy(e => e.Mapping.Type)
                .ToDictionary(g => g.Key, g => g.ToList());

            while (queue.Count > 0)
            {
                var principalEntry = queue.Dequeue();
                var principal = principalEntry.Entity;
                if (principal == null)
                    continue;

                foreach (var relation in principalEntry.Mapping.Relations.Values)
                {
                    if (!relation.CascadeDelete)
                        continue;
                    if (!trackedByType.TryGetValue(relation.DependentType, out var dependents))
                        continue;

                    var principalKeyValues = new object?[relation.PrincipalKeys.Count];
                    for (var i = 0; i < relation.PrincipalKeys.Count; i++)
                        principalKeyValues[i] = relation.PrincipalKeys[i].Getter(principal);

                    // Added dependents may be linked only by navigation (their FK is
                    // still default until fixup runs — which skips deleted principals,
                    // so without this they would INSERT with a dangling FK).
                    HashSet<object>? collectionMembers = null;
                    if (relation.NavProp.GetValue(principal) is System.Collections.IEnumerable membersEnumerable
                        && membersEnumerable is not string)
                    {
                        foreach (var member in membersEnumerable)
                        {
                            if (member != null)
                                (collectionMembers ??= new HashSet<object>(ReferenceEqualityComparer.Instance)).Add(member);
                        }
                    }

                    foreach (var dependentEntry in dependents)
                    {
                        if (dependentEntry.State is EntityState.Deleted or EntityState.Detached)
                            continue;
                        var dependent = dependentEntry.Entity;
                        if (dependent == null)
                            continue;
                        var matches = true;
                        for (var i = 0; i < relation.ForeignKeys.Count && matches; i++)
                            matches = Equals(relation.ForeignKeys[i].Getter(dependent), principalKeyValues[i]);

                        if (!matches && dependentEntry.State == EntityState.Added)
                        {
                            if (collectionMembers != null && collectionMembers.Contains(dependent))
                            {
                                matches = true;
                            }
                            else
                            {
                                foreach (var navProp in dependentEntry.Mapping.ReferenceNavigations)
                                {
                                    if (navProp.PropertyType != principalEntry.Mapping.Type)
                                        continue;
                                    object? navValue;
                                    try { navValue = navProp.GetValue(dependent); }
                                    catch { continue; }
                                    if (ReferenceEquals(navValue, principal))
                                    {
                                        matches = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if (!matches)
                            continue;

                        if (dependentEntry.State == EntityState.Added)
                        {
                            // Never persisted — nothing to delete; just stop tracking it.
                            ChangeTracker.Remove(dependent);
                            continue;
                        }

                        dependentEntry.State = EntityState.Deleted;
                        if (dependentEntry.Mapping.Relations.Count > 0)
                            queue.Enqueue(dependentEntry);
                    }
                }
            }
        }

        /// <summary>
        /// Strips just-deleted instances out of tracked entities' navigations. A
        /// deleted instance left sitting in a navigation would be re-discovered by
        /// relationship fixup on the NEXT SaveChanges — its tracker entry is gone
        /// by then — and silently re-inserted: a deleted child through a principal's
        /// collection, or a deleted principal through a dependent's reference.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Navigation cleanup reads navigation properties via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Navigation cleanup reflects over navigation properties; trimming may remove the required members.")]
        private void RemoveDeletedInstancesFromTrackedNavigations(IReadOnlyList<object> deleted)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                var entity = entry.Entity;
                if (entity == null)
                    continue;

                foreach (var relation in entry.Mapping.Relations.Values)
                {
                    if (relation.NavProp.GetValue(entity) is not System.Collections.IList list || list.IsReadOnly)
                        continue;
                    foreach (var gone in deleted)
                    {
                        if (relation.DependentType.IsInstanceOfType(gone))
                            list.Remove(gone);
                    }
                }

                foreach (var navProp in entry.Mapping.ReferenceNavigations)
                {
                    object? current;
                    try { current = navProp.GetValue(entity); }
                    catch { continue; }
                    if (current == null)
                        continue;
                    foreach (var gone in deleted)
                    {
                        if (ReferenceEquals(current, gone))
                        {
                            try { navProp.SetValue(entity, null); }
                            catch { /* read-only navigation — leave it */ }
                            break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Materialized grouping used by SaveChanges ordering when a group must be
        /// split (replace-in-place delete hoisting) while keeping the IGrouping
        /// pipeline shape.
        /// </summary>
        private sealed class SaveChangesEntryGroup : List<EntityEntry>, IGrouping<(EntityState State, TableMapping Mapping), EntityEntry>
        {
            public SaveChangesEntryGroup((EntityState State, TableMapping Mapping) key, IEnumerable<EntityEntry> entries)
                : base(entries) => Key = key;

            public (EntityState State, TableMapping Mapping) Key { get; }
        }
    }
}
