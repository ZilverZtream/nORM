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
        #region Change Tracking
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
            return ChangeTracker.Track(entity, EntityState.Added, GetMapping(typeof(T)));
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
            return ChangeTracker.Track(entity, EntityState.Modified, GetMapping(typeof(T)));
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
            return ChangeTracker.Track(entity, EntityState.Deleted, GetMapping(typeof(T)));
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
            // Auto-attaching untracked entities is dangerous � it silently modifies tracking state.
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
                    // Only retry pre-commit transient failures � if commit was attempted, the outcome
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
            // Only detect changes if requested � DetectChanges is O(entities � properties).
            if (detectChanges)
            {
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

            await using var transactionManager = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
            ct = transactionManager.Token;
            var transaction = transactionManager.Transaction;

            var totalAffected = 0;
            try
            {
                var allGroups = changedEntries.GroupBy(e => (e.State, e.Mapping)).ToList();
                var addedGroups = allGroups.Where(g => g.Key.State == EntityState.Added).ToList();
                var modifiedGroups = allGroups.Where(g => g.Key.State == EntityState.Modified).ToList();
                var deletedGroups = allGroups.Where(g => g.Key.State == EntityState.Deleted).ToList();

                var sortedAddedMappings = TopologicalSortMappings(addedGroups.Select(g => g.Key.Mapping)).ToList();
                // Gate D fix: Apply the same topological sort to modified groups so that FK
                // constraints do not fire when a dependent row is updated before its principal.
                // Inserts already follow principal-first order; updates must do the same.
                var sortedModifiedMappings = TopologicalSortMappings(modifiedGroups.Select(g => g.Key.Mapping)).ToList();
                var sortedDeletedMappings = TopologicalSortMappings(deletedGroups.Select(g => g.Key.Mapping)).Reverse().ToList();

                var orderedAddedGroups = sortedAddedMappings.Select(m => addedGroups.First(g => g.Key.Mapping == m));
                var orderedModifiedGroups = sortedModifiedMappings.Select(m => modifiedGroups.First(g => g.Key.Mapping == m));
                var orderedDeletedGroups = sortedDeletedMappings.Select(m => deletedGroups.First(g => g.Key.Mapping == m));
                var orderedGroups = orderedAddedGroups.Concat(orderedModifiedGroups).Concat(orderedDeletedGroups);

                foreach (var group in orderedGroups)
                {
                    var entries = group.ToList();
                    if (entries.Count == 0)
                        continue;
                    var map = group.Key.Mapping;
                    var state = group.Key.State;

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
                    foreach (var entry in changedEntries)
                    {
                        if (entry.State == EntityState.Deleted)
                        {
                            // Remove deleted entities from the ChangeTracker
                            if (entry.Entity is { } entityToRemove)
                                ChangeTracker.Remove(entityToRemove, true);
                        }
                        else
                        {
                            // Mark Added/Modified entities as Unchanged
                            entry.AcceptChanges();
                        }
                    }
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
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable � satisfies compiler
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
        /// Inserts, updates or deletes owned collection items for a single owner entity.
        /// For Added owners: INSERT all items. For Modified owners: DELETE then INSERT.
        /// For Deleted owners: DELETE all items (called BEFORE the owner is deleted).
        /// </summary>
        private async Task SaveOwnedCollectionsAsync(object owner, TableMapping ownerMap, EntityState ownerState, DbTransaction? transaction, CancellationToken ct)
        {
            if (ownerMap.KeyColumns.Length == 0) return;

            foreach (var ownedMap in ownerMap.OwnedCollections)
            {
                // Resolve which owner key column the FK on the owned table references.
                // For single-key owners this is trivial; for composite-key owners we use
                // name matching to find the right key column rather than always using index 0.
                var ownerKeyCol = ResolveOwnerKeyColumnForOwnedFk(ownerMap.KeyColumns, ownedMap.ForeignKeyColumn, ownerMap.Type.Name);
                var ownerKey = ownerKeyCol.Getter(owner);
                if (ownerKey == null) continue;

                if (ownerState == EntityState.Modified || ownerState == EntityState.Deleted)
                {
                    // DELETE existing owned items � use a dedicated command so that the
                    // INSERT command below starts fully fresh (no prepared-statement residue).
                    await using var delScope = new CommandScope(RawConnection, transaction);
                    await using var delCmd = delScope.CreateCommand();
                    var delSql = $"DELETE FROM {ownedMap.EscTable} WHERE {ownedMap.EscForeignKeyColumn} = @ownerPk";
                    var dp = delCmd.CreateParameter();
                    dp.ParameterName = "@ownerPk";
                    dp.Value = ownerKey;
                    delCmd.Parameters.Add(dp);

                    // X1: Scope DELETE to current tenant when multi-tenancy is configured
                    // on the owned child table, preventing cross-tenant data destruction.
                    if (Options.TenantProvider != null)
                    {
                        var ownedTenantCol = Array.Find(ownedMap.Columns, c => c.PropName == Options.TenantColumnName)
                            ?? throw new NormConfigurationException(
                                $"TenantProvider is configured, but owned collection '{ownedMap.OwnedType.Name}' " +
                                $"does not map tenant column '{Options.TenantColumnName}'. nORM fails closed for tenant-scoped owned deletes.");
                        delSql += $" AND {ownedTenantCol.EscCol} = @tenantId";
                        var tp = delCmd.CreateParameter();
                        tp.ParameterName = "@tenantId";
                        tp.Value = GetRequiredTenantId(ownedTenantCol, ownedMap.OwnedType, "owned collection delete");
                        delCmd.Parameters.Add(tp);
                    }

                    delCmd.CommandText = delSql;
                    await delCmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }

                if (ownerState == EntityState.Deleted) continue; // no re-insert for deleted owners

                // INSERT all current owned items
                var collection = ownedMap.CollectionGetter(owner);
                if (collection == null) continue;
                var items = ((System.Collections.IEnumerable)collection).Cast<object>().ToList();
                if (items.Count == 0) continue;

                var insertCols = Array.FindAll(ownedMap.Columns, c => !c.IsDbGenerated);
                var colNames = string.Join(", ", insertCols.Select(c => c.EscCol).Prepend(ownedMap.EscForeignKeyColumn));

                // SAVE1: Detect tenant column on owned table once before the INSERT loop.
                Column? insertTenantCol = null;
                object? insertTenantId = null;
                if (Options.TenantProvider != null)
                {
                    insertTenantCol = Array.Find(insertCols, c => c.PropName == Options.TenantColumnName)
                        ?? throw new NormConfigurationException(
                            $"TenantProvider is configured, but owned collection '{ownedMap.OwnedType.Name}' " +
                            $"does not map tenant column '{Options.TenantColumnName}'. nORM fails closed for tenant-scoped owned inserts.");
                    insertTenantId = GetRequiredTenantId(insertTenantCol, ownedMap.OwnedType, "owned collection insert");
                }

                // INSERT each item individually � avoids multi-statement batch issues
                // across providers (e.g. SQLite drivers that stop after the first statement).
                foreach (var item in items)
                {
                    // SAVE1: Validate owned child tenant before INSERT to prevent cross-tenant contamination.
                    if (insertTenantCol != null && insertTenantId != null)
                    {
                        var childTenant = insertTenantCol.Getter(item);
                        if (childTenant == null)
                            throw new NormConfigurationException(
                                $"Tenant ID is required on owned child entity before saving but was null. " +
                                "Explicitly set the tenant ID on the child entity.");
                        if (!TenantIdsEqual(childTenant, insertTenantId))
                            throw new NormConfigurationException(
                                $"Owned child tenant '{childTenant}' does not match current tenant '{insertTenantId}'.");
                    }

                    await using var insScope = new CommandScope(RawConnection, transaction);
                    await using var insCmd = insScope.CreateCommand();
                    var valuePlaceholders = new StringBuilder("@ownerFk");
                    var fkp = insCmd.CreateParameter();
                    fkp.ParameterName = "@ownerFk";
                    fkp.Value = ownerKey;
                    insCmd.Parameters.Add(fkp);
                    int pj = 0;
                    foreach (var col in insertCols)
                    {
                        var pname = $"@op{pj}";
                        valuePlaceholders.Append($", {pname}");
                        var pp = insCmd.CreateParameter();
                        pp.ParameterName = pname;
                        var rawVal = col.Getter(item);
                        if (col.Converter != null) rawVal = col.Converter.ConvertToProvider(rawVal);
                        pp.Value = rawVal ?? DBNull.Value;
                        insCmd.Parameters.Add(pp);
                        pj++;
                    }
                    insCmd.CommandText = $"INSERT INTO {ownedMap.EscTable} ({colNames}) VALUES ({valuePlaceholders});";
                    await insCmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Resolves which owner key column corresponds to the FK column on an owned table.
        /// For single-key owners this is the one key column. For composite-key owners the
        /// method tries: (1) exact column name match, (2) owner-type-name-prefixed match,
        /// then falls back to the first key column.
        /// </summary>
        private static Column ResolveOwnerKeyColumnForOwnedFk(Column[] ownerKeyColumns, string ownedFkColumnName, string ownerTypeName)
        {
            if (ownerKeyColumns.Length == 1) return ownerKeyColumns[0];

            // Try 1: exact FK column name matches a key column name (e.g. FK="OrderId", key="OrderId")
            var match = Array.Find(ownerKeyColumns, c =>
                string.Equals(c.Name, ownedFkColumnName, StringComparison.OrdinalIgnoreCase));
            if (match != null) return match;

            // Try 2: strip owner type name prefix (e.g. FK="OrderId", owner type="Order" ? "Id", key="Id")
            if (ownedFkColumnName.Length > ownerTypeName.Length &&
                ownedFkColumnName.StartsWith(ownerTypeName, StringComparison.OrdinalIgnoreCase))
            {
                var suffix = ownedFkColumnName.Substring(ownerTypeName.Length);
                match = Array.Find(ownerKeyColumns, c =>
                    string.Equals(c.Name, suffix, StringComparison.OrdinalIgnoreCase));
                if (match != null) return match;
            }

            // Fall back to first key column (preserves legacy single-key behavior)
            return ownerKeyColumns[0];
        }

        /// <summary>
        /// Syncs the many-to-many join table rows for a single owner entity.
        /// For Added/Modified owners: computes delta from snapshot, DELETEs removed rows, INSERTs new rows.
        /// For Deleted owners: DELETEs ALL join rows for this entity.
        /// </summary>
        private async Task ExecuteJoinTableSyncAsync(object entity, EntityEntry entry, DbTransaction? transaction, CancellationToken ct)
        {
            var map = entry.Mapping;
            // Build tenant subquery fragment once if multi-tenancy is active on the left entity.
            // This ensures join table DELETE operations are scoped to rows whose left FK belongs to
            // the current tenant, preventing cross-tenant join row modification when different tenants
            // share the same PK space.
            var tenantId = Options.TenantProvider != null ? GetRequiredTenantId(map, "many-to-many sync") : null;
            var leftTenantCol = Options.TenantProvider != null ? RequireTenantColumn(map, "many-to-many sync") : null;
            var leftPkCol = map.KeyColumns.Length > 0 ? map.KeyColumns[0] : null;
            var hasTenantFilter = Options.TenantProvider != null && leftPkCol != null;

            foreach (var jtm in map.ManyToManyJoins)
            {
                var leftPk = jtm.LeftPkGetter(entity);
                if (leftPk == null) continue;

                // Tenant-scoped subquery: ensures the left FK belongs to the current tenant
                var tenantFilter = hasTenantFilter
                    ? $" AND {jtm.EscLeftFkColumn} IN (SELECT {leftPkCol!.EscCol} FROM {map.EscTable} WHERE {leftPkCol.EscCol} = {_p.ParamPrefix}lpk AND {leftTenantCol!.EscCol} = {_p.ParamPrefix}jtenant)"
                    : "";

                await using var cmdScope = new CommandScope(RawConnection, transaction);
                await using var cmd = cmdScope.CreateCommand();

                if (entry.State == EntityState.Deleted)
                {
                    // Delete all join rows for this entity, scoped to current tenant
                    cmd.CommandText = $"DELETE FROM {jtm.EscTableName} WHERE {jtm.EscLeftFkColumn} = {_p.ParamPrefix}lpk{tenantFilter}";
                    cmd.AddParam($"{_p.ParamPrefix}lpk", leftPk);
                    if (hasTenantFilter)
                        cmd.AddParam($"{_p.ParamPrefix}jtenant", tenantId!);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                    continue;
                }

                // Compute current set of right PKs from the collection.
                // SEC1: Also keep a pk?entity map so we can validate right-entity tenant before INSERT.
                var collection = jtm.LeftCollectionGetter(entity);
                var currentSet = new HashSet<object>();
                var rightEntityByPk = new Dictionary<object, object>();
                if (collection != null)
                {
                    foreach (var item in collection)
                    {
                        if (item == null) continue;
                        var rpk = jtm.RightPkGetter(item);
                        if (rpk != null)
                        {
                            currentSet.Add(rpk);
                            rightEntityByPk[rpk] = item;
                        }
                    }
                }

                // For Added entities, snapshot is irrelevant � insert everything.
                // For Modified entities, use the snapshot to compute delta.
                HashSet<object> snapshot;
                if (entry.State == EntityState.Added ||
                    entry.ManyToManySnapshots == null ||
                    !entry.ManyToManySnapshots.TryGetValue(jtm.LeftNavPropertyName, out var snap))
                {
                    snapshot = new HashSet<object>();
                }
                else
                {
                    snapshot = snap;
                }

                var toAdd = currentSet.Except(snapshot).ToList();
                var toRemove = snapshot.Except(currentSet).ToList();

                // Tenant filter for individual deletes uses the same lpk param name
                var tenantFilterIndiv = hasTenantFilter
                    ? $" AND {jtm.EscLeftFkColumn} IN (SELECT {leftPkCol!.EscCol} FROM {map.EscTable} WHERE {leftPkCol.EscCol} = {_p.ParamPrefix}lp AND {leftTenantCol!.EscCol} = {_p.ParamPrefix}jtenant)"
                    : "";

                // DELETE removed join rows, scoped to current tenant
                foreach (var removedPk in toRemove)
                {
                    cmd.CommandText = $"DELETE FROM {jtm.EscTableName} WHERE {jtm.EscLeftFkColumn} = {_p.ParamPrefix}lp AND {jtm.EscRightFkColumn} = {_p.ParamPrefix}rp{tenantFilterIndiv}";
                    cmd.Parameters.Clear();
                    cmd.AddParam($"{_p.ParamPrefix}lp", leftPk);
                    cmd.AddParam($"{_p.ParamPrefix}rp", removedPk);
                    if (hasTenantFilter)
                        cmd.AddParam($"{_p.ParamPrefix}jtenant", tenantId!);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }

                // INSERT new join rows (idempotent / ignore duplicates)
                foreach (var addedPk in toAdd)
                {
                    // SEC1: Validate right entity tenant before inserting the join row to prevent
                    // cross-tenant join-table contamination when tenancy is active on the left side.
                    if (Options.TenantProvider != null && rightEntityByPk.TryGetValue(addedPk, out var rightEntity))
                    {
                        var rightMap = GetMapping(jtm.RightType);
                        var rightTenantCol = RequireTenantColumn(rightMap, "many-to-many sync right side");
                        var rightTenant = rightTenantCol.Getter(rightEntity);
                        if (rightTenant == null)
                            throw new NormConfigurationException(
                                $"Cannot add M2M relation: related entity '{jtm.RightType.Name}' has null tenant ID. " +
                                "Explicitly set the tenant ID on the related entity.");
                        if (!TenantIdsEqual(rightTenant, tenantId!))
                            throw new NormConfigurationException(
                                $"Cannot add cross-tenant M2M relation: related entity tenant '{rightTenant}' " +
                                $"does not match current tenant '{tenantId}'.");
                    }

                    cmd.Parameters.Clear();
                    var p1 = $"{_p.ParamPrefix}lp";
                    var p2 = $"{_p.ParamPrefix}rp";
                    cmd.CommandText = Provider.GetInsertOrIgnoreSql(
                        jtm.EscTableName, jtm.EscLeftFkColumn, jtm.EscRightFkColumn, p1, p2);
                    cmd.AddParam(p1, leftPk);
                    cmd.AddParam(p2, addedPk);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Loads owned collection items for all given owner entities using a single IN-query per collection.
        /// </summary>
        internal async Task LoadOwnedCollectionsAsync(System.Collections.IList owners, TableMapping ownerMap, CancellationToken ct)
        {
            if (ownerMap.KeyColumns.Length == 0 || owners.Count == 0) return;

            foreach (var ownedMap in ownerMap.OwnedCollections)
            {
                // Resolve which owner key column this FK references (composite-key aware).
                var pkCol = ResolveOwnerKeyColumnForOwnedFk(ownerMap.KeyColumns, ownedMap.ForeignKeyColumn, ownerMap.Type.Name);

                // Build PK ? owner lookup keyed by the FK-referenced key column value.
                var ownerByPk = new Dictionary<object, object>(owners.Count);
                foreach (var owner in owners)
                {
                    if (owner == null) continue;
                    var pk = pkCol.Getter(owner);
                    if (pk != null && !ownerByPk.ContainsKey(pk))
                        ownerByPk[pk] = owner;
                }
                if (ownerByPk.Count == 0) continue;
                // SELECT owned cols + fk_col FROM child_table WHERE fk_col IN (@p0, @p1, ...)
                var pks = ownerByPk.Keys.ToArray();
                var sqlBuilder = new StringBuilder();
                sqlBuilder.Append("SELECT ");
                for (int ci = 0; ci < ownedMap.Columns.Length; ci++)
                {
                    if (ci > 0) sqlBuilder.Append(", ");
                    sqlBuilder.Append(ownedMap.Columns[ci].EscCol);
                }
                if (ownedMap.Columns.Length > 0) sqlBuilder.Append(", ");
                sqlBuilder.Append(ownedMap.EscForeignKeyColumn);
                sqlBuilder.Append(" FROM ").Append(ownedMap.EscTable)
                          .Append(" WHERE ").Append(ownedMap.EscForeignKeyColumn).Append(" IN (");
                for (int pi = 0; pi < pks.Length; pi++)
                {
                    if (pi > 0) sqlBuilder.Append(", ");
                    sqlBuilder.Append(_p.ParamPrefix).Append("lpk").Append(pi);
                }
                sqlBuilder.Append(')');

                // X1: Scope SELECT to current tenant when multi-tenancy is configured
                // on the owned child table, preventing cross-tenant data leakage.
                Column? ownedTenantColLoad = null;
                if (Options.TenantProvider != null)
                    ownedTenantColLoad = Array.Find(ownedMap.Columns, c => c.PropName == Options.TenantColumnName)
                        ?? throw new NormConfigurationException(
                            $"TenantProvider is configured, but owned collection '{ownedMap.OwnedType.Name}' " +
                            $"does not map tenant column '{Options.TenantColumnName}'. nORM fails closed for tenant-scoped owned loads.");
                if (ownedTenantColLoad != null)
                    sqlBuilder.Append(" AND ").Append(ownedTenantColLoad.EscCol)
                              .Append(" = ").Append(_p.ParamPrefix).Append("tenantId");
                var querySql = sqlBuilder.ToString();

                await using var cmd = CreateCommand();
                cmd.CommandText = querySql;
                cmd.CommandTimeout = ToSecondsClamped(Options.TimeoutConfiguration.BaseTimeout);
                for (int i = 0; i < pks.Length; i++)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = _p.ParamPrefix + "lpk" + i;
                    p.Value = pks[i];
                    cmd.Parameters.Add(p);
                }
                if (ownedTenantColLoad != null)
                {
                    var tp = cmd.CreateParameter();
                    tp.ParameterName = _p.ParamPrefix + "tenantId";
                    tp.Value = GetRequiredTenantId(ownedTenantColLoad, ownedMap.OwnedType, "owned collection load");
                    cmd.Parameters.Add(tp);
                }

                // Initialize empty collections on all owners first
                foreach (var owner in owners)
                {
                    if (owner == null) continue;
                    var existing = ownedMap.CollectionGetter(owner);
                    if (existing == null)
                        ownedMap.CollectionSetter(owner, Activator.CreateInstance(typeof(List<>).MakeGenericType(ownedMap.OwnedType)));
                }

                int fkOrdinal = ownedMap.Columns.Length; // FK is the last column in our SELECT
                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    // Materialize owned item
                    var item = Activator.CreateInstance(ownedMap.OwnedType)!;
                    for (int ci = 0; ci < ownedMap.Columns.Length; ci++)
                    {
                        var col = ownedMap.Columns[ci];
                        if (reader.IsDBNull(ci)) continue;
                        var raw = reader.GetValue(ci);
                        object? converted;
                        if (col.Converter != null)
                            converted = col.Converter.ConvertFromProvider(raw);
                        else
                            converted = ConvertSimple(raw, col.Prop.PropertyType);
                        col.Setter(item, converted);
                    }

                    // Read FK and assign to owner.
                    // Type coercion fallback: ADO.NET providers may return the FK value in a different
                    // numeric type than the CLR PK property (e.g. SQLite returns Int64 for all integers
                    // while the PK property may be Int32). The initial TryGetValue uses the raw provider
                    // type for a zero-allocation fast path; on miss, ConvertSimple coerces to the PK
                    // property type so the dictionary lookup succeeds across type-width mismatches.
                    if (reader.IsDBNull(fkOrdinal)) continue;
                    var fkVal = reader.GetValue(fkOrdinal);
                    if (!ownerByPk.TryGetValue(fkVal, out var ownerEntity))
                    {
                        fkVal = ConvertSimple(fkVal, pkCol.Prop.PropertyType)!;
                        if (fkVal == null || !ownerByPk.TryGetValue(fkVal, out ownerEntity)) continue;
                    }

                    var col2 = ownedMap.CollectionGetter(ownerEntity);
                    if (col2 is System.Collections.IList list)
                        list.Add(item);
                }
            }
        }

        /// <summary>Converts a DB value to the target CLR type using safe fallback logic.</summary>
        private static object? ConvertSimple(object raw, Type targetType)
        {
            if (raw == null || raw == DBNull.Value) return null;
            var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (raw.GetType() == underlying) return raw;
            try { return Convert.ChangeType(raw, underlying); }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException) { return raw; }
        }

        /// <summary>
        /// Returns true if any key column in the array has <c>IsDbGenerated == true</c>.
        /// Uses a for-loop to avoid LINQ enumerator allocation on the insert hot path.
        /// </summary>
        internal static bool HasDbGeneratedKey(Column[] keyColumns)
        {
            for (int i = 0; i < keyColumns.Length; i++)
            {
                if (keyColumns[i].IsDbGenerated)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Returns true when every DB-generated key column on the entity still holds its
        /// default value (0 for integer types, <see cref="Guid.Empty"/> for GUIDs, null
        /// for nullable types) — meaning no INSERT has been committed for this entity yet.
        /// </summary>
        private static bool IsDefaultDbGeneratedKey(object entity, TableMapping map)
        {
            foreach (var k in map.KeyColumns)
            {
                if (!k.IsDbGenerated) continue;
                var val = k.Getter(entity);
                if (val is null) continue;
                var underlying = Nullable.GetUnderlyingType(k.Prop.PropertyType) ?? k.Prop.PropertyType;
                if (underlying == typeof(int)  && (int)val  != 0) return false;
                if (underlying == typeof(long) && (long)val != 0L) return false;
                if (underlying == typeof(Guid) && (Guid)val != Guid.Empty) return false;
                if (!val.Equals(Activator.CreateInstance(underlying))) return false;
            }
            return true;
        }

        /// <summary>
        /// Builds and executes a batched INSERT command for the provided entities.
        /// </summary>
        /// <param name="cmd">The command used to execute the batch.</param>
        /// <param name="map">Mapping information for the target table.</param>
        /// <param name="batch">Entities to insert.</param>
        /// <param name="sql">Reusable <see cref="StringBuilder"/> for composing the SQL batch.</param>
        /// <param name="paramIndex">Starting parameter index for parameter naming.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected.</returns>
        private async Task<int> ExecuteInsertBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct)
        {
            // Providers like MySQL expose the generated key directly on the command object
            // (LastInsertedId) rather than through a reader result set. Batching multiple
            // INSERT + SELECT LAST_INSERT_ID() statements produces result-set ordering that
            // the generic reader loop below cannot correctly attribute to individual entities,
            // so execute them one at a time and read the key after each.
            if (HasDbGeneratedKey(map.KeyColumns) && _p.SupportsCommandGeneratedKeyRetrieval)
            {
                var insertSql = _p.BuildInsert(map, false);
                cmd.CommandText = insertSql;
                cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, insertSql));
                int keysAssigned = 0;
                foreach (var entry in batch)
                {
                    var entity = entry.Entity ?? throw new InvalidOperationException("Entity is null");
                    cmd.Parameters.Clear();
                    AddParametersOptimized(cmd, map, entity, WriteOperation.Insert);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                    var newId = _p.GetCommandGeneratedKey(cmd, map);
                    if (newId != null && newId != DBNull.Value)
                    {
                        map.SetPrimaryKey(entity, newId);
                        ChangeTracker.ReindexAfterInsert(entity, map);
                        keysAssigned++;
                    }
                }
                if (keysAssigned != batch.Count)
                    throw new InvalidOperationException(
                        $"Generated key mismatch: expected {batch.Count} keys for inserted " +
                        $"'{map.Type.Name}' entities but only {keysAssigned} were assigned. " +
                        $"Identity retrieval SQL: '{_p.GetIdentityRetrievalString(map)}'. " +
                        "Possible causes: trigger interference, driver quirk, or partial batch execution. " +
                        "The transaction will be rolled back.");
                return batch.Count;
            }

            foreach (var entry in batch)
            {
                sql.Append(BuildInsertBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entry.Entity ?? throw new InvalidOperationException("Entity is null"),
                    WriteOperation.Insert, paramIndex);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, cmd.CommandText));
            // Skip PrepareAsync when identity retrieval is needed: the INSERT SQL contains
            // multiple result-set-returning statements (e.g. "; SELECT LAST_INSERT_ID();")
            // and some providers (MySQL) do not support preparing multi-statement commands.
            if (batch.Count > 1 && !HasDbGeneratedKey(map.KeyColumns))
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            if (HasDbGeneratedKey(map.KeyColumns))
            {
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, ct).ConfigureAwait(false);
                int i = 0;
                int keysAssigned = 0; // track actual key assignments to detect partial results
                do
                {
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var newId = reader.GetValue(0);
                        var entity = batch[i].Entity;
                        if (entity != null)
                        {
                            map.SetPrimaryKey(entity, newId);
                            // When the provider includes a server-assigned timestamp as a second
                            // OUTPUT column (e.g. SQL Server ROWVERSION), read it back now so the
                            // entity's in-memory token matches the DB and the first UPDATE succeeds.
                            if (reader.FieldCount > 1 && map.TimestampColumn != null)
                            {
                                var tsValue = reader.GetValue(1);
                                if (tsValue != DBNull.Value)
                                    map.TimestampColumn.Setter(entity, tsValue);
                            }
                            ChangeTracker.ReindexAfterInsert(entity, map);
                            keysAssigned++;
                        }
                    }
                    // AcceptChanges is intentionally deferred until after commit.
                    i++;
                }
                while (await reader.NextResultAsync(ct).ConfigureAwait(false) && i < batch.Count);

                // If DB returned fewer keys than entities, identity map is corrupt.
                // Throwing here triggers the SaveChanges catch block → rollback.
                if (keysAssigned != batch.Count)
                {
                    var identitySql = _p.GetIdentityRetrievalString(map);
                    throw new InvalidOperationException(
                        $"Generated key mismatch: expected {batch.Count} keys for inserted " +
                        $"'{map.Type.Name}' entities but only {keysAssigned} were assigned. " +
                        $"Identity retrieval SQL: '{identitySql}'. " +
                        "Possible causes: trigger interference, driver quirk, or partial batch execution. " +
                        "The transaction will be rolled back.");
                }

                return reader.RecordsAffected;
            }

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // AcceptChanges is intentionally deferred until after commit.
            return affected;
        }

        /// <summary>
        /// Builds and executes a batched UPDATE statement for the provided entities.
        /// </summary>
        /// <param name="cmd">The command used to execute the batch.</param>
        /// <param name="map">Mapping information for the target table.</param>
        /// <param name="batch">Entities to update.</param>
        /// <param name="sql">Reusable <see cref="StringBuilder"/> for composing the SQL batch.</param>
        /// <param name="paramIndex">Starting parameter index for parameter naming.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        private async Task<int> ExecuteUpdateBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct)
        {
            // S1 enforcement: warn or throw when the provider uses affected-row semantics with OCC tokens.
            // Affected-row semantics (MySQL default) cannot detect OCC conflicts where the concurrent
            // writer sets the token to the same value. RequireMatchedRowOccSemantics opts into strict mode.
            if (map.TimestampColumn != null && Provider.UseAffectedRowsSemantics)
            {
                if (Options.RequireMatchedRowOccSemantics)
                    throw new NormConfigurationException(
                        $"Entity '{map.Type.Name}' uses optimistic concurrency tokens ([Timestamp]) but " +
                        "the provider is configured with affected-row semantics (UseAffectedRowsSemantics=true). " +
                        "Affected-row semantics cannot detect conflicts where a concurrent writer sets the token " +
                        "to the same value. To fix: add 'useAffectedRows=false' to the MySQL connection string " +
                        "and override the provider, or set DbContextOptions.RequireMatchedRowOccSemantics=false " +
                        "to suppress this error and accept the known trade-off (S1).");
                else
                    Options.Logger?.LogWarning(
                        "S1: Entity '{EntityType}' uses OCC tokens but the provider uses affected-row semantics. " +
                        "Stale-write conflicts where the new token equals the original token will NOT be detected. " +
                        "Set RequireMatchedRowOccSemantics=true to enforce strict OCC, or add useAffectedRows=false " +
                        "to the MySQL connection string for full OCC guarantees.",
                        map.Type.Name);
            }

            foreach (var entry in batch)
            {
                var entity = entry.Entity ?? throw new InvalidOperationException("Entity is null");

                // Detect if the primary key was mutated after tracking.
                // Updating with a mutated key would target the wrong row.
                if (entry.OriginalKey != null && map.KeyColumns.Length > 0)
                {
                    object? currentKey;
                    if (map.KeyColumns.Length == 1)
                    {
                        currentKey = map.KeyColumns[0].Getter(entity);
                    }
                    else
                    {
                        var vals = new object?[map.KeyColumns.Length];
                        for (int i = 0; i < map.KeyColumns.Length; i++)
                            vals[i] = map.KeyColumns[i].Getter(entity);
                        currentKey = vals;
                    }

                    bool pkChanged;
                    if (currentKey is object?[] currentArr && entry.OriginalKey is object?[] origArr)
                        pkChanged = !currentArr.SequenceEqual(origArr);
                    else
                        pkChanged = !Equals(currentKey, entry.OriginalKey);

                    if (pkChanged)
                        throw new InvalidOperationException(
                            $"Primary key mutation detected on entity '{map.Type.Name}'. " +
                            "Primary keys cannot be changed after an entity is tracked. " +
                            "Detach the entity, modify the key, then re-attach.");
                }

                sql.Append(BuildUpdateBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entity,
                    WriteOperation.Update, paramIndex, entry.OriginalToken);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Update, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var updated = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // S1 � Optimistic-concurrency rowcount check.
            // For matched-row providers (UseAffectedRowsSemantics=false): 0 rows updated means
            // the WHERE clause (pk + token) did not match, so the token was stale � throw.
            // For affected-row providers (UseAffectedRowsSemantics=true, e.g. MySQL default):
            // 0 rows can mean either a genuine stale token OR a same-value update (no columns
            // actually changed). Disambiguate with a SELECT-then-verify: query for rows that
            // still carry the original token. If all tokens still match, it was a same-value
            // update with no conflict. If any token is missing, it's a genuine stale-row conflict.
            if (map.TimestampColumn != null && updated != batch.Count)
            {
                if (Provider.UseAffectedRowsSemantics)
                    await VerifyUpdateOccAsync(cmd, map, batch, ct).ConfigureAwait(false);
                else
                    throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            }
            // AcceptChanges is intentionally deferred until after the transaction commits.
            return updated;
        }

        /// <summary>
        /// Builds and executes a batched DELETE statement for the provided entities.
        /// </summary>
        /// <param name="cmd">The command used to execute the batch.</param>
        /// <param name="map">Mapping information for the target table.</param>
        /// <param name="batch">Entities to delete.</param>
        /// <param name="sql">Reusable <see cref="StringBuilder"/> for composing the SQL batch.</param>
        /// <param name="paramIndex">Starting parameter index for parameter naming.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        private async Task<int> ExecuteDeleteBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct)
        {
            foreach (var entry in batch)
            {
                var entity = entry.Entity ?? throw new InvalidOperationException("Entity is null");

                // Detect if the primary key was mutated after tracking.
                // Deleting with a mutated key would target the wrong row.
                if (entry.OriginalKey != null && map.KeyColumns.Length > 0)
                {
                    object? currentKey;
                    if (map.KeyColumns.Length == 1)
                        currentKey = map.KeyColumns[0].Getter(entity);
                    else
                    {
                        var vals = new object?[map.KeyColumns.Length];
                        for (int i = 0; i < map.KeyColumns.Length; i++)
                            vals[i] = map.KeyColumns[i].Getter(entity);
                        currentKey = vals;
                    }
                    bool pkChanged;
                    if (currentKey is object?[] currentArr && entry.OriginalKey is object?[] origArr)
                        pkChanged = !currentArr.SequenceEqual(origArr);
                    else
                        pkChanged = !Equals(currentKey, entry.OriginalKey);
                    if (pkChanged)
                        throw new InvalidOperationException(
                            $"Primary key mutation detected on entity '{map.Type.Name}'. " +
                            "Primary keys cannot be changed after an entity is tracked. " +
                            "Detach the entity, modify the key, then re-attach.");
                }

                sql.Append(BuildDeleteBatch(map, paramIndex)).Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entity,
                    WriteOperation.Delete, paramIndex, entry.OriginalToken);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Delete, cmd.CommandText));
            if (batch.Count > 1)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // S1 � DELETE rowcount check. Unlike UPDATE, DELETE has no same-value ambiguity:
            // a row is "deleted" only if it actually existed and was removed. Even on affected-row
            // providers (UseAffectedRowsSemantics=true), 0 deleted rows always means either the
            // token was stale or the row was already gone � both are genuine conflicts. No
            // SELECT-then-verify is needed; we always throw when deleted != batch.Count.
            if (map.TimestampColumn != null && deleted != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            // Entity removal from ChangeTracker is deferred until after the transaction commits.
            return deleted;
        }

        /// <summary>
        /// S1 � SELECT-then-verify fallback for affected-row semantics providers (e.g. MySQL).
        /// Called from <see cref="ExecuteUpdateBatch"/> when <c>UseAffectedRowsSemantics=true</c>
        /// and the UPDATE rowcount does not match the batch size. Queries the database for rows
        /// that still carry their original concurrency tokens:
        /// <list type="bullet">
        ///   <item>If all tokens still match (<c>count == batch.Count</c>): the UPDATE returned 0
        ///     because no column values actually changed (same-value update). No conflict � return.</item>
        ///   <item>If any token is missing (<c>count &lt; batch.Count</c>): a competing writer
        ///     changed the token. Genuine stale-row conflict � throw <see cref="DbConcurrencyException"/>.</item>
        /// </list>
        /// </summary>
        private async Task VerifyUpdateOccAsync(DbCommand batchCmd, TableMapping map, List<EntityEntry> batch, CancellationToken ct)
        {
            // Guard: each entity consumes (KeyColumns.Length + 1) parameters for the OCC verify
            // query, plus 1 optional tenant parameter. Ensure we don't exceed the provider's limit.
            var paramsPerEntity = map.KeyColumns.Length + 1; // PK columns + timestamp token
            var totalParams = paramsPerEntity * batch.Count
                + (Options.TenantProvider != null && map.TenantColumn != null ? 1 : 0);
            if (_p.MaxParameters != int.MaxValue && totalParams > _p.MaxParameters)
                throw new InvalidOperationException(
                    $"OCC verification for '{map.Type.Name}' requires {totalParams} parameters " +
                    $"but the provider allows at most {_p.MaxParameters}. Reduce batch size.");

            await using var cmd = _cn.CreateCommand();
            if (batchCmd.Transaction != null)
                cmd.Transaction = batchCmd.Transaction;

            var tc = map.TimestampColumn!;
            var sb = new StringBuilder("SELECT COUNT(*) FROM ").Append(map.EscTable).Append(" WHERE ");
            var conditions = new List<string>(batch.Count);
            int pi = 0;

            foreach (var entry in batch)
            {
                var entity = entry.Entity!;
                var pkConds = new List<string>(map.KeyColumns.Length + 1);
                foreach (var kc in map.KeyColumns)
                {
                    pkConds.Add($"{kc.EscCol}={_p.ParamPrefix}v{pi}");
                    cmd.AddParam($"{_p.ParamPrefix}v{pi++}", kc.Getter(entity));
                }
                // Null-safe token equality matches the predicate used in BuildUpdateBatch/BuildDeleteBatch.
                pkConds.Add($"({tc.EscCol}={_p.ParamPrefix}v{pi} OR ({tc.EscCol} IS NULL AND {_p.ParamPrefix}v{pi} IS NULL))");
                var tok = entry.OriginalToken;
                cmd.AddParam($"{_p.ParamPrefix}v{pi++}", tok ?? (object)DBNull.Value);
                conditions.Add($"({string.Join(" AND ", pkConds)})");
            }

            // X1 fix: Parenthesize the OR-chain so the tenant predicate applies to ALL
            // disjuncts, not just the last one. Without parens, SQL AND binds tighter than OR,
            // causing the tenant restriction to apply only to the final row condition.
            if (Options.TenantProvider != null)
            {
                var tenantCol = RequireTenantColumn(map, "OCC batch verification");
                sb.Append('(');
                sb.Append(string.Join(" OR ", conditions));
                sb.Append($") AND {tenantCol.EscCol} = {_p.ParamPrefix}tenantVerify");
                cmd.AddParam($"{_p.ParamPrefix}tenantVerify", GetRequiredTenantId(map, "OCC batch verification"));
            }
            else
            {
                sb.Append(string.Join(" OR ", conditions));
            }

            cmd.CommandText = sb.ToString();

            // Guard against null scalar result � Convert.ToInt32(null) throws ArgumentNullException
            // X2: route through interceptor pipeline so observability tools see verification queries.
            var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
            var matchCount = scalarResult == null || scalarResult is DBNull ? 0 : Convert.ToInt32(scalarResult);
            if (matchCount != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
        }

        /// <summary>
        /// S1 � SELECT-then-verify for the single-entity direct UpdateAsync path on affected-row
        /// semantics providers (e.g. MySQL). Called when <c>recordsAffected == 0</c> and
        /// <c>UseAffectedRowsSemantics=true</c> to distinguish a same-value update (token still
        /// present ? no conflict) from a genuine stale-row conflict (token gone ? throw).
        /// </summary>
        private async Task VerifySingleUpdateOccAsync<T>(
            DbCommand writeCmd,
            TableMapping map,
            T entity,
            object? originalToken,
            CancellationToken ct) where T : class
        {
            var tc = map.TimestampColumn!;
            await using var cmd = _cn.CreateCommand();
            if (writeCmd.Transaction != null)
                cmd.Transaction = writeCmd.Transaction;

            int pi = 0;
            var conditions = new List<string>(map.KeyColumns.Length + 1);
            foreach (var kc in map.KeyColumns)
            {
                conditions.Add($"{kc.EscCol}={_p.ParamPrefix}v{pi}");
                cmd.AddParam($"{_p.ParamPrefix}v{pi++}", kc.Getter(entity));
            }
            // Null-safe token equality mirrors the predicate in BuildUpdate/BuildUpdateBatch.
            conditions.Add($"({tc.EscCol}={_p.ParamPrefix}v{pi} OR ({tc.EscCol} IS NULL AND {_p.ParamPrefix}v{pi} IS NULL))");
            var tok = originalToken ?? tc.Getter(entity);
            cmd.AddParam($"{_p.ParamPrefix}v{pi++}", tok ?? (object)DBNull.Value);

            var sb = new StringBuilder("SELECT COUNT(*) FROM ").Append(map.EscTable).Append(" WHERE ");
            sb.Append(string.Join(" AND ", conditions));
            // SP1: Append tenant predicate so a tenant-blocked direct UPDATE (0 rows because
            // the tenant WHERE clause filtered it out) is correctly classified as a concurrency
            // conflict rather than a benign same-value update. Without this, the verifier may
            // find a foreign-tenant row with the same PK+token and return count=1, masking the error.
            if (Options.TenantProvider != null)
            {
                var tenantCol = RequireTenantColumn(map, "OCC single verification");
                sb.Append($" AND {tenantCol.EscCol} = {_p.ParamPrefix}tenantVerifySingle");
                cmd.AddParam($"{_p.ParamPrefix}tenantVerifySingle", GetRequiredTenantId(map, "OCC single verification"));
            }
            cmd.CommandText = sb.ToString();

            // X2: route through interceptor pipeline so observability tools see verification queries.
            var result = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
            var count = result == null || result is DBNull ? 0 : Convert.ToInt32(result);
            // count=0 means the token is gone (stale); count=1 means same-value update (no conflict).
            if (count == 0)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
        }

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
            return false;   // TimeoutException is excluded � retrying a timed-out write can duplicate data.
        }
        /// <summary>
        /// Converts a <see cref="TimeSpan"/> to an integer number of seconds, clamping at
        /// <see cref="int.MaxValue"/> to prevent overflow when very large or maximum timeouts
        /// are provided. Negative results are raised to a minimum of 1.
        /// </summary>
        private static int ToSecondsClamped(TimeSpan t)
        {
            // Check for overflow before casting
            if (t.TotalSeconds > int.MaxValue)
                return int.MaxValue;

            return Math.Max(1, (int)Math.Ceiling(t.TotalSeconds));
        }
        #endregion
    }
}
