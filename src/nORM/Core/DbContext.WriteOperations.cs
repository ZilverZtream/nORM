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
        #region Standard CRUD
        /// <summary>
        /// Inserts the specified entity into the database asynchronously using any
        /// configured retry policies.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to insert.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> InsertAsync<T>(T entity, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            // Fast-path for the common case: no transaction, no retry policy, no tenant, cached prepared command.
            // This avoids 4 async state machine allocations by inlining the entire chain.
            var map = GetMapping(typeof(T));
            EnsureWritableMapping(map, "InsertAsync");
            var tx = CurrentTransaction;
            if (tx == null && Options.TenantProvider == null && Options.RetryPolicy == null
                && System.Transactions.Transaction.Current == null)
            {
                var key = (map.Type, true);
                if (_preparedInsertCache.TryGetValue(key, out var prepared)
                    && ReferenceEquals(prepared.BoundTransaction, null))
                {
                    // Skip EnableLazyLoading on insert fast path.
                    // Entities being inserted don't need lazy loading proxies - they're being
                    // written to DB, not read from it. Saves ~2-5us ConditionalWeakTable + reflection.
                    return prepared.ExecuteAsync(entity, ct);
                }
            }
            return InsertAsyncSlow(entity, map, tx, ct);
        }

        private async Task<int> InsertAsyncSlow<T>(T entity, TableMapping map, DbTransaction? tx, CancellationToken ct) where T : class
        {
            ValidateTenantContext(entity, map, WriteOperation.Insert);
            var ambientTransaction = tx == null ? System.Transactions.Transaction.Current : null;
            if (tx != null)
            {
                var prepared = await GetOrCreatePreparedInsertCommandAsync(map, tx, true, ct).ConfigureAwait(false);
                NavigationPropertyExtensions.EnableLazyLoading(entity, this);
                return await prepared.ExecuteAsync(entity, ct).ConfigureAwait(false);
            }
            if (ambientTransaction != null)
            {
                await using var ambientScope = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
                var prepared = await GetOrCreatePreparedInsertCommandAsync(map, null, true, ct).ConfigureAwait(false);
                NavigationPropertyExtensions.EnableLazyLoading(entity, this);
                return await prepared.ExecuteAsync(entity, ambientScope.Token).ConfigureAwait(false);
            }
            if (Options.RetryPolicy == null)
            {
                var prepared = await GetOrCreatePreparedInsertCommandAsync(map, null, true, ct).ConfigureAwait(false);
                NavigationPropertyExtensions.EnableLazyLoading(entity, this);
                return await prepared.ExecuteAsync(entity, ct).ConfigureAwait(false);
            }
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            var commitAttempted = false;
            return await _executionStrategy.ExecuteAsync((ctx, token) =>
            {
                commitAttempted = false; // reset per attempt
                return WriteWithTransactionAsync(entity, map, WriteOperation.Insert, null, token,
                    ownsTransaction: true, onCommitAttempted: () => commitAttempted = true);
            }, () => commitAttempted, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Inserts the specified entity within the provided transaction scope.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to insert.</param>
        /// <param name="transaction">Optional transaction used to execute the command.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> InsertAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            if (transaction != null)
                ThrowIfStrictProviderMobilityEscapeHatch("external DbTransaction insert");
            var result = WriteOptimizedAsync(entity, WriteOperation.Insert, ct, transaction);
            NavigationPropertyExtensions.EnableLazyLoading(entity, this);
            return result;
        }

        /// <summary>
        /// Updates the specified entity in the database asynchronously.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to update.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> UpdateAsync<T>(T entity, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            return UpdateAsync(entity, null, ct);
        }

        /// <summary>
        /// Updates the entity within the given transaction.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Transaction to use; if null the context manages one.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> UpdateAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            if (transaction != null)
                ThrowIfStrictProviderMobilityEscapeHatch("external DbTransaction update");
            return WriteOptimizedAsync(entity, WriteOperation.Update, ct, transaction);
        }

        /// <summary>
        /// Deletes the specified entity from the database asynchronously.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            return DeleteAsync(entity, null, ct);
        }

        /// <summary>
        /// Deletes the entity within the supplied transaction.
        /// </summary>
        /// <typeparam name="T">CLR type of the entity.</typeparam>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction for the delete operation.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>The number of affected rows.</returns>
        public Task<int> DeleteAsync<T>(T entity, DbTransaction? transaction, CancellationToken ct = default) where T : class
        {
            ThrowIfDisposed();
            if (transaction != null)
                ThrowIfStrictProviderMobilityEscapeHatch("external DbTransaction delete");
            return WriteOptimizedAsync(entity, WriteOperation.Delete, ct, transaction);
        }

        private enum WriteOperation { Insert, Update, Delete }

        private async Task<int> WriteOptimizedAsync<T>(T entity, WriteOperation operation, CancellationToken ct, DbTransaction? transaction = null) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var map = GetMapping(typeof(T));
            EnsureWritableMapping(map, operation.ToString());
            ValidateTenantContext(entity, map, operation);

            // A tracked entity still pending insertion has no database row: a direct
            // UPDATE has nothing to write (the pending INSERT carries the current
            // values), and a direct DELETE discards the pending insert (add-then-delete
            // is a net no-op). Executing SQL instead would misreport the missing row as
            // a concurrency conflict for [Timestamp] entities, or no-op and then
            // wrongly accept the Added entry, silently dropping the pending INSERT.
            if (operation is WriteOperation.Update or WriteOperation.Delete)
            {
                var pendingEntry = ChangeTracker.GetEntryOrDefault(entity);
                if (pendingEntry != null && ReferenceEquals(pendingEntry.Entity, entity)
                    && pendingEntry.State == EntityState.Added)
                {
                    if (operation == WriteOperation.Delete)
                        ChangeTracker.Remove(entity, true);
                    return 0;
                }
            }

            var tx = transaction ?? CurrentTransaction;
            var ambientTransaction = tx == null ? System.Transactions.Transaction.Current : null;

            if (operation == WriteOperation.Insert)
            {
                if (tx != null)
                    return await ExecuteFastInsert(entity, map, ct, tx).ConfigureAwait(false);

                if (ambientTransaction != null)
                {
                    await using var ambientScope = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
                    return await ExecuteFastInsert(entity, map, ambientScope.Token, null).ConfigureAwait(false);
                }

                if (Options.RetryPolicy == null)
                    return await ExecuteFastInsert(entity, map, ct, null).ConfigureAwait(false);
            }

            if (tx != null)
            {
                return await WriteWithTransactionAsync(entity, map, operation, tx, ct, ownsTransaction: false).ConfigureAwait(false);
            }

            if (ambientTransaction != null)
            {
                await using var ambientScope = await TransactionManager.CreateAsync(this, ct).ConfigureAwait(false);
                return await ExecuteWriteCommandAsync(entity, map, operation, null, ambientScope.Token).ConfigureAwait(false);
            }

            var commitAttempted = false;
            return await _executionStrategy.ExecuteAsync((ctx, token) =>
            {
                commitAttempted = false; // reset per attempt
                return WriteWithTransactionAsync(entity, map, operation, null, token,
                    ownsTransaction: true, onCommitAttempted: () => commitAttempted = true);
            }, () => commitAttempted, ct).ConfigureAwait(false);
        }

        private async Task<int> WriteWithTransactionAsync<T>(T entity, TableMapping map, WriteOperation operation, DbTransaction? transaction, CancellationToken ct, bool ownsTransaction, Action? onCommitAttempted = null) where T : class
        {
            await EnsureConnectionAsync(ct).ConfigureAwait(false);
            // When we own the transaction, track it separately so it can always be disposed
            // in a finally block (releasing server-side lock memory and log space) regardless
            // of whether the operation succeeds or fails.
            DbTransaction currentTransaction;
            DbTransaction? ownedTransaction = null;
            if (ownsTransaction)
            {
                ownedTransaction = await RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
                currentTransaction = ownedTransaction;
            }
            else
            {
                currentTransaction = transaction!;
            }

            try
            {
                var recordsAffected = await ExecuteWriteCommandAsync(entity, map, operation, currentTransaction, ct).ConfigureAwait(false);
                if (ownsTransaction)
                {
                    // Signal that the commit is about to run: a failure from here on has an unknown
                    // outcome and must not be retried, or the write could be duplicated.
                    onCommitAttempted?.Invoke();
                    await currentTransaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                    SyncTrackerAfterDirectWrite(entity, map, operation);
                }
                return recordsAffected;
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                if (ownsTransaction)
                {
                    try
                    {
                        // Use CancellationToken.None so a cancelled caller token does not abort the rollback.
                        await currentTransaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "Write operation failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable - satisfies compiler
            }
            finally
            {
                // Always dispose the owned transaction to release server-side resources.
                if (ownedTransaction != null)
                    await ownedTransaction.DisposeAsync().ConfigureAwait(false);
            }
        }
        private async Task<int> ExecuteFastInsert<T>(T entity, TableMapping map, CancellationToken ct, DbTransaction? transaction) where T : class
        {
            var preparedInsert = await GetOrCreatePreparedInsertCommandAsync(
                map, transaction, hydrateGeneratedKeys: true, ct).ConfigureAwait(false);
            return await preparedInsert.ExecuteAsync(entity, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Inserts an entity whose store-generated convention key is at its default value: the key column is
        /// omitted so the database generates it, and the generated value is read back onto the entity via the
        /// provider's identity-retrieval clause (RETURNING on SQLite). Mirrors how a DB-generated key is
        /// hydrated on the direct write paths. Explicit (non-default) convention keys never reach here — they
        /// insert the value as-is on the normal path. The caller performs any tracker accept (the prepared
        /// path via AcceptTrackedInsert, the transactional path via SyncTrackerAfterDirectWrite).
        /// </summary>
        private async Task<int> ExecuteConventionDefaultInsertAsync(object entity, TableMapping map, DbTransaction? transaction, CancellationToken ct)
        {
            await using var scope = new CommandScope(RawConnection, transaction);
            await using var cmd = scope.CreateCommand();
            var cols = map.InsertColumnsWithoutConventionKey;
            cmd.CommandText = BuildConventionDefaultInsertSql(map, cols);
            cmd.CommandTimeout = ToSecondsClamped(Options.TimeoutConfiguration.BaseTimeout);
            AddParametersOptimized(cmd, map, entity, WriteOperation.Insert, insertColumns: cols);
            var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
            if (newId != null && newId != DBNull.Value)
                map.SetPrimaryKey(entity, newId);
            Options.CacheProvider?.InvalidateTag(map.TableName);
            return 1;
        }

        /// <summary>
        /// Builds the INSERT for a store-generated convention key's default-value row — the key column omitted
        /// (<paramref name="cols"/> = <see cref="TableMapping.InsertColumnsWithoutConventionKey"/>) plus the
        /// provider's identity-retrieval clause to read the generated key back. Uses @PropName parameters to
        /// match <see cref="AddParametersOptimized"/>. Mirrors <see cref="Providers.DatabaseProvider.BuildInsert"/>.
        /// </summary>
        private string BuildConventionDefaultInsertSql(TableMapping map, Column[] cols)
        {
            var prefix = _p.GetIdentityRetrievalPrefix(map);
            var fragment = _p.GetIdentityRetrievalString(map);
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable}{prefix} {_p.DefaultValuesInsertClause}{fragment}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var valParams = string.Join(", ", cols.Select(c => _p.ParamPrefix + c.PropName));
            return $"INSERT INTO {map.EscTable} ({colNames}){prefix} VALUES ({valParams}){fragment}";
        }

        /// <summary>
        /// Plain INSERT for exactly <paramref name="cols"/> with no identity-retrieval clause — used by the
        /// command-generated-key batch path (MySQL), which reads the generated key from the command object
        /// (LAST_INSERT_ID) rather than a returned result set. For the convention key's default-value run
        /// <paramref name="cols"/> omits the key. Uses @PropName parameters to match <see cref="AddParametersOptimized"/>.
        /// </summary>
        private string BuildInsertFromColumns(TableMapping map, Column[] cols)
        {
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable} {_p.DefaultValuesInsertClause}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var valParams = string.Join(", ", cols.Select(c => _p.ParamPrefix + c.PropName));
            return $"INSERT INTO {map.EscTable} ({colNames}) VALUES ({valParams})";
        }

        private async Task<int> ExecuteWriteCommandAsync<T>(
            T entity,
            TableMapping map,
            WriteOperation operation,
            DbTransaction? transaction,
            CancellationToken ct) where T : class
        {
            // S2: Guard against primary key mutation on tracked entities before executing the write.
            // Mirrors the same guard in ExecuteUpdateBatch / ExecuteDeleteBatch.
            if (operation is WriteOperation.Update or WriteOperation.Delete)
            {
                var trackerEntry = ChangeTracker.GetEntryOrDefault(entity);
                if (trackerEntry?.OriginalKey != null && map.KeyColumns.Length > 0)
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
                    if (currentKey is object?[] currentArr && trackerEntry.OriginalKey is object?[] origArr)
                        pkChanged = !currentArr.SequenceEqual(origArr);
                    else
                        pkChanged = !Equals(currentKey, trackerEntry.OriginalKey);

                    if (pkChanged)
                        throw new InvalidOperationException(
                            $"Primary key mutation detected on entity '{map.Type.Name}'. " +
                            "Primary keys cannot be changed after an entity is tracked. " +
                            "Detach the entity, modify the key, then re-attach.");
                }
            }

            await EnsureConnectionAsync(ct).ConfigureAwait(false);

            // Store-generated convention key at its default: omit the key so the database generates it and
            // read the value back, instead of inserting the default (which would collide on a second row).
            // Explicit (non-default) keys fall through to the normal path and are inserted as-is (honored).
            if (operation == WriteOperation.Insert && map.ConventionGeneratedKeyColumn is { } convKey
                && IsDefaultConventionKey(entity, convKey))
                return await ExecuteConventionDefaultInsertAsync(entity, map, transaction, ct).ConfigureAwait(false);

            await using var commandScope = new CommandScope(RawConnection, transaction);
            await using var cmd = commandScope.CreateCommand();
            // X1: pass includeTenant so the WHERE clause targets only the current tenant's rows,
            // matching the predicate parity of the batched SaveChangesAsync path.
            if (Options.TenantProvider != null)
                RequireTenantColumn(map, operation.ToString());
            var includeTenant = Options.TenantProvider != null;
            cmd.CommandText = operation switch
            {
                WriteOperation.Insert => _p.BuildInsert(map),
                WriteOperation.Update => _p.BuildUpdate(map, includeTenant),
                WriteOperation.Delete => _p.BuildDelete(map, includeTenant),
                _ => throw new ArgumentOutOfRangeException(nameof(operation))
            };
            // Simple INSERT/UPDATE/DELETE have no JOINs/subqueries - use base timeout
            // to avoid SQL string scanning in GetAdaptiveTimeout.
            cmd.CommandTimeout = ToSecondsClamped(Options.TimeoutConfiguration.BaseTimeout);
            var originalToken = ChangeTracker.GetEntryOrDefault(entity)?.OriginalToken;
            AddParametersOptimized(cmd, map, entity, operation, originalToken);

            if (operation == WriteOperation.Insert && HasDbGeneratedKey(map.KeyColumns))
            {
                var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
                if (newId != null && newId != DBNull.Value)
                    map.SetPrimaryKey(entity, newId);
                // The active-record Insert/Update/DeleteAsync writes persist changes just like
                // SaveChanges, ExecuteUpdate/Delete and the Bulk* ops, so the result cache for this
                // table must be invalidated or Cacheable() queries keep replaying pre-write rows.
                Options.CacheProvider?.InvalidateTag(map.TableName);
                return 1;
            }

            // Server-generated tokens (ROWVERSION) with application-supplied keys: the INSERT
            // carries an OUTPUT clause returning the generated token. Read it back so the
            // entity's first UPDATE or DELETE compares against the current value instead of
            // throwing a false stale-token conflict.
            if (operation == WriteOperation.Insert && map.TimestampColumn != null
                && _p.SupportsNativeRowVersion && _p.GetInsertTokenOutputClause(map).Length > 0)
            {
                var tokenValue = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
                if (tokenValue != null && tokenValue != DBNull.Value)
                    map.TimestampColumn.Setter(entity, tokenValue);
                Options.CacheProvider?.InvalidateTag(map.TableName);
                return 1;
            }

            // Server-generated tokens (ROWVERSION): the UPDATE carries an OUTPUT clause
            // returning the regenerated token (see BuildUpdate). Read it back so the same
            // instance's next UPDATE or DELETE compares against the current value; an empty
            // result means the WHERE (pk + token) matched nothing - a genuine conflict.
            if (operation == WriteOperation.Update && map.TimestampColumn != null
                && _p.SupportsNativeRowVersion && _p.GetUpdateTokenOutputClause(map).Length > 0)
            {
                var newToken = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
                if (newToken == null || newToken == DBNull.Value)
                    throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
                map.TimestampColumn.Setter(entity, newToken);
                RefreshTrackedOriginalToken(entity, map);
                Options.CacheProvider?.InvalidateTag(map.TableName);
                return 1;
            }

            var recordsAffected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            if ((operation is WriteOperation.Update or WriteOperation.Delete) &&
                map.TimestampColumn != null && recordsAffected == 0)
            {
                // S1: On affected-row semantics providers (e.g. MySQL default), 0 rows affected from
                // an UPDATE can mean either a stale OCC token OR a same-value update (no columns
                // actually changed). Disambiguate with a SELECT-then-verify, mirroring ExecuteUpdateBatch.
                // DELETE has no same-value ambiguity - always a genuine conflict.
                if (Provider.UseAffectedRowsSemantics && operation == WriteOperation.Update)
                    await VerifySingleUpdateOccAsync(cmd, map, entity, originalToken, ct).ConfigureAwait(false);
                else
                    throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            }

            // Client-managed tokens stamp the fresh value on the entity before executing; a
            // tracked entry's original-token snapshot must follow, or the next direct update
            // of the same tracked instance compares the pre-update token and false-conflicts.
            if (operation == WriteOperation.Update && map.TimestampColumn != null
                && map.ClientManagedConcurrencyToken && recordsAffected > 0)
            {
                RefreshTrackedOriginalToken(entity, map);
            }

            // Invalidate after a confirmed write (mirrors SaveChanges/ExecuteUpdate-Delete/Bulk).
            Options.CacheProvider?.InvalidateTag(map.TableName);
            return recordsAffected;
        }

        /// <summary>
        /// After a direct write commits durably, aligns the change tracker with the database
        /// for an entity instance the context happens to track: INSERT/UPDATE accept the
        /// entity's current values as the new baseline (a later SaveChanges must not re-write
        /// them over a concurrent writer's committed state), DELETE evicts the entry and
        /// clears navigation references so the tracked corpse cannot resurrect the row,
        /// block a same-key re-add, or false-conflict a later save. Mirrors the SaveChanges
        /// accept phase; direct writes inside externally-owned or ambient transactions skip
        /// this because the caller controls durability (same rule as SaveChanges).
        /// </summary>
        private void SyncTrackerAfterDirectWrite(object entity, TableMapping map, WriteOperation operation)
        {
            var entry = ChangeTracker.GetEntryOrDefault(entity);
            if (entry == null || !ReferenceEquals(entry.Entity, entity))
                return;
            if (operation == WriteOperation.Delete)
            {
                ChangeTracker.Remove(entity, true);
                RemoveDeletedInstancesFromTrackedNavigations(new List<object> { entity });
            }
            else
            {
                entry.AcceptChanges();
            }
        }

        /// <summary>
        /// Aligns a tracked entry's original-token snapshot with the entity's current token
        /// after a successful direct UPDATE, so subsequent direct writes of the same tracked
        /// instance compare against the token the row actually carries.
        /// </summary>
        private void RefreshTrackedOriginalToken(object entity, TableMapping map)
        {
            if (map.TimestampColumn == null)
                return;
            var entry = ChangeTracker.GetEntryOrDefault(entity);
            if (entry == null)
                return;
            var current = map.TimestampColumn.Getter(entity);
            entry.OriginalToken = current is byte[] bytes ? bytes.Clone() : current;
        }

        private void AddParametersOptimized<T>(DbCommand cmd, TableMapping map, T entity, WriteOperation operation, object? originalToken = null, Column[]? insertColumns = null) where T : class
        {
            switch (operation)
            {
                case WriteOperation.Insert:
                    // Stamp the TPH discriminator so a derived entity persists (and reads back) as its subtype.
                    map.ApplyDiscriminator(entity);
                    // insertColumns overrides the default set for the store-generated convention key's
                    // default-value insert (the key column is omitted and read back).
                    foreach (var col in insertColumns ?? _p.GetInsertColumns(map))
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    break;
                case WriteOperation.Update:
                    // Client-managed token: capture the OLD value for the WHERE predicate, then stamp a
                    // fresh one on the entity so the SET binding below writes the new value. BuildUpdate
                    // names the SET token @Token and the WHERE token @Token_orig so they don't collide.
                    object? whereTokenDirect = null;
                    if (map.TimestampColumn != null)
                    {
                        var tc = map.TimestampColumn;
                        whereTokenDirect = originalToken ?? tc.Getter(entity);
                        if (map.ClientManagedConcurrencyToken)
                            tc.Setter(entity, ConcurrencyTokenGenerator.Next(tc, whereTokenDirect));
                    }
                    foreach (var col in map.UpdateColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    if (map.ClientManagedConcurrencyToken)
                    {
                        var tc = map.TimestampColumn!;
                        var newTok = tc.Getter(entity);
                        cmd.AddOptimizedParam(_p.ParamPrefix + tc.PropName, newTok, GetParameterKnownType(tc, newTok));
                    }
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // WHERE compares the OLD token (captured above). When client-managed, bind it
                        // under the distinct @Token_orig name; otherwise the DB-generated rowversion
                        // path keeps the original @Token name.
                        var whereName = map.ClientManagedConcurrencyToken
                            ? map.TimestampColumn.PropName + "_orig"
                            : map.TimestampColumn.PropName;
                        cmd.AddOptimizedParam(_p.ParamPrefix + whereName, whereTokenDirect, GetParameterKnownType(map.TimestampColumn, whereTokenDirect));
                    }
                    // X1: bind tenant param to match the WHERE predicate added by BuildUpdate(includeTenant=true).
                    // Skip if TenantColumn is already in UpdateColumns - same @PropName is already bound
                    // for the SET clause, and SQLite/ADO.NET providers reuse named params by name, so
                    // the SET-bound value is used for the WHERE predicate too. Adding it twice throws.
                    if (Options.TenantProvider != null)
                    {
                        var tenantCol = RequireTenantColumn(map, "update");
                        if (!map.UpdateColumns.Any(c => c.PropName == tenantCol.PropName))
                            cmd.AddParam(_p.ParamPrefix + tenantCol.PropName, GetRequiredTenantId(map, "update"));
                    }
                    break;

                case WriteOperation.Delete:
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // Fallback: use current property value when originalToken is null (same
                        // rationale as the Update case above - entities attached without snapshot).
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddOptimizedParam(_p.ParamPrefix + map.TimestampColumn.PropName, tokenValue, GetParameterKnownType(map.TimestampColumn, tokenValue));
                    }
                    // X1: bind tenant param to match the WHERE predicate added by BuildDelete(includeTenant=true).
                    if (Options.TenantProvider != null)
                    {
                        var tenantCol = RequireTenantColumn(map, "delete");
                        cmd.AddParam(_p.ParamPrefix + tenantCol.PropName, GetRequiredTenantId(map, "delete"));
                    }
                    break;
            }
        }

        private static IEnumerable<TableMapping> TopologicalSortMappings(IEnumerable<TableMapping> mappings)
        {
            var all = mappings.ToList();
            // Zero or one mapping has no inter-type ordering constraint, so it IS its own topological order.
            // Short-circuit before building the dependency dictionaries + sort structures — this is the common
            // single-entity / single-type SaveChanges shape and the sort machinery is pure allocation there
            // (the method runs three times per save: added, modified, deleted mappings).
            if (all.Count <= 1)
                return all;
            var hardDeps = all.ToDictionary(
                m => m,
                // A self-referential type (Category→Parent, Employee→Manager) is never its own
                // *type-level* dependency: it sorts to a single position and any parent-before-child
                // ordering is resolved among rows within the group, not between types. The `other != m`
                // guard must therefore cover BOTH dependency tests — otherwise an explicit self
                // relationship registers the mapping as depending on itself and the cycle check
                // misreports a legitimate self-reference as a circular FK dependency.
                m => all.Where(other => other != m && (Array.Exists(m.Columns, c =>
                    // Match by full type name first (namespace-qualified) to avoid collisions
                    // between types with the same simple name in different namespaces.
                    MatchesPrincipalType(c.ForeignKeyPrincipalTypeName, other.Type)) ||
                    HasExplicitRelationshipDependency(m, other))).ToList());

            // Reference navigations (Order.Customer) impose the same principal-first write
            // order as FK-marker columns, but only the navigation property knows the
            // principal TYPE when the FK column's marker carries the navigation NAME
            // (e.g. a BackupDept navigation to a Dept principal). These are soft edges: a
            // model where two types reference each other is legal, so a cycle introduced
            // by soft edges falls back to the hard-edge order instead of failing the save.
            var softDeps = all.ToDictionary(
                m => m,
                m => hardDeps[m].Concat(all.Where(other => other != m
                        && !hardDeps[m].Contains(other)
                        && Array.Exists(m.ReferenceNavigations, nav => nav.PropertyType == other.Type)))
                    .ToList());

            if (TrySortTopologically(all, softDeps, out var softOrder, out _))
                return softOrder;
            if (TrySortTopologically(all, hardDeps, out var hardOrder, out var cyclePath))
                return hardOrder;

            const int maxCycleDisplay = 5;
            var displayNames = cyclePath!.Count <= maxCycleDisplay + 1
                ? cyclePath.Select(m => m.Type.Name)
                : cyclePath.Take(maxCycleDisplay).Select(m => m.Type.Name).Append("...");
            throw new NormConfigurationException(
                $"Circular FK dependency detected: {string.Join(" -> ", displayNames)}");
        }

        private static bool TrySortTopologically(
            List<TableMapping> all,
            Dictionary<TableMapping, List<TableMapping>> deps,
            out List<TableMapping> result,
            out List<TableMapping>? cyclePath)
        {
            var sorted = new List<TableMapping>();
            var visited = new HashSet<TableMapping>();
            var inProgress = new HashSet<TableMapping>();
            List<TableMapping>? cycle = null;

            bool Visit(TableMapping node, List<TableMapping> path)
            {
                if (inProgress.Contains(node))
                {
                    var cycleStart = path.IndexOf(node);
                    cycle = path.Skip(cycleStart).Append(node).ToList();
                    return false;
                }
                if (!visited.Add(node)) return true;
                inProgress.Add(node);
                path.Add(node);
                foreach (var dep in deps[node])
                    if (!Visit(dep, path))
                        return false;
                path.RemoveAt(path.Count - 1);
                inProgress.Remove(node);
                sorted.Add(node);
                return true;
            }

            foreach (var m in all)
            {
                if (!Visit(m, new List<TableMapping>()))
                {
                    result = sorted;
                    cyclePath = cycle;
                    return false;
                }
            }

            result = sorted;
            cyclePath = null;
            return true;
        }

        /// <summary>
        /// Orders the rows of a single self-referential mapping group so a parent row is written
        /// before the child row that references it (<paramref name="childrenFirst"/> = false, for
        /// inserts) or after it (<paramref name="childrenFirst"/> = true, for deletes). A
        /// self-referential table is one mapping, so <see cref="TopologicalSortMappings"/> cannot
        /// separate parents from children — the dependency lives between rows, not types, and an
        /// in-batch child insert that precedes its parent would violate the self-foreign-key.
        /// Returns the input list unchanged when the mapping has no self-reference or has fewer
        /// than two rows, so the common non-self-referential path pays no reordering cost.
        /// </summary>
        private static List<EntityEntry> OrderSelfReferentialRows(List<EntityEntry> entries, TableMapping map, bool childrenFirst)
        {
            if (entries.Count < 2)
                return entries;

            var selfRelation = map.Relations.Values.FirstOrDefault(r => r.DependentType == map.Type);
            if (selfRelation == null)
                return entries;

            var principalKeys = selfRelation.PrincipalKeys;
            var foreignKeys = selfRelation.ForeignKeys;

            // Index rows by their principal-key value so a child row can locate its parent row
            // within the same batch. Keys are formatted invariantly and length-delimited so that
            // single- and multi-column (composite) self-references compare consistently.
            static string FormatKey(IReadOnlyList<Column> cols, object entity)
            {
                // Length-prefixed, fully-printable segments (mirrors AppendSegment): a value can
                // never forge a segment boundary, and the -1 length is an unambiguous null marker
                // that no real value can produce.
                var sb = new StringBuilder();
                foreach (var c in cols)
                {
                    var v = c.Getter(entity);
                    if (v == null)
                    {
                        sb.Append("-1:|");
                    }
                    else
                    {
                        var s = Convert.ToString(v, CultureInfo.InvariantCulture) ?? string.Empty;
                        sb.Append(s.Length).Append(':').Append(s).Append('|');
                    }
                }
                return sb.ToString();
            }

            static bool AllKeyPartsNull(IReadOnlyList<Column> cols, object entity)
            {
                foreach (var c in cols)
                    if (c.Getter(entity) != null)
                        return false;
                return true;
            }

            var byPrincipal = new Dictionary<string, EntityEntry>(StringComparer.Ordinal);
            foreach (var e in entries)
            {
                if (e.Entity != null)
                    byPrincipal[FormatKey(principalKeys, e.Entity)] = e;
            }

            var ordered = new List<EntityEntry>(entries.Count);
            // 1 = on the current DFS path (cycle guard), 2 = emitted.
            var state = new Dictionary<EntityEntry, int>();

            void Visit(EntityEntry node)
            {
                if (state.TryGetValue(node, out _))
                    return; // already emitted, or on the current path (row-level cycle) — leave order to the DB.

                state[node] = 1;
                if (node.Entity != null && !AllKeyPartsNull(foreignKeys, node.Entity))
                {
                    var parentKey = FormatKey(foreignKeys, node.Entity);
                    if (byPrincipal.TryGetValue(parentKey, out var parent) && !ReferenceEquals(parent, node))
                        Visit(parent); // emit the parent first (post-order places it ahead of this node)
                }

                state[node] = 2;
                ordered.Add(node);
            }

            foreach (var e in entries)
                Visit(e);

            if (childrenFirst)
                ordered.Reverse();
            return ordered;
        }

        /// <summary>
        /// FK-2: Checks whether a FK principal type name matches the given type.
        /// Prefers exact full-name match (namespace-qualified) to avoid collisions
        /// between types with the same simple name in different namespaces.
        /// </summary>
        private static bool MatchesPrincipalType(string? principalTypeName, Type candidateType)
        {
            if (string.IsNullOrEmpty(principalTypeName)) return false;

            // Prefer full name match: e.g. "ModuleA.Customer" full name ends with ".Customer"
            var fullName = candidateType.FullName;
            if (fullName != null &&
                fullName.EndsWith("." + principalTypeName, StringComparison.OrdinalIgnoreCase))
                return true;

            // Fall back to simple name match for backward compatibility
            return string.Equals(principalTypeName, candidateType.Name, StringComparison.OrdinalIgnoreCase);
        }

        private static bool HasExplicitRelationshipDependency(TableMapping dependent, TableMapping candidatePrincipal)
            => candidatePrincipal.Relations.Values.Any(r => r.DependentType == dependent.Type);

        // Batched positional parameter names ("@p0".."@pN") are a small finite set that recurs on every
        // save; interpolating them per parameter (`$"{prefix}p{index}"`) allocated ~one string per column
        // per save. Pre-build and cache them per provider prefix so the hot binder just indexes an array.
        private const int PooledParamNameLimit = 256;
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, string[]> _pooledParamNames = new();

        /// <summary>
        /// Returns the batched positional parameter name <c>prefix + "p" + index</c>, byte-identical to the
        /// former <c>$"{prefix}p{index}"</c> interpolation (invariant digits, no grouping — matches the SQL
        /// placeholder emitted by <c>AppendUpdateBatch</c>/<c>AppendInsertBatch</c>). Names for indices below
        /// <see cref="PooledParamNameLimit"/> come from a per-prefix cache (zero allocation); larger indices
        /// (huge batches) fall back to concatenation.
        /// </summary>
        private static string ParamName(string prefix, int index)
        {
            if ((uint)index < PooledParamNameLimit)
            {
                var names = _pooledParamNames.GetOrAdd(prefix, static p =>
                {
                    var arr = new string[PooledParamNameLimit];
                    for (int i = 0; i < arr.Length; i++)
                        arr[i] = p + "p" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    return arr;
                });
                return names[index];
            }
            return prefix + "p" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        private int AddParametersBatched(DbCommand cmd, TableMapping map, object entity, WriteOperation operation, int startIndex, object? originalToken = null, IReadOnlyList<Column>? setColumns = null, Column[]? insertColumns = null)
        {
            var index = startIndex;
            switch (operation)
            {
                case WriteOperation.Insert:
                    // Stamp the TPH discriminator so a derived entity persists (and reads back) as its subtype.
                    map.ApplyDiscriminator(entity);
                    // insertColumns overrides the default set for the store-generated convention key's
                    // default-value run (the key column is omitted). Must equal the set AppendInsertBatch
                    // used, or the positional @pN parameters misalign.
                    foreach (var col in insertColumns ?? _p.GetInsertColumns(map))
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), val, GetParameterKnownType(col, val));
                    }
                    break;
                case WriteOperation.Update:
                    // Capture the OLD token for the WHERE predicate BEFORE stamping a new one, then
                    // (when client-managed) write a fresh token onto the entity so the SET loop below —
                    // which now includes the timestamp column — binds the new value. The concurrency
                    // check stays correct because WHERE compares the captured old token.
                    object? whereTokenBatched = null;
                    if (map.TimestampColumn != null)
                    {
                        var tc = map.TimestampColumn;
                        whereTokenBatched = originalToken ?? tc.Getter(entity);
                        if (map.ClientManagedConcurrencyToken)
                            tc.Setter(entity, ConcurrencyTokenGenerator.Next(tc, whereTokenBatched));
                    }
                    // SET values — only the changed columns for a partial update (setColumns), or every
                    // mutable column for a full/forced update. Must match BuildUpdateBatch's SET emission
                    // exactly (same subset, same order) or the positional @pN parameters misalign.
                    foreach (var col in (setColumns ?? (IReadOnlyList<Column>)map.UpdateColumns))
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), val, GetParameterKnownType(col, val));
                    }
                    if (map.ClientManagedConcurrencyToken)
                    {
                        // SET slot for the freshly-stamped token (set on the entity above).
                        var tc = map.TimestampColumn!;
                        var newTok = tc.Getter(entity);
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), newTok, GetParameterKnownType(tc, newTok));
                    }
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // WHERE compares the OLD token captured before the new one was stamped.
                        var tokenValue = whereTokenBatched;
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), tokenValue, GetParameterKnownType(map.TimestampColumn, tokenValue));
                    }
                    if (Options.TenantProvider != null)
                        cmd.AddParam(ParamName(_p.ParamPrefix, index++), GetRequiredTenantId(map, "update batch"));
                    break;
                case WriteOperation.Delete:
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // Use the original snapshot token when available.
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddOptimizedParam(ParamName(_p.ParamPrefix, index++), tokenValue, GetParameterKnownType(map.TimestampColumn, tokenValue));
                    }
                    if (Options.TenantProvider != null)
                        cmd.AddParam(ParamName(_p.ParamPrefix, index++), GetRequiredTenantId(map, "delete batch"));
                    break;
            }
            return index;
        }

        // Parameter binding, concurrency-token generation, and command scoping live in
        // DbContext.WriteSupport.cs so this partial stays focused on CRUD/batch orchestration.
        #endregion

        #region Bulk Operations

        // Bulk, stored-procedure, and temporal writes are non-idempotent and expose no commit barrier the
        // retry strategy can observe: their commit happens inside the provider (bulk) or is a single
        // auto-commit statement (proc/temporal). Passing this as isCommitAttempted makes the retry strategy
        // treat every failure as possibly-committed and NEVER retry them, so a transient fault at or after
        // commit cannot re-run the operation and duplicate rows. Idempotent reads and the tracked
        // single-entity / SaveChanges paths — which thread a real commit-attempted flag — are unaffected.
        private static readonly Func<bool> s_nonIdempotentNoRetry = static () => true;

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
                foreach (var entity in entityList)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Insert);
                    // Stamp the TPH discriminator so bulk-inserted derived entities also persist as
                    // their subtype (covers every provider's bulk path from this single choke point).
                    map.ApplyDiscriminator(entity);
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
                foreach (var entity in entityList)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Update);
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
                foreach (var entity in entityList)
                {
                    NormValidator.ValidateEntity(entity, nameof(entities));
                    ValidateTenantContext(entity, map, WriteOperation.Delete);
                }
                return await _p.BulkDeleteAsync(ctx, map, entityList, token).ConfigureAwait(false);
            }, s_nonIdempotentNoRetry, ct);
        }
        #endregion
    }
}
