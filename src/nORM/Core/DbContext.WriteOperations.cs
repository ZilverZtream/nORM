using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
            return await _executionStrategy.ExecuteAsync((ctx, token) =>
                WriteWithTransactionAsync(entity, map, WriteOperation.Insert, null, token, ownsTransaction: true), ct).ConfigureAwait(false);
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

            return await _executionStrategy.ExecuteAsync((ctx, token) =>
                WriteWithTransactionAsync(entity, map, operation, null, token, ownsTransaction: true), ct).ConfigureAwait(false);
        }

        private async Task<int> WriteWithTransactionAsync<T>(T entity, TableMapping map, WriteOperation operation, DbTransaction? transaction, CancellationToken ct, bool ownsTransaction) where T : class
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
                    await currentTransaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
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

            return recordsAffected;
        }

        private void AddParametersOptimized<T>(DbCommand cmd, TableMapping map, T entity, WriteOperation operation, object? originalToken = null) where T : class
        {
            switch (operation)
            {
                case WriteOperation.Insert:
                    foreach (var col in _p.GetInsertColumns(map))
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    break;
                case WriteOperation.Update:
                    foreach (var col in map.UpdateColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam(_p.ParamPrefix + col.PropName, val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // Use the original snapshot token (not the current possibly-mutated property value)
                        // to match the concurrency predicate parity of the batched SaveChanges path.
                        // Fallback to current property value when originalToken is null - this happens
                        // for entities that were attached without going through full snapshot tracking
                        // (e.g. manual Attach() or first-time tracked entities where no snapshot was
                        // captured yet). In that case the current property value is the best available
                        // token for the WHERE predicate.
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddOptimizedParam(_p.ParamPrefix + map.TimestampColumn.PropName, tokenValue, GetParameterKnownType(map.TimestampColumn, tokenValue));
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
            var deps = all.ToDictionary(
                m => m,
                m => all.Where(other => other != m && Array.Exists(m.Columns, c =>
                    // FK-2: Match by full type name first (namespace-qualified) to avoid collisions
                    // between types with the same simple name in different namespaces.
                    MatchesPrincipalType(c.ForeignKeyPrincipalTypeName, other.Type)) ||
                    HasExplicitRelationshipDependency(m, other)).ToList());

            var result = new List<TableMapping>();
            var visited = new HashSet<TableMapping>();
            var inProgress = new HashSet<TableMapping>();

            void Visit(TableMapping node, List<TableMapping> path)
            {
                if (inProgress.Contains(node))
                {
                    var cycleStart = path.IndexOf(node);
                    var cyclePath = path.Skip(cycleStart).Append(node).ToList();
                    const int maxCycleDisplay = 5;
                    var displayNames = cyclePath.Count <= maxCycleDisplay + 1
                        ? cyclePath.Select(m => m.Type.Name)
                        : cyclePath.Take(maxCycleDisplay).Select(m => m.Type.Name).Append("...");
                    throw new NormConfigurationException(
                        $"Circular FK dependency detected: {string.Join(" -> ", displayNames)}");
                }
                if (!visited.Add(node)) return;
                inProgress.Add(node);
                path.Add(node);
                foreach (var dep in deps[node]) Visit(dep, path);
                path.RemoveAt(path.Count - 1);
                inProgress.Remove(node);
                result.Add(node);
            }

            foreach (var m in all) Visit(m, new List<TableMapping>());
            return result;
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

        private string BuildInsertBatch(TableMapping map, int startParamIndex)
        {
            // INS-1: Only append identity retrieval when at least one key column is DB-generated.
            // For natural-key entities the fragment is wasteful and potentially wrong across providers.
            var hasDbGeneratedKey = HasDbGeneratedKey(map.KeyColumns);
            var identityPrefix = hasDbGeneratedKey
                ? _p.GetIdentityRetrievalPrefix(map)
                : string.Empty;
            var identityFragment = hasDbGeneratedKey
                ? _p.GetIdentityRetrievalString(map)
                : string.Empty;
            var cols = _p.GetInsertColumns(map);
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable}{identityPrefix} DEFAULT VALUES{identityFragment}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var paramNames = string.Join(", ", cols.Select((c, i) => $"{_p.ParamPrefix}p{startParamIndex + i}"));
            return $"INSERT INTO {map.EscTable} ({colNames}){identityPrefix} VALUES ({paramNames}){identityFragment}";
        }

        private string BuildUpdateBatch(TableMapping map, int startParamIndex)
        {
            if (map.KeyColumns.Length == 0)
                throw new NormConfigurationException(string.Format(
                    ErrorMessages.InvalidConfiguration,
                    $"Entity '{map.Type.Name}' has no primary key; UPDATE requires a key."));

            // Guard against empty SET clause when entity has no mutable columns.
            // This happens when all columns are either keys or concurrency tokens.
            // Emitting "UPDATE T SET WHERE ..." is invalid SQL; throw a clear, actionable error.
            if (map.UpdateColumns.Length == 0)
                throw new NormConfigurationException(
                    $"Entity '{map.Type.Name}' has no mutable columns to update " +
                    "(all non-key columns are concurrency tokens or the entity only has key columns). " +
                    "Use [NotMapped] for computed properties or add at least one mutable property " +
                    "that is not a key or concurrency token.");

            var setSb = new StringBuilder();
            var idx = startParamIndex;
            for (int i = 0; i < map.UpdateColumns.Length; i++)
            {
                if (i > 0) setSb.Append(", ");
                setSb.Append(map.UpdateColumns[i].EscCol)
                    .Append('=')
                    .Append(_p.ParamPrefix).Append('p').Append(idx++);
            }
            var whereParts = new List<string>();
            foreach (var col in map.KeyColumns)
                whereParts.Add($"{col.EscCol}={_p.ParamPrefix}p{idx++}");
            if (map.TimestampColumn != null)
            {
                var tc = map.TimestampColumn;
                // Null-safe equality: handles the case where the concurrency token is a nullable column.
                // Optimization opportunity: when the column is known non-nullable at mapping time,
                // the OR branch is unreachable and could be elided to produce simpler SQL. Currently
                // we always emit the full null-safe form for correctness across all column definitions.
                whereParts.Add($"({tc.EscCol}={_p.ParamPrefix}p{idx} OR ({tc.EscCol} IS NULL AND {_p.ParamPrefix}p{idx} IS NULL))");
                idx++;
            }
            if (Options.TenantProvider != null)
            {
                var tenantCol = RequireTenantColumn(map, "update batch");
                whereParts.Add($"{tenantCol.EscCol}={_p.ParamPrefix}p{idx++}");
            }
            var where = string.Join(" AND ", whereParts);
            return $"UPDATE {map.EscTable} SET {setSb} WHERE {where}";
        }

        private string BuildDeleteBatch(TableMapping map, int startParamIndex)
        {
            if (map.KeyColumns.Length == 0)
                throw new NormConfigurationException(string.Format(
                    ErrorMessages.InvalidConfiguration,
                    $"Entity '{map.Type.Name}' has no primary key; DELETE requires a key."));
            var idx = startParamIndex;
            var whereParts = new List<string>();
            foreach (var col in map.KeyColumns)
                whereParts.Add($"{col.EscCol}={_p.ParamPrefix}p{idx++}");
            if (map.TimestampColumn != null)
            {
                var tc = map.TimestampColumn;
                whereParts.Add($"({tc.EscCol}={_p.ParamPrefix}p{idx} OR ({tc.EscCol} IS NULL AND {_p.ParamPrefix}p{idx} IS NULL))");
                idx++;
            }
            if (Options.TenantProvider != null)
            {
                var tenantCol = RequireTenantColumn(map, "delete batch");
                whereParts.Add($"{tenantCol.EscCol}={_p.ParamPrefix}p{idx++}");
            }
            var where = string.Join(" AND ", whereParts);
            return $"DELETE FROM {map.EscTable} WHERE {where}";
        }

        private int AddParametersBatched(DbCommand cmd, TableMapping map, object entity, WriteOperation operation, int startIndex, object? originalToken = null)
        {
            var index = startIndex;
            switch (operation)
            {
                case WriteOperation.Insert:
                    foreach (var col in _p.GetInsertColumns(map))
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam($"{_p.ParamPrefix}p{index++}", val, GetParameterKnownType(col, val));
                    }
                    break;
                case WriteOperation.Update:
                    foreach (var col in map.UpdateColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam($"{_p.ParamPrefix}p{index++}", val, GetParameterKnownType(col, val));
                    }
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam($"{_p.ParamPrefix}p{index++}", val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // Use the original snapshot token when available rather than the current
                        // (possibly mutated) property value, to ensure the correct concurrency check.
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddOptimizedParam($"{_p.ParamPrefix}p{index++}", tokenValue, GetParameterKnownType(map.TimestampColumn, tokenValue));
                    }
                    if (Options.TenantProvider != null)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", GetRequiredTenantId(map, "update batch"));
                    break;
                case WriteOperation.Delete:
                    foreach (var col in map.KeyColumns)
                    {
                        var rawVal = col.Getter(entity);
                        var val = col.Converter != null ? col.Converter.ConvertToProvider(rawVal) : rawVal;
                        cmd.AddOptimizedParam($"{_p.ParamPrefix}p{index++}", val, GetParameterKnownType(col, val));
                    }
                    if (map.TimestampColumn != null)
                    {
                        // Use the original snapshot token when available.
                        var tokenValue = originalToken ?? map.TimestampColumn.Getter(entity);
                        cmd.AddOptimizedParam($"{_p.ParamPrefix}p{index++}", tokenValue, GetParameterKnownType(map.TimestampColumn, tokenValue));
                    }
                    if (Options.TenantProvider != null)
                        cmd.AddParam($"{_p.ParamPrefix}p{index++}", GetRequiredTenantId(map, "delete batch"));
                    break;
            }
            return index;
        }

        internal static Type? GetParameterKnownType(Column col, object? providerValue)
            => providerValue?.GetType() ?? (col.Converter == null ? col.Prop.PropertyType : null);

        // TODO: Consider replacing the tuple array with parallel name[] and value[] arrays
        // to reduce per-element overhead. ValueTuple<string, object> boxes the object on every
        // iteration. Two flat arrays (string[] names, object[] values) would avoid the tuple
        // allocation and improve cache locality for the SetParametersFast hot path.
        private IReadOnlyDictionary<string, object> AddParametersFast(DbCommand cmd, object[] parameters)
        {
            var span = new (string name, object value)[parameters.Length];
            var hasProviderParameters = false;
            for (int i = 0; i < parameters.Length; i++)
            {
                var name = $"{_p.ParamPrefix}p{i}";
                var value = parameters[i] ?? DBNull.Value;
                span[i] = (name, value);
                hasProviderParameters |= value is DbParameter;
            }

            if (hasProviderParameters)
            {
                cmd.Parameters.Clear();
                var providerParameterDict = new Dictionary<string, object>(parameters.Length);

                foreach (var (name, value) in span)
                {
                    if (value is DbParameter providerParameter)
                    {
                        providerParameter.ParameterName = name;
                        cmd.Parameters.Add(providerParameter);
                        providerParameterDict[name] = providerParameter.Value ?? DBNull.Value;
                    }
                    else
                    {
                        var parameter = cmd.CreateParameter();
                        parameter.ParameterName = name;
                        nORM.Query.ParameterAssign.AssignValue(parameter, value);
                        cmd.Parameters.Add(parameter);
                        providerParameterDict[name] = value;
                    }
                }

                return providerParameterDict;
            }

            cmd.SetParametersFast(span);

            // Gate B fix: Always populate the parameter dictionary so that ValidateRawSql
            // receives accurate parameter metadata regardless of logging state.
            // The dictionary is required for validation (not just logging) - decoupling the
            // two concerns ensures parameterized queries are never incorrectly flagged by
            // the validator when debug logging is disabled.
            if (parameters.Length == 0)
                return EmptyDictionary<string, object>.Instance;

            var dict = new Dictionary<string, object>(parameters.Length);
            foreach (var (name, value) in span) dict[name] = value;
            return dict;
        }

        internal static class EmptyDictionary<TKey, TValue> where TKey : notnull
        {
            public static readonly IReadOnlyDictionary<TKey, TValue> Instance = new Dictionary<TKey, TValue>(0);
        }

        private readonly struct CommandScope : IAsyncDisposable
        {
            private readonly DbConnection _connection;
            private readonly DbTransaction? _transaction;
            public CommandScope(DbConnection connection, DbTransaction? transaction)
            {
                _connection = connection;
                _transaction = transaction;
            }

            /// <summary>
            /// Creates a <see cref="DbCommand"/> tied to the scoped connection and transaction.
            /// </summary>
            /// <returns>A configured command ready for parameter population and execution.</returns>
            public DbCommand CreateCommand()
            {
                var cmd = _connection.CreateCommand();
                if (_transaction != null)
                    cmd.Transaction = _transaction;
                return cmd;
            }
            /// <summary>
            /// Disposes the command scope. For pooled connections no additional
            /// cleanup is required so a completed <see cref="ValueTask"/> is returned.
            /// </summary>
            /// <returns>A completed task representing the asynchronous dispose operation.</returns>
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
        #endregion

        #region Bulk Operations
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
                }
                return await _p.BulkInsertAsync(ctx, map, entityList, token).ConfigureAwait(false);
            }, ct);
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
            }, ct);
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
            }, ct);
        }
        #endregion
    }
}
