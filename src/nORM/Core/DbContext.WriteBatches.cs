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
        /// for nullable types) - meaning no INSERT has been committed for this entity yet.
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
                AppendInsertBatch(sql, map, paramIndex);
                sql.Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entry.Entity ?? throw new InvalidOperationException("Entity is null"),
                    WriteOperation.Insert, paramIndex);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Insert, cmd.CommandText));
            // Skip PrepareAsync when identity retrieval is needed: the INSERT SQL contains
            // multiple result-set-returning statements (e.g. "; SELECT LAST_INSERT_ID();")
            // and some providers (MySQL) do not support preparing multi-statement commands.
            if (batch.Count > 1 && !HasDbGeneratedKey(map.KeyColumns) && _p.SupportsPreparedBatchCommands)
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
                // Throwing here triggers the SaveChanges catch block -> rollback.
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

            // Server-generated tokens (ROWVERSION) with application-supplied keys: each
            // INSERT carries an OUTPUT clause returning the generated token (one result
            // set per statement). Hydrate it so the same context's first UPDATE or DELETE
            // of this row compares against the current value instead of throwing a false
            // stale-token conflict.
            if (map.TimestampColumn != null && _p.SupportsNativeRowVersion
                && _p.GetInsertTokenOutputClause(map).Length > 0)
            {
                var inserted = 0;
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, ct).ConfigureAwait(false);
                var entryIndex = 0;
                do
                {
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var tokenValue = reader.GetValue(0);
                        var insertedEntity = batch[entryIndex].Entity;
                        if (insertedEntity != null && tokenValue != DBNull.Value)
                            map.TimestampColumn.Setter(insertedEntity, tokenValue);
                        inserted++;
                    }
                    entryIndex++;
                }
                while (await reader.NextResultAsync(ct).ConfigureAwait(false) && entryIndex < batch.Count);
                // AcceptChanges is intentionally deferred until after commit.
                return inserted;
            }

            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // AcceptChanges is intentionally deferred until after commit.
            return affected;
        }

        /// <summary>
        /// The SET column subset for one entry's partial UPDATE: the columns flagged changed for the entry
        /// (<see cref="EntityEntry.GetChangedColumns"/>) intersected with the mutable allowlist
        /// <paramref name="updateColSet"/> (so a changed key / token / db-generated column can never leak into
        /// the SET). Falls back to the full <see cref="TableMapping.UpdateColumns"/> when nothing eligible is
        /// flagged — a forced Modified entity (ctx.Update / State = Modified) flags ALL columns, and a
        /// navigation/owned-only change flags none — so a Modified row is never silently skipped nor given an
        /// invalid empty SET clause.
        /// </summary>
        private static IReadOnlyList<Column> ComputeUpdateSetColumns(EntityEntry entry, TableMapping map, HashSet<Column> updateColSet)
        {
            var changed = entry.GetChangedColumns();
            if (changed.Length == 0)
                return map.UpdateColumns;
            var filtered = new List<Column>(changed.Length);
            foreach (var c in changed)
                if (updateColSet.Contains(c)) filtered.Add(c);
            return filtered.Count > 0 && filtered.Count < map.UpdateColumns.Length ? filtered : map.UpdateColumns;
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
        /// <param name="deferAccept">True when the save runs under a caller-owned/enlisted transaction, so
        /// AcceptChanges is deferred and each entry's original-token snapshot must be advanced here for a later
        /// write of the same entity in the transaction to match. False on the retryable owned-transaction path,
        /// where post-commit AcceptChanges does this instead and advancing early would break a retry.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        private async Task<int> ExecuteUpdateBatch(DbCommand cmd, TableMapping map, List<EntityEntry> batch, StringBuilder sql, int paramIndex, CancellationToken ct, bool deferAccept = false)
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

            // Allowlist of columns eligible for the SET clause (mutable: non-key, non-timestamp,
            // non-db-generated). Built once per batch since the mapping is constant.
            var updateColSet = new HashSet<Column>(map.UpdateColumns);

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

                // A tenant column is identity-defining: changing it on a tracked entity would move the
                // row into a different tenant, silently removing it from the current tenant's data (the
                // UPDATE matches the current tenant in its WHERE clause but the SET rewrites the tenant
                // value). Like a primary-key mutation, reject it loudly rather than perform a silent
                // cross-tenant move. The direct and bulk write paths already enforce this via
                // ValidateTenantContext; the batched SaveChanges path did not.
                if (Options.TenantProvider != null && map.TenantColumn != null
                    && entry.HasColumnValueChanged(map.TenantColumn))
                {
                    throw new InvalidOperationException(
                        $"Tenant column mutation detected on entity '{map.Type.Name}'. " +
                        "The tenant of a tracked entity cannot be changed by an update, because it would " +
                        "move the row out of the current tenant. Detach the entity to migrate it deliberately.");
                }

                // Partial-column UPDATE: the SAME setCols array feeds both the SQL builder and the parameter
                // binder, keeping the positional @pN parameters aligned. See ComputeUpdateSetColumns.
                var setCols = ComputeUpdateSetColumns(entry, map, updateColSet);

                AppendUpdateBatch(sql, map, setCols, paramIndex);
                sql.Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entity,
                    WriteOperation.Update, paramIndex, entry.OriginalToken, setCols);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Update, cmd.CommandText));
            if (batch.Count > 1 && _p.SupportsPreparedBatchCommands)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            int updated;
            if (map.TimestampColumn != null && _p.SupportsNativeRowVersion)
            {
                // Each UPDATE carries an OUTPUT clause returning the regenerated
                // token (one result set per statement, empty when the row did not
                // match). Hydrate the fresh token into the entity so the same
                // context's next save of this row compares against the current
                // value instead of throwing a false stale-token conflict.
                updated = 0;
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(this, CommandBehavior.Default, ct).ConfigureAwait(false);
                var entryIndex = 0;
                do
                {
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var tokenValue = reader.GetValue(0);
                        var updatedEntity = batch[entryIndex].Entity;
                        if (updatedEntity != null && tokenValue != DBNull.Value)
                            map.TimestampColumn.Setter(updatedEntity, tokenValue);
                        updated++;
                    }
                    entryIndex++;
                }
                while (await reader.NextResultAsync(ct).ConfigureAwait(false) && entryIndex < batch.Count);
            }
            else
            {
                updated = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            }
            // S1 - Optimistic-concurrency rowcount check.
            // For matched-row providers (UseAffectedRowsSemantics=false): 0 rows updated means
            // the WHERE clause (pk + token) did not match, so the token was stale - throw.
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
            // The UPDATE stamped a fresh token (native OUTPUT hydrated above, client-managed stamped in the
            // parameter binder). Under a caller-owned/enlisted transaction AcceptChanges is deferred, so align
            // each tracked entry's original-token snapshot with the value the row now carries — otherwise a
            // second UPDATE or a DELETE of the same tracked entity inside one transaction compares the
            // pre-update token and false-conflicts. Only when accept is deferred: an owned-transaction save
            // accepts post-commit (which does this), and it is the retryable path — advancing the snapshot on
            // an attempt that later rolls back would make the retry compare against the reverted row. A
            // caller-owned transaction never retries (SaveChangesWithRetryAsync runs it exactly once) and its
            // rollback restores the snapshot (DbContext.Transactions). Mirrors RefreshTrackedOriginalToken.
            if (deferAccept && map.TimestampColumn != null)
            {
                var tc = map.TimestampColumn;
                foreach (var entry in batch)
                    if (entry.Entity is { } e)
                    {
                        var current = tc.Getter(e);
                        entry.OriginalToken = current is byte[] bytes ? bytes.Clone() : current;
                    }
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

                AppendDeleteBatch(sql, map, paramIndex);
                sql.Append(';');
                paramIndex = AddParametersBatched(cmd, map,
                    entity,
                    WriteOperation.Delete, paramIndex, entry.OriginalToken);
            }
            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = ToSecondsClamped(GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.Delete, cmd.CommandText));
            if (batch.Count > 1 && _p.SupportsPreparedBatchCommands)
                await cmd.PrepareAsync(ct).ConfigureAwait(false);

            var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
            // S1 - DELETE rowcount check. Unlike UPDATE, DELETE has no same-value ambiguity:
            // a row is "deleted" only if it actually existed and was removed. Even on affected-row
            // providers (UseAffectedRowsSemantics=true), 0 deleted rows always means either the
            // token was stale or the row was already gone - both are genuine conflicts. No
            // SELECT-then-verify is needed; we always throw when deleted != batch.Count.
            if (map.TimestampColumn != null && deleted != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
            // Entity removal from ChangeTracker is deferred until after the transaction commits.
            return deleted;
        }

        /// <summary>
        /// S1 - SELECT-then-verify fallback for affected-row semantics providers (e.g. MySQL).
        /// Called from <see cref="ExecuteUpdateBatch"/> when <c>UseAffectedRowsSemantics=true</c>
        /// and the UPDATE rowcount does not match the batch size. Queries the database for rows
        /// that still carry their original concurrency tokens:
        /// <list type="bullet">
        ///   <item>If all tokens still match (<c>count == batch.Count</c>): the UPDATE returned 0
        ///     because no column values actually changed (same-value update). No conflict - return.</item>
        ///   <item>If any token is missing (<c>count &lt; batch.Count</c>): a competing writer
        ///     changed the token. Genuine stale-row conflict - throw <see cref="DbConcurrencyException"/>.</item>
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
                    // Bind the converter's provider value like the write itself does — a raw
                    // model key matches nothing and misreports every row as a stale conflict.
                    var rawKey = kc.Getter(entity);
                    cmd.AddParam($"{_p.ParamPrefix}v{pi++}", kc.Converter != null ? kc.Converter.ConvertToProvider(rawKey) : rawKey);
                }
                // Null-safe token equality matches the predicate used in BuildUpdateBatch/BuildDeleteBatch.
                pkConds.Add($"({tc.EscCol}={_p.ParamPrefix}v{pi} OR ({tc.EscCol} IS NULL AND {_p.ParamPrefix}v{pi} IS NULL))");
                var tok = entry.OriginalToken;
                if (tok != null && tc.Converter != null) tok = tc.Converter.ConvertToProvider(tok);
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

            // Guard against null scalar result - Convert.ToInt32(null) throws ArgumentNullException
            // X2: route through interceptor pipeline so observability tools see verification queries.
            var scalarResult = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
            var matchCount = scalarResult == null || scalarResult is DBNull ? 0 : Convert.ToInt32(scalarResult);
            if (matchCount != batch.Count)
                throw new DbConcurrencyException("A concurrency conflict occurred. The row may have been modified or deleted by another user.");
        }

        /// <summary>
        /// S1 - SELECT-then-verify for the single-entity direct UpdateAsync path on affected-row
        /// semantics providers (e.g. MySQL). Called when <c>recordsAffected == 0</c> and
        /// <c>UseAffectedRowsSemantics=true</c> to distinguish a same-value update (token still
        /// present -> no conflict) from a genuine stale-row conflict (token gone -> throw).
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
                // Bind the converter's provider value like the write itself does — a raw
                // model key matches nothing and misreports the update as a stale conflict.
                var rawKey = kc.Getter(entity);
                cmd.AddParam($"{_p.ParamPrefix}v{pi++}", kc.Converter != null ? kc.Converter.ConvertToProvider(rawKey) : rawKey);
            }
            // Null-safe token equality mirrors the predicate in BuildUpdate/BuildUpdateBatch.
            conditions.Add($"({tc.EscCol}={_p.ParamPrefix}v{pi} OR ({tc.EscCol} IS NULL AND {_p.ParamPrefix}v{pi} IS NULL))");
            var tok = originalToken ?? tc.Getter(entity);
            if (tok != null && tc.Converter != null) tok = tc.Converter.ConvertToProvider(tok);
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

        // String form of AppendInsertBatch, kept for insert template-length estimation (SaveChanges
        // batch sizing). Both share the same builder logic, so they can never diverge.
        private string BuildInsertBatch(TableMapping map, int startParamIndex)
        {
            var sb = new StringBuilder();
            AppendInsertBatch(sb, map, startParamIndex);
            return sb.ToString();
        }

        /// <summary>
        /// Appends one entity's batched INSERT directly into <paramref name="sql"/>. Output is
        /// byte-identical to the former interpolation; this form skips the two <c>Select</c>+<c>string.Join</c>
        /// passes (column and parameter name lists) and the return-string allocation, per entity per save.
        /// The parameter numbering (<c>startParamIndex + i</c> over the provider's insert columns)
        /// matches <see cref="AddParametersBatched"/>.
        /// </summary>
        private void AppendInsertBatch(StringBuilder sql, TableMapping map, int startParamIndex)
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
            // Server-generated tokens (ROWVERSION) are assigned on INSERT too; when no
            // identity retrieval reads them back, the token output clause does (the
            // identity clause already includes the token for DB-generated keys).
            var tokenPrefix = !hasDbGeneratedKey && map.TimestampColumn != null && _p.SupportsNativeRowVersion
                ? _p.GetInsertTokenOutputClause(map)
                : string.Empty;
            var cols = _p.GetInsertColumns(map);
            if (cols.Length == 0)
            {
                sql.Append("INSERT INTO ").Append(map.EscTable).Append(identityPrefix).Append(tokenPrefix)
                   .Append(' ').Append(_p.DefaultValuesInsertClause).Append(identityFragment);
                return;
            }
            sql.Append("INSERT INTO ").Append(map.EscTable).Append(" (");
            for (int i = 0; i < cols.Length; i++)
            {
                if (i > 0) sql.Append(", ");
                sql.Append(cols[i].EscCol);
            }
            sql.Append(')').Append(identityPrefix).Append(tokenPrefix).Append(" VALUES (");
            for (int i = 0; i < cols.Length; i++)
            {
                if (i > 0) sql.Append(", ");
                sql.Append(_p.ParamPrefix).Append('p').Append(startParamIndex + i);
            }
            sql.Append(')').Append(identityFragment);
        }

        // Full-column overload (used for template-length estimation): updates every mutable column.
        private string BuildUpdateBatch(TableMapping map, int startParamIndex)
            => BuildUpdateBatch(map, map.UpdateColumns, startParamIndex);

        /// <summary>
        /// String form of <see cref="AppendUpdateBatch"/>, kept for template-length estimation (batch
        /// sizing). The hot save path calls <see cref="AppendUpdateBatch"/> directly to write into the
        /// shared batch builder without the intermediate string/List/Join/return-string allocations.
        /// Both share the same builder, so they can never diverge.
        /// </summary>
        private string BuildUpdateBatch(TableMapping map, IReadOnlyList<Column> setColumns, int startParamIndex)
        {
            var sb = new StringBuilder();
            AppendUpdateBatch(sb, map, setColumns, startParamIndex);
            return sb.ToString();
        }

        /// <summary>
        /// Appends one entity's batched UPDATE directly into <paramref name="sql"/>, writing only
        /// <paramref name="setColumns"/> in the SET clause (a subset of <see cref="TableMapping.UpdateColumns"/>
        /// — the changed columns for a partial update, or all of them for a forced/full update). The
        /// concurrency-token SET slot, key/token/tenant WHERE predicates, and positional parameter order are
        /// identical to the full-column form, so <see cref="AddParametersBatched"/> MUST bind the SAME
        /// <paramref name="setColumns"/> in the same order. Output is byte-identical to the former
        /// <c>$"UPDATE {EscTable} SET {setSb}{tokenOutput} WHERE {where}"</c> interpolation; this form just
        /// skips the per-entity <c>List</c>/<c>string.Join</c>/interpolation/return-string allocations.
        /// </summary>
        private void AppendUpdateBatch(StringBuilder sql, TableMapping map, IReadOnlyList<Column> setColumns, int startParamIndex)
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

            sql.Append("UPDATE ").Append(map.EscTable).Append(" SET ");
            var idx = startParamIndex;
            var wroteSet = false;
            for (int i = 0; i < setColumns.Count; i++)
            {
                if (i > 0) sql.Append(", ");
                sql.Append(setColumns[i].EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx++);
                wroteSet = true;
            }
            // Client-managed concurrency token: write a fresh value in the SET clause so a stale
            // concurrent UPDATE (whose WHERE still carries the old token) affects zero rows. The
            // parameter binder generates the new value and binds this slot; the old token is compared
            // separately in the WHERE below. The leading ", " mirrors the former "setSb.Length > 0" check.
            if (map.ClientManagedConcurrencyToken)
            {
                if (wroteSet) sql.Append(", ");
                sql.Append(map.TimestampColumn!.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx++);
            }
            // Server-generated tokens (ROWVERSION) regenerate on every UPDATE; the provider's OUTPUT
            // clause reads the fresh value back so the tracked instance can save again. Emitted between
            // the SET list and " WHERE ", exactly as the former "{setSb}{tokenOutput} WHERE" interpolation.
            if (map.TimestampColumn != null && _p.SupportsNativeRowVersion)
                sql.Append(_p.GetUpdateTokenOutputClause(map));
            sql.Append(" WHERE ");
            var wroteWhere = false;
            foreach (var col in map.KeyColumns)
            {
                if (wroteWhere) sql.Append(" AND ");
                sql.Append(col.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx++);
                wroteWhere = true;
            }
            if (map.TimestampColumn != null)
            {
                if (wroteWhere) sql.Append(" AND ");
                var tc = map.TimestampColumn;
                // Null-safe equality: handles the case where the concurrency token is a nullable column.
                sql.Append('(').Append(tc.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx)
                   .Append(" OR (").Append(tc.EscCol).Append(" IS NULL AND ").Append(_p.ParamPrefix).Append('p').Append(idx).Append(" IS NULL))");
                idx++;
                wroteWhere = true;
            }
            if (Options.TenantProvider != null)
            {
                if (wroteWhere) sql.Append(" AND ");
                var tenantCol = RequireTenantColumn(map, "update batch");
                sql.Append(tenantCol.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx++);
                wroteWhere = true;
            }
        }

        // String form of AppendDeleteBatch (currently unused by the hot path, kept for symmetry/
        // template use). Both share the same builder logic, so they can never diverge.
        private string BuildDeleteBatch(TableMapping map, int startParamIndex)
        {
            var sb = new StringBuilder();
            AppendDeleteBatch(sb, map, startParamIndex);
            return sb.ToString();
        }

        /// <summary>
        /// Appends one entity's batched DELETE directly into <paramref name="sql"/>. Output is
        /// byte-identical to the former <c>$"DELETE FROM {EscTable} WHERE {where}"</c> interpolation;
        /// this form skips the per-entity <c>List</c>/<c>string.Join</c>/interpolation/return-string
        /// allocations. Key/token/tenant predicate order matches <see cref="AddParametersBatched"/>.
        /// </summary>
        private void AppendDeleteBatch(StringBuilder sql, TableMapping map, int startParamIndex)
        {
            if (map.KeyColumns.Length == 0)
                throw new NormConfigurationException(string.Format(
                    ErrorMessages.InvalidConfiguration,
                    $"Entity '{map.Type.Name}' has no primary key; DELETE requires a key."));
            sql.Append("DELETE FROM ").Append(map.EscTable).Append(" WHERE ");
            var idx = startParamIndex;
            var wroteWhere = false;
            foreach (var col in map.KeyColumns)
            {
                if (wroteWhere) sql.Append(" AND ");
                sql.Append(col.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx++);
                wroteWhere = true;
            }
            if (map.TimestampColumn != null)
            {
                if (wroteWhere) sql.Append(" AND ");
                var tc = map.TimestampColumn;
                sql.Append('(').Append(tc.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx)
                   .Append(" OR (").Append(tc.EscCol).Append(" IS NULL AND ").Append(_p.ParamPrefix).Append('p').Append(idx).Append(" IS NULL))");
                idx++;
                wroteWhere = true;
            }
            if (Options.TenantProvider != null)
            {
                if (wroteWhere) sql.Append(" AND ");
                var tenantCol = RequireTenantColumn(map, "delete batch");
                sql.Append(tenantCol.EscCol).Append('=').Append(_p.ParamPrefix).Append('p').Append(idx++);
                wroteWhere = true;
            }
        }
    }
}
