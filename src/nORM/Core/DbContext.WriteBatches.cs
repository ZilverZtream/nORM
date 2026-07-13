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
    }
}
