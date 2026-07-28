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
        // String form of AppendInsertBatch, kept for insert template-length estimation (SaveChanges
        // batch sizing). Both share the same builder logic, so they can never diverge. Estimation uses the
        // full insert-column set and the DB-generated-key read-back flag (the convention default-key run
        // omits one column and reads back, so this is an upper bound — safe for buffer sizing).
        private string BuildInsertBatch(TableMapping map, int startParamIndex)
        {
            var sb = new StringBuilder();
            AppendInsertBatch(sb, map, startParamIndex, _p.GetInsertColumns(map), HasDbGeneratedKey(map.KeyColumns));
            return sb.ToString();
        }

        /// <summary>
        /// Appends one entity's batched INSERT directly into <paramref name="sql"/>, writing exactly
        /// <paramref name="cols"/> in the column and VALUES lists. When <paramref name="readBackKey"/> is
        /// true the provider's identity-retrieval clause is appended so the generated key is read back —
        /// used both for DB-generated keys and for the store-generated convention key's default-value run
        /// (whose <paramref name="cols"/> omits the key). The parameter numbering
        /// (<c>startParamIndex + i</c> over <paramref name="cols"/>) matches <see cref="AddParametersBatched"/>,
        /// which MUST be given the SAME column set.
        /// </summary>
        private void AppendInsertBatch(StringBuilder sql, TableMapping map, int startParamIndex, Column[] cols, bool readBackKey)
        {
            // INS-1: Only append identity retrieval when the key is read back (DB-generated, or the
            // convention key's store-generated default-value run). For natural-key entities the fragment is
            // wasteful and potentially wrong across providers.
            var identityPrefix = readBackKey
                ? _p.GetIdentityRetrievalPrefix(map)
                : string.Empty;
            var identityFragment = readBackKey
                ? _p.GetIdentityRetrievalString(map)
                : string.Empty;
            // Server-generated tokens (ROWVERSION) are assigned on INSERT too; when no
            // identity retrieval reads them back, the token output clause does (the
            // identity clause already includes the token for DB-generated keys).
            var tokenPrefix = !readBackKey && map.TimestampColumn != null && _p.SupportsNativeRowVersion
                ? _p.GetInsertTokenOutputClause(map)
                : string.Empty;
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
