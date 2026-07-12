using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Configuration;

#nullable enable

namespace nORM.Providers
{
    public abstract partial class DatabaseProvider
    {
        #region SQL Generation
        /// <summary>
        /// Returns the columns that should appear in an INSERT statement for the given mapping.
        /// Override in provider subclasses to exclude server-managed columns (e.g. SQL Server ROWVERSION)
        /// that cannot receive explicit values on INSERT.
        /// </summary>
        public virtual Column[] GetInsertColumns(TableMapping m) => m.InsertColumns;

        /// <summary>
        /// Builds a parameterized <c>INSERT</c> statement for the specified table
        /// mapping, caching the generated SQL for future use.
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <param name="hydrateGeneratedKeys">
        /// When <c>true</c>, append provider-specific identity retrieval so callers can hydrate generated keys.
        /// </param>
        /// <returns>An <c>INSERT</c> statement ready for parameter substitution.</returns>
        public string BuildInsert(TableMapping m, bool hydrateGeneratedKeys = true)
        {
            var includeIdentityRetrieval = hydrateGeneratedKeys && m.KeyColumns.Any(k => k.IsDbGenerated);
            var cacheKey = includeIdentityRetrieval ? "INSERT" : "INSERT_PLAIN";
            return _sqlCache.GetOrAdd((m.Type, m.TableName, cacheKey, m.SqlShapeKey), _ => {
                var cols = GetInsertColumns(m);
                var identityPrefix = includeIdentityRetrieval ? GetIdentityRetrievalPrefix(m) : string.Empty;
                var identitySuffix = includeIdentityRetrieval ? GetIdentityRetrievalString(m) : string.Empty;
                if (cols.Length == 0)
                {
                    return $"INSERT INTO {m.EscTable}{identityPrefix} {DefaultValuesInsertClause}{identitySuffix}";
                }
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                var valParams = string.Join(", ", cols.Select(c => ParamPrefix + c.PropName));
                return $"INSERT INTO {m.EscTable} ({colNames}){identityPrefix} VALUES ({valParams}){identitySuffix}";
            });
        }

        /// <summary>
        /// Builds a parameterized <c>UPDATE</c> statement that updates all non-key
        /// columns and filters by the entity's primary key (and timestamp when present).
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <param name="includeTenant">When <c>true</c>, appends an AND predicate for the tenant
        /// column so the statement can only modify the current tenant's rows.</param>
        /// <returns>An <c>UPDATE</c> SQL statement.</returns>
        public string BuildUpdate(TableMapping m, bool includeTenant = false)
        {
            if (m.UpdateColumns.Length == 0)
                throw new NormConfigurationException(
                    $"Entity '{m.Type.Name}' has no mutable columns to update. Add at least one non-key, non-timestamp property.");

            // X1: cache key distinguishes tenant vs non-tenant SQL so both shapes can coexist.
            var cacheOp = includeTenant ? "UPDATE_TENANT" : "UPDATE";
            return _sqlCache.GetOrAdd((m.Type, m.TableName, cacheOp, m.SqlShapeKey), _ =>
            {
                var setCols = m.UpdateColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                // Client-managed concurrency token: write a fresh value in SET (@Token) so a stale
                // concurrent UPDATE affects zero rows. Providers without a native rowversion only.
                if (m.ClientManagedConcurrencyToken)
                    setCols.Add($"{m.TimestampColumn!.EscCol}={ParamPrefix}{m.TimestampColumn.PropName}");
                var set = string.Join(", ", setCols);

                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                if (m.TimestampColumn != null)
                {
                    var tc = m.TimestampColumn;
                    // When the token is also in SET (@Token), the WHERE compares the OLD value under a
                    // distinct name (@Token_orig) so the two bindings don't collide.
                    var whereParam = m.ClientManagedConcurrencyToken ? $"{tc.PropName}_orig" : tc.PropName;
                    whereCols.Add($"({tc.EscCol}={ParamPrefix}{whereParam} OR ({tc.EscCol} IS NULL AND {ParamPrefix}{whereParam} IS NULL))");
                }
                // X1: include tenant column in WHERE so direct UpdateAsync cannot cross-write rows
                // belonging to other tenants - parity with the batched SaveChangesAsync path.
                if (includeTenant && m.TenantColumn != null)
                    whereCols.Add($"{m.TenantColumn.EscCol}={ParamPrefix}{m.TenantColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"UPDATE {m.EscTable} SET {set} WHERE {where}";
            });
        }

        /// <summary>
        /// Builds a parameterized <c>DELETE</c> statement that filters by the primary
        /// key (and timestamp when applicable) to ensure a single row is targeted.
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <param name="includeTenant">When <c>true</c>, appends an AND predicate for the tenant
        /// column so the statement can only delete the current tenant's rows.</param>
        /// <returns>A <c>DELETE</c> SQL statement.</returns>
        public string BuildDelete(TableMapping m, bool includeTenant = false)
        {
            // X1: cache key distinguishes tenant vs non-tenant SQL so both shapes can coexist.
            var cacheOp = includeTenant ? "DELETE_TENANT" : "DELETE";
            return _sqlCache.GetOrAdd((m.Type, m.TableName, cacheOp, m.SqlShapeKey), _ =>
            {
                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                if (m.TimestampColumn != null)
                {
                    var tc = m.TimestampColumn;
                    whereCols.Add($"({tc.EscCol}={ParamPrefix}{tc.PropName} OR ({tc.EscCol} IS NULL AND {ParamPrefix}{tc.PropName} IS NULL))");
                }
                // X1: include tenant column in WHERE so direct DeleteAsync cannot cross-delete rows
                // belonging to other tenants - parity with the batched SaveChangesAsync path.
                if (includeTenant && m.TenantColumn != null)
                    whereCols.Add($"{m.TenantColumn.EscCol}={ParamPrefix}{m.TenantColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"DELETE FROM {m.EscTable} WHERE {where}";
            });
        }
        #endregion
    }
}
