using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class MySqlProvider
    {
        /// <summary>
        /// Performs a high-performance bulk insert using <c>INSERT INTO ... VALUES</c> statements grouped into batches.
        /// </summary>
        /// <typeparam name="T">Entity type being inserted.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> orchestrating the operation.</param>
        /// <param name="m">Mapping metadata for the entity's table.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows inserted.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var operationKey = $"MySql_BulkInsert_{m.Type.Name}";
            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);

            var bulkCopyType = Type.GetType("MySqlConnector.MySqlBulkCopy, MySqlConnector");
            var bulkCopyKey = GetBulkCopyCapabilityKey(ctx.RawConnection);
            if (bulkCopyType != null &&
                ctx.RawConnection.GetType().FullName == "MySqlConnector.MySqlConnection" &&
                !_bulkCopyUnavailable.ContainsKey(bulkCopyKey))
            {
                // Respect ambient CurrentTransaction; only create a new one if none is active.
                bool ownedTx = ctx.CurrentTransaction == null;
                DbTransaction transaction = ctx.CurrentTransaction
                    ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
                try
                {
                    dynamic bulkCopy = Activator.CreateInstance(bulkCopyType, ctx.RawConnection, transaction)!;
                    bulkCopy.DestinationTableName = m.EscTable.Trim('`');
                    bulkCopy.BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                    var totalInserted = 0;
                    for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
                    {
                        var batch = entityList.GetRange(i, Math.Min(sizing.OptimalBatchSize, entityList.Count - i));
                        using var table = GetDataTable(m);
                        LoadBulkInsertRows(table, insertableCols, batch);

                        var batchSw = Stopwatch.StartNew();
                        await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
                        batchSw.Stop();
                        totalInserted += table.Rows.Count;
                        BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                    }

                    // Use CancellationToken.None so a cancelled caller token after a successful commit
                    // does not cause a spurious OperationCanceledException for already-committed data.
                    if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                    ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
                    ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
                    return totalInserted;
                }
                catch (Exception ex) when (IsMySqlBulkCopyUnavailable(ex))
                {
                    _bulkCopyUnavailable.TryAdd(bulkCopyKey, true);
                    if (ownedTx)
                    {
                        try { await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                        catch (Exception rollbackEx) { throw new AggregateException(ex, rollbackEx); }
                    }
                }
                catch (Exception ex)
                {
                    if (ownedTx)
                    {
                        try { await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                        catch (Exception rollbackEx) { throw new AggregateException(ex, rollbackEx); }
                    }
                    throw;
                }
                finally
                {
                    if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
                }
            }

            var affected = await base.BulkInsertAsync(ctx, m, entityList, ct).ConfigureAwait(false);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, affected, sw.Elapsed);
            return affected;
        }

        private static string GetBulkCopyCapabilityKey(DbConnection connection)
            => $"{connection.GetType().FullName}|{connection.DataSource}|{connection.Database}";

        private static bool IsMySqlBulkCopyUnavailable(Exception ex)
        {
            for (var current = ex; current != null; current = current.InnerException)
            {
                if (current is NotSupportedException)
                    return true;

                if (current.Message.Contains("Loading local data is disabled", StringComparison.OrdinalIgnoreCase) ||
                    current.Message.Contains("AllowLoadLocalInfile", StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Updates multiple entities using a MySQL-optimized temp table approach for efficient bulk updates.
        /// Always uses MySQL-specific implementation with temp tables for optimal performance.
        /// </summary>
        /// <typeparam name="T">Entity type being updated.</typeparam>
        /// <param name="ctx">The active <see cref="DbContext"/>.</param>
        /// <param name="m">Table mapping for the entity.</param>
        /// <param name="entities">Entities containing updated values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            // A client-managed [Timestamp] token must be freshly stamped per row on each update; the
            // temp-table fast path below copies entity columns without changing the token, silently
            // losing a concurrent write. Route through the row-by-row BatchedUpdateAsync instead.
            if (m.ClientManagedConcurrencyToken)
                return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"`BulkUpdate_{Guid.NewGuid():N}`";
            // Build the SET from UpdateColumns (non-key, non-timestamp, non-db-generated), NOT every
            // non-key column. DB-generated non-key columns are created in the temp table but never
            // staged (StageForBulkUpdateAsync excludes them), so SETting them here would overwrite the
            // committed live value with the staged NULL.
            var nonKeyCols = m.UpdateColumns.ToList();
            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetStagingSqlType(c, m)}"));

            var tempCreated = false;
            var updatedCount = 0;
            try
            {
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    cmd.CommandText = $"CREATE TEMPORARY TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                tempCreated = true;

                await StageForBulkUpdateAsync(ctx, m, tempTableName, entities, ct).ConfigureAwait(false);

                var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
                var joinConditions = m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}").ToList();
                // X3: Include timestamp in join to enforce OCC semantics
                if (m.TimestampColumn != null)
                    joinConditions.Add($"T1.{m.TimestampColumn.EscCol} = T2.{m.TimestampColumn.EscCol}");
                var joinClause = string.Join(" AND ", joinConditions);

                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    // X1: Add tenant predicate to prevent cross-tenant bulk updates
                    var tenantCol = ctx.Options.TenantProvider != null
                        ? ctx.RequireTenantColumn(m, "MySQL bulk update")
                        : null;
                    if (tenantCol != null)
                        cmd.AddParam($"{ParamPrefix}__tenant_bulk", ctx.GetRequiredTenantId(m, "MySQL bulk update"));
                    cmd.CommandText = BuildBulkUpdateSql(
                        m.EscTable,
                        tempTableName,
                        setClause,
                        joinClause,
                        tenantCol?.EscCol);
                    updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                // X4: Always drop temp table to prevent session resource leak
                if (tempCreated)
                {
                    try
                    {
                        await using var dropCmd = ctx.CreateCommand();
                        dropCmd.CommandText = BuildBulkUpdateDropTempTableSql(tempTableName);
                        await dropCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (DbException ex)
                    {
                        Trace.TraceWarning($"MySqlProvider: Failed to drop temp table {tempTableName} during BulkUpdate cleanup: {ex.Message}");
                    }
                }
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
            return updatedCount;
        }

        internal string BuildBulkUpdateSql(string tableName, string tempTableName, string setClause, string joinClause, string? tenantColumn)
        {
            var sql = $"UPDATE {tableName} T1 JOIN {tempTableName} T2 ON {joinClause} SET {setClause}";
            return tenantColumn == null
                ? sql
                : $"{sql} WHERE T1.{tenantColumn} = {ParamPrefix}__tenant_bulk";
        }

        internal static string BuildBulkUpdateDropTempTableSql(string tempTableName)
            => $"DROP TEMPORARY TABLE IF EXISTS {tempTableName}";

        /// <summary>
        /// Stages rows into the bulk-UPDATE temp table INCLUDING key columns — even a DB-generated
        /// identity key — so the UPDATE ... JOIN correlates the target to the staging table on the key.
        /// The shared bulk-insert path excludes DB-generated columns (correct for a real insert, where
        /// the DB assigns them); reusing it here left the staged key NULL, so the join matched nothing
        /// and every update was silently discarded.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk staging reflects over entity members; trimming may remove the required members.")]
        private async Task StageForBulkUpdateAsync<T>(DbContext ctx, TableMapping m, string tempTable, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var cols = m.Columns.Where(c => !c.IsDbGenerated || c.IsKey).ToList();
            var list = entities.ToList();
            if (list.Count == 0 || cols.Count == 0) return;

            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var batchSize = Math.Max(1, Math.Min(list.Count, (MaxParameters - 10) / cols.Count));
            for (int start = 0; start < list.Count; start += batchSize)
            {
                var count = Math.Min(batchSize, list.Count - start);
                var sb = new StringBuilder();
                sb.Append("INSERT INTO ").Append(tempTable).Append(" (").Append(colNames).Append(") VALUES ");
                await using var cmd = ctx.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                var pIndex = 0;
                for (int i = 0; i < count; i++)
                {
                    sb.Append(i > 0 ? ",(" : "(");
                    for (int j = 0; j < cols.Count; j++)
                    {
                        var pName = $"{ParamPrefix}s{pIndex++}";
                        var raw = cols[j].Getter(list[start + i]);
                        var conv = cols[j].Converter;
                        cmd.AddParam(pName, conv != null ? conv.ConvertToProvider(raw) : raw);
                        sb.Append(j > 0 ? "," : string.Empty).Append(pName);
                    }
                    sb.Append(')');
                }
                cmd.CommandText = sb.ToString();
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Deletes multiple records using MySQL-optimized WHERE IN clauses for efficient bulk deletes.
        /// Always uses MySQL-specific implementation with batched WHERE IN for optimal performance.
        /// </summary>
        /// <typeparam name="T">Entity type to delete.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> managing the connection.</param>
        /// <param name="m">Mapping describing the table and key columns.</param>
        /// <param name="entities">Entities whose keys determine the rows to remove.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var keyCols = m.KeyColumns.ToList();
            if (keyCols.Count == 0)
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");

            // Optimistic concurrency: the native IN(keys) path matches by key only, which
            // cannot honour a concurrency token. Route token-carrying entities through the
            // shared fallback, which matches (key AND token) so a row another writer has
            // updated is skipped, not destroyed. Token-less entities keep the fast path.
            if (m.TimestampColumn != null)
                return await base.BatchedDeleteAsync(ctx, m, entityList, ct).ConfigureAwait(false);

            var operationKey = $"MySql_BulkDelete_{m.Type.Name}";
            var hasTenant = ctx.Options.TenantProvider != null;
            var tenantColumn = hasTenant ? ctx.RequireTenantColumn(m, "MySQL bulk delete") : null;
            var paramsPerEntity = keyCols.Count + (hasTenant ? 1 : 0);
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);
            var maxBatchForProvider = MaxParameters / Math.Max(1, paramsPerEntity);
            var batchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));

            var totalDeleted = 0;
            for (int i = 0; i < entityList.Count; i += batchSize)
            {
                var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                await using var cmd = ctx.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                var valueClauses = new List<string>();
                var paramIndex = 0;
                foreach (var entity in batch)
                {
                    var paramNames = new List<string>();
                    foreach (var col in keyCols)
                    {
                        var pName = $"{ParamPrefix}p{paramIndex++}";
                        var raw = col.Getter(entity);
                        var conv = col.Converter;
                        cmd.AddParam(pName, conv != null ? conv.ConvertToProvider(raw) : raw);
                        paramNames.Add(pName);
                    }
                    if (keyCols.Count == 1)
                        valueClauses.Add(paramNames[0]);
                    else
                        valueClauses.Add($"({string.Join(", ", paramNames)})");
                }

                string whereClause;
                if (keyCols.Count == 1)
                {
                    whereClause = $"{keyCols[0].EscCol} IN ({string.Join(", ", valueClauses)})";
                }
                else
                {
                    var cols = string.Join(", ", keyCols.Select(c => c.EscCol));
                    whereClause = $"({cols}) IN ({string.Join(", ", valueClauses)})";
                }

                // X1: Add tenant predicate to prevent cross-tenant bulk deletes
                if (hasTenant)
                {
                    var tenantParam = $"{ParamPrefix}__tenant_bulk";
                    cmd.AddParam(tenantParam, ctx.GetRequiredTenantId(m, "MySQL bulk delete"));
                    whereClause += $" AND {tenantColumn!.EscCol} = {tenantParam}";
                }
                cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {whereClause}";
                var batchSw = Stopwatch.StartNew();
                totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                batchSw.Stop();
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }

        /// <summary>
        /// Staging temp-table column type. Decimal columns honour the model's configured
        /// precision/scale (HasPrecision) so bulk staging never rounds a value the
        /// destination can hold — the old flat DECIMAL(18,2) silently truncated any
        /// decimal with more than two fractional digits. Unconfigured decimals use a wide
        /// DECIMAL(38,18) default instead of (18,2); configure HasPrecision to match the
        /// destination column for exactness (no single SQL DECIMAL holds every .NET decimal).
        /// </summary>
        private static string GetStagingSqlType(Mapping.Column c, TableMapping m)
        {
            // Values are staged post-conversion (ConvertToProvider), so the staging
            // column must be typed for the PROVIDER representation, not the CLR
            // property (e.g. an enum stored as string must stage as text).
            var effective = c.Converter?.ProviderType ?? c.Prop.PropertyType;
            var underlying = Nullable.GetUnderlyingType(effective) ?? effective;
            if (underlying == typeof(decimal))
            {
                if (m.FluentConfiguration?.Precisions is { } precisions
                    && precisions.TryGetValue(c.Prop, out var pc))
                {
                    return $"DECIMAL({pc.Precision},{pc.Scale ?? 0})";
                }
                return "DECIMAL(38,18)";
            }
            return GetSqlType(underlying);
        }

        /// <summary>
        /// Maps a CLR type to its corresponding MySQL column type. Staged values are
        /// written back into the destination by UPDATE ... JOIN, so every mapping must
        /// preserve the full value: DATETIME/TIME carry (6) because the bare types
        /// round fractional seconds away (a bare DATETIME turned 12:30:15.7 into
        /// 12:30:16 — silently persisted), strings/blobs use the LONG variants because
        /// TEXT/BLOB cap at 64KB, and every numeric type maps natively rather than
        /// falling through to a text column.
        /// </summary>
        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t.IsEnum) t = Enum.GetUnderlyingType(t);
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(short)) return "SMALLINT";
            if (t == typeof(byte)) return "TINYINT UNSIGNED";
            if (t == typeof(sbyte)) return "TINYINT";
            if (t == typeof(ushort)) return "SMALLINT UNSIGNED";
            if (t == typeof(uint)) return "INT UNSIGNED";
            if (t == typeof(ulong)) return "BIGINT UNSIGNED";
            if (t == typeof(string)) return "LONGTEXT";
            if (t == typeof(DateTime)) return "DATETIME(6)";
            if (t == typeof(DateTimeOffset)) return "DATETIME(6)";
            if (t == typeof(DateOnly)) return "DATE";
            if (t == typeof(TimeOnly)) return "TIME(6)";
            if (t == typeof(TimeSpan)) return "TIME(6)";
            if (t == typeof(bool)) return "TINYINT(1)";
            if (t == typeof(decimal)) return "DECIMAL(38,18)";
            if (t == typeof(double)) return "DOUBLE";
            if (t == typeof(float)) return "FLOAT";
            if (t == typeof(Guid)) return "CHAR(36)";
            if (t == typeof(byte[])) return "LONGBLOB";
            if (t == typeof(char)) return "CHAR(1)";
            return "LONGTEXT";
        }

        /// <summary>
        /// Returns a cloned DataTable with the schema matching the specified columns.
        /// Rows are loaded post-conversion (ConvertToProvider), so each column is typed
        /// for the PROVIDER representation — typing by the CLR property would make
        /// DataTable reject or coerce converted values (e.g. a DateTime stored as
        /// BIGINT ticks). Cached per mapping (not per CLR type) because two contexts
        /// can map the same entity with different converters.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2072",
            Justification = "Column types are mapped entity property types rooted by TableMapping registration; DataColumn only needs the type identity for bulk-load schema definition.")]
        private static DataTable GetDataTable(TableMapping m)
        {
            var schema = _tableSchemas.GetOrAdd(m, static mapping =>
            {
                var dt = new DataTable();
                foreach (var c in mapping.Columns.Where(c => !c.IsDbGenerated))
                {
                    var effective = c.Converter?.ProviderType ?? c.Prop.PropertyType;
                    var columnType = Nullable.GetUnderlyingType(effective) ?? effective;
                    if (columnType.IsEnum) columnType = Enum.GetUnderlyingType(columnType);
                    dt.Columns.Add(c.PropName, columnType);
                }
                return dt;
            });
            return schema.Clone();
        }

        private static void LoadBulkInsertRows<T>(DataTable table, List<Column> cols, List<T> batch) where T : class
        {
            var values = new object[cols.Count];
            table.BeginLoadData();
            try
            {
                for (int i = 0; i < batch.Count; i++)
                {
                    var entity = batch[i];
                    for (int j = 0; j < cols.Count; j++)
                    {
                        var raw = cols[j].Getter(entity);
                        var conv = cols[j].Converter;
                        var val = (conv != null ? conv.ConvertToProvider(raw) : raw) ?? DBNull.Value;
                        // Match every other write path's ulong contract (ParameterAssign.ToStorableInt64):
                        // store as signed 64-bit, failing loud above long.MaxValue rather than staging a
                        // wrapped value into the bulk-load DataTable.
                        if (val is ulong ul) val = nORM.Query.ParameterAssign.ToStorableInt64(ul);
                        values[j] = val;
                    }

                    table.Rows.Add(values);
                }
            }
            finally
            {
                table.EndLoadData();
            }
        }
    }
}
