using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class SqlServerProvider
    {
        #region SQL Server Bulk Operations
        /// <summary>
        /// Performs a high-throughput bulk insert using the SQL Server <c>SqlBulkCopy</c> API when available,
        /// falling back to batched inserts otherwise.
        /// </summary>
        /// <typeparam name="T">Entity type being inserted.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> driving the operation.</param>
        /// <param name="m">Mapping metadata for the entity.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total rows inserted.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();

            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            if (entityList.Count <= SqlBulkCopySmallBatchThreshold)
                return await base.BulkInsertAsync(ctx, m, entityList, ct).ConfigureAwait(false);

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var operationKey = $"SqlServer_BulkInsert_{m.Type.Name}";

            // When temporal versioning is enabled the table carries an AFTER INSERT trigger that writes
            // each history row. SqlBulkCopy suppresses triggers by default, so a >512-row bulk insert
            // would write ZERO history (and leave those rows with no open history version, so the first
            // later update/delete also cannot preserve their prior state). Fire triggers so the temporal
            // (and any user-defined) trigger runs within the same transaction as the copy.
            var bulkCopyOptions = ctx.Options.IsTemporalVersioningEnabled
                ? SqlBulkCopyOptions.FireTriggers
                : SqlBulkCopyOptions.Default;

            var totalInserted = await ExecuteBulkOperationAsync(ctx, m, entityList, operationKey,
                async (batch, tx, token) =>
                {
                    using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.RawConnection, bulkCopyOptions, (SqlTransaction)tx)
                    {
                        DestinationTableName = m.EscTable,
                        BatchSize = batch.Count,
                        EnableStreaming = true,
                        BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds
                    };

                    foreach (var col in insertableCols)
                    {
                        var unescapedName = UnescapeSqlServerIdentifier(col.EscCol);
                        bulkCopy.ColumnMappings.Add(col.PropName, unescapedName);
                    }

                    using var reader = new EntityDataReader<T>(batch, insertableCols);
                    await bulkCopy.WriteToServerAsync(reader, token).ConfigureAwait(false);
                    return reader.RecordsProcessed;
                }, ct).ConfigureAwait(false);

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2: Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
            return totalInserted;
        }

        /// <summary>
        /// Updates multiple entities in batches using parameterized <c>UPDATE</c> statements.
        /// </summary>
        /// <typeparam name="T">Type of entity being updated.</typeparam>
        /// <param name="ctx">Database context providing connection and options.</param>
        /// <param name="m">Mapping information for the entity.</param>
        /// <param name="entities">Entities containing updated values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            // Build the SET from UpdateColumns (non-key, non-timestamp, non-db-generated), NOT every
            // non-key column. DB-generated non-key columns are created in the temp table but never
            // staged (BulkInsertInternalAsync excludes them), so SETting them here would overwrite the
            // committed live value with the staged NULL. The rowversion is likewise DB-managed.
            var nonKeyCols = m.UpdateColumns.ToList();
            if (nonKeyCols.Count == 0) return 0;
            var tempTableName = $"#BulkUpdate_{Guid.NewGuid():N}";
            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetStagingSqlType(c, m)}"));

            var tempCreated = false;
            var updatedCount = 0;
            try
            {
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    cmd.CommandText = $"CREATE TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                tempCreated = true;

                await BulkInsertInternalAsync(ctx, m, entities, tempTableName, ct).ConfigureAwait(false);

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
                        ? ctx.RequireTenantColumn(m, "SQL Server bulk update")
                        : null;
                    if (tenantCol != null)
                        cmd.AddParam("@__tenant_bulk", ctx.GetRequiredTenantId(m, "SQL Server bulk update"));
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
                // X4: Always drop temp table to prevent resource leak
                if (tempCreated)
                {
                    try
                    {
                        await using var dropCmd = ctx.CreateCommand();
                        dropCmd.CommandText = BuildDropTempTableSql(tempTableName);
                        await dropCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (DbException ex)
                    {
                        Trace.TraceWarning($"SqlServerProvider: Failed to drop temp table {tempTableName} during BulkUpdate cleanup: {ex.Message}");
                    }
                }
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2: Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
            return updatedCount;
        }

        private async Task<int> BulkInsertInternalAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, string destinationTableName, CancellationToken ct) where T : class
        {
            // This helper stages rows into the bulk-UPDATE temp table (its only caller). Key columns
            // MUST be staged — even a DB-generated identity key — because the UPDATE joins the target
            // to the staging table on the key. Excluding a DB-generated key left the staged key NULL,
            // so the join matched nothing and every update was silently discarded (lost update).
            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated || c.IsKey).ToList();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var operationKey = $"SqlServer_BulkInsert_{destinationTableName}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);

            var totalInserted = 0;
            for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
            {
                var batch = entityList.GetRange(i, Math.Min(sizing.OptimalBatchSize, entityList.Count - i));
                using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.RawConnection)
                {
                    DestinationTableName = destinationTableName,
                    BatchSize = batch.Count,
                    EnableStreaming = true,
                    BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds
                };

                foreach (var col in insertableCols)
                    bulkCopy.ColumnMappings.Add(col.PropName, UnescapeSqlServerIdentifier(col.EscCol));

                using var reader = new EntityDataReader<T>(batch, insertableCols);
                var batchSw = Stopwatch.StartNew();
                await bulkCopy.WriteToServerAsync(reader, ct).ConfigureAwait(false);
                batchSw.Stop();
                totalInserted += reader.RecordsProcessed;
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            return totalInserted;
        }

        /// <summary>
        /// Deletes entities in batches using parameterized <c>DELETE</c> statements based on primary keys.
        /// </summary>
        /// <typeparam name="T">Type of entity to delete.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping describing the table and key columns.</param>
        /// <param name="entities">Entities identifying rows to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            // Optimistic concurrency: the temp-table key JOIN matches by key only, which
            // cannot honour a concurrency token. Route token-carrying entities through the
            // shared fallback, which matches (key AND token) so a row another writer has
            // updated is skipped, not destroyed. Token-less entities keep the fast path.
            if (m.TimestampColumn != null)
                return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"#BulkDelete_{Guid.NewGuid():N}";
            var keyColDefs = string.Join(", ", m.KeyColumns.Select(c => $"{c.EscCol} {GetStagingSqlType(c, m)}"));

            var tempCreated = false;
            int deletedCount = 0;
            try
            {
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    cmd.CommandText = $"CREATE TABLE {tempTableName} ({keyColDefs})";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                tempCreated = true;

                using (var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.RawConnection)
                {
                    DestinationTableName = tempTableName,
                    BatchSize = ctx.Options.BulkBatchSize,
                    EnableStreaming = true
                })
                {
                    using var table = GetKeyTable(m);
                    var batchCount = 0;
                    foreach (var entity in entities)
                    {
                        table.Rows.Add(m.KeyColumns.Select(c =>
                        {
                            var raw = c.Getter(entity);
                            var value = c.Converter != null ? c.Converter.ConvertToProvider(raw) : raw;
                            return value ?? DBNull.Value;
                        }).ToArray());
                        batchCount++;
                        if (batchCount >= ctx.Options.BulkBatchSize)
                        {
                            await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
                            table.Clear();
                            batchCount = 0;
                        }
                    }
                    if (batchCount > 0)
                        await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
                }

                var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
                await using (var cmd = ctx.CreateCommand())
                {
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    // X1: Add tenant predicate to prevent cross-tenant bulk deletes
                    var tenantCol = ctx.Options.TenantProvider != null
                        ? ctx.RequireTenantColumn(m, "SQL Server bulk delete")
                        : null;
                    if (tenantCol != null)
                        cmd.AddParam("@__tenant_bulk", ctx.GetRequiredTenantId(m, "SQL Server bulk delete"));
                    cmd.CommandText = BuildBulkDeleteSql(
                        m.EscTable,
                        tempTableName,
                        joinClause,
                        tenantCol?.EscCol);
                    deletedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                // X4: Always drop temp table to prevent resource leak
                if (tempCreated)
                {
                    try
                    {
                        await using var dropCmd = ctx.CreateCommand();
                        dropCmd.CommandText = BuildDropTempTableSql(tempTableName);
                        await dropCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (DbException ex)
                    {
                        Trace.TraceWarning($"SqlServerProvider: Failed to drop temp table {tempTableName} during BulkDelete cleanup: {ex.Message}");
                    }
                }
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2: Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, deletedCount, sw.Elapsed);
            return deletedCount;
        }

        internal static string BuildBulkUpdateSql(string tableName, string tempTableName, string setClause, string joinClause, string? tenantColumn)
        {
            var sql = $"UPDATE T1 SET {setClause} FROM {tableName} T1 JOIN {tempTableName} T2 ON {joinClause}";
            return tenantColumn == null
                ? sql
                : $"{sql} WHERE T1.{tenantColumn} = @__tenant_bulk";
        }

        internal static string BuildBulkDeleteSql(string tableName, string tempTableName, string joinClause, string? tenantColumn)
        {
            var sql = $"DELETE T1 FROM {tableName} T1 JOIN {tempTableName} T2 ON {joinClause}";
            return tenantColumn == null
                ? sql
                : $"{sql} WHERE T1.{tenantColumn} = @__tenant_bulk";
        }

        internal static string BuildDropTempTableSql(string tempTableName)
            => $"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}";

        /// <summary>
        /// Staging temp-table column type. For decimal columns this honours the model's
        /// configured precision/scale (HasPrecision) so bulk staging never rounds a value
        /// the destination column can hold — the old flat DECIMAL(18,2) silently truncated
        /// any decimal with more than two fractional digits (money/rates). When precision
        /// is not configured, a wide DECIMAL(38,18) default replaces (18,2); note that no
        /// single SQL DECIMAL holds every .NET decimal (scale up to 28), so configuring
        /// HasPrecision to match the destination column is the correct path for exactness.
        /// </summary>
        private static string GetStagingSqlType(Mapping.Column c, TableMapping m)
        {
            // Rows are staged post-conversion (EntityDataReader applies ConvertToProvider),
            // so the staging column must be typed for the PROVIDER representation — typing
            // by the CLR property makes SqlBulkCopy reject or coerce converted values
            // (e.g. a DateTime stored as BIGINT ticks).
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

        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t.IsEnum) t = Enum.GetUnderlyingType(t);
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(short)) return "SMALLINT";
            if (t == typeof(byte)) return "TINYINT";
            if (t == typeof(string)) return "NVARCHAR(MAX)";
            if (t == typeof(DateTime)) return "DATETIME2";
            if (t == typeof(DateOnly)) return "DATE";
            if (t == typeof(TimeOnly)) return "TIME";
            if (t == typeof(DateTimeOffset)) return "DATETIMEOFFSET";
            if (t == typeof(TimeSpan)) return "TIME";
            if (t == typeof(bool)) return "BIT";
            // Wide default: staging overrides decimals via GetStagingSqlType, so this
            // entry serves the temporal history fallback, where a narrow scale would
            // silently round copied rows.
            if (t == typeof(decimal)) return "DECIMAL(38,18)";
            if (t == typeof(double)) return "FLOAT";
            if (t == typeof(float)) return "REAL";
            if (t == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (t == typeof(byte[])) return "VARBINARY(MAX)";
            if (t == typeof(char)) return "NCHAR(1)";
            if (t == typeof(sbyte)) return "SMALLINT";
            if (t == typeof(ushort)) return "INT";
            if (t == typeof(uint)) return "BIGINT";
            if (t == typeof(ulong)) return "DECIMAL(20,0)";
            return "NVARCHAR(MAX)";
        }

        /// <summary>
        /// Unescapes a SQL Server bracket-quoted identifier by removing outer brackets
        /// and reversing the <c>]]</c> to <c>]</c> escape applied by <see cref="Escape"/>.
        /// </summary>
        /// <param name="escapedIdentifier">The escaped identifier (e.g., <c>[ColumnName]</c> or <c>[My]]Column]</c>).</param>
        /// <returns>The unescaped identifier (e.g., <c>ColumnName</c> or <c>My]Column</c>).</returns>
        private static string UnescapeSqlServerIdentifier(string escapedIdentifier)
        {
            if (string.IsNullOrEmpty(escapedIdentifier))
                return escapedIdentifier;

            // Remove outer brackets only if present
            if (escapedIdentifier.StartsWith('[') && escapedIdentifier.EndsWith(']') && escapedIdentifier.Length >= 2)
            {
                var inner = escapedIdentifier.Substring(1, escapedIdentifier.Length - 2);
                // Reverse the ]] escape applied by Escape()
                return inner.Replace("]]", "]");
            }

            return escapedIdentifier;
        }

        /// <summary>
        /// Key rows are staged post-conversion so the temp-table join compares the
        /// PROVIDER representation the destination actually stores — a model-typed
        /// key column made the delete join match nothing for converter keys and the
        /// bulk delete silently removed zero rows. Cached per mapping because two
        /// contexts can map the same entity with different converters.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2072",
            Justification = "Key column types are mapped entity property types rooted by TableMapping registration; DataColumn only needs the type identity for TVP schema definition.")]
        private static DataTable GetKeyTable(TableMapping m)
        {
            var schema = _keyTableSchemas.GetOrAdd(m, static mapping =>
            {
                var dt = new DataTable();
                foreach (var c in mapping.KeyColumns)
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

        private sealed class EntityDataReader<T> : IDataReader where T : class
        {
            private readonly IEnumerator<T> _enumerator;
            private readonly List<Column> _columns;
            private bool _disposed;
            private T? _current;

            public int RecordsProcessed { get; private set; }

            public EntityDataReader(IEnumerable<T> entities, List<Column> columns)
            {
                _enumerator = entities.GetEnumerator();
                _columns = columns;
            }

            /// <summary>
            /// Advances the reader to the next record in the underlying entity sequence.
            /// </summary>
            /// <returns><c>true</c> if another record is available; otherwise, <c>false</c>.</returns>
            public bool Read()
            {
                if (_enumerator.MoveNext())
                {
                    _current = _enumerator.Current;
                    RecordsProcessed++;
                    return true;
                }
                return false;
            }

            /// <summary>
            /// Gets the number of fields exposed by the data reader.
            /// </summary>
            public int FieldCount => _columns.Count;

            /// <summary>
            /// Gets the value of the specified field by ordinal position.
            /// </summary>
            public object this[int i] => GetValue(i);

            /// <summary>
            /// Gets the value of the specified field by column name.
            /// </summary>
            public object this[string name] => GetValue(GetOrdinal(name));

            /// <summary>
            /// Retrieves the value of the field at the given ordinal.
            /// </summary>
            /// <param name="i">Zero-based column ordinal.</param>
            /// <returns>The boxed value of the column or <see cref="DBNull"/>.</returns>
            public object GetValue(int i)
            {
                if (_current == null) throw new InvalidOperationException("No current record");
                // Apply the value converter like every non-bulk write path so SqlBulkCopy writes the
                // converter-encoded value, not the raw model value (which read-back would corrupt).
                var raw = _columns[i].Getter(_current);
                var conv = _columns[i].Converter;
                var val = (conv != null ? conv.ConvertToProvider(raw) : raw) ?? DBNull.Value;
                // Match every other write path's ulong contract (ParameterAssign.ToStorableInt64):
                // store as signed 64-bit, failing loud above long.MaxValue rather than letting
                // SqlBulkCopy persist a wrapped value.
                if (val is ulong ul) val = nORM.Query.ParameterAssign.ToStorableInt64(ul);
                return val;
            }

            /// <summary>
            /// Gets the name of the column at the specified ordinal.
            /// </summary>
            public string GetName(int i) => _columns[i].PropName;

            /// <summary>
            /// Gets the data type name of the column at the specified ordinal.
            /// </summary>
            public string GetDataTypeName(int i) => GetFieldType(i).Name;

            /// <summary>
            /// Gets the <see cref="Type"/> of the column at the specified ordinal.
            /// </summary>
            [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2073",
                Justification = "Mapped entity property types are rooted by TableMapping registration and their members are accessed by the materializer, so the trimmer preserves them.")]
            [return: System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicFields | System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)]
            public Type GetFieldType(int i) => _columns[i].Prop.PropertyType;

            /// <summary>
            /// Returns the zero-based column ordinal given the column name.
            /// </summary>
            /// <exception cref="IndexOutOfRangeException">Thrown when the column name is not found.</exception>
            public int GetOrdinal(string name)
            {
                var index = _columns.FindIndex(c => c.PropName == name);
                if (index < 0)
                    throw new IndexOutOfRangeException($"Column '{name}' not found in the reader.");
                return index;
            }

            /// <summary>
            /// Determines whether the column at the specified ordinal is set to <see cref="DBNull"/>.
            /// </summary>
            public bool IsDBNull(int i) => GetValue(i) == DBNull.Value;

            /// <summary>
            /// Copies field values into the provided array.
            /// </summary>
            /// <param name="values">Destination array.</param>
            /// <returns>The number of values copied.</returns>
            public int GetValues(object[] values)
            {
                var count = Math.Min(values.Length, FieldCount);
                for (var i = 0; i < count; i++)
                    values[i] = GetValue(i);
                return count;
            }

            /// <summary>
            /// Advances to the next result set. Always returns <c>false</c> as only a single
            /// result set is supported.
            /// </summary>
            public bool NextResult() => false;

            /// <summary>
            /// Gets the depth of nesting for the current row. Always <c>0</c> for this reader.
            /// </summary>
            public int Depth => 0;

            /// <summary>
            /// Gets a value indicating whether the reader is closed.
            /// </summary>
            public bool IsClosed => _disposed;

            /// <summary>
            /// Gets the number of rows affected. Always <c>-1</c> for this reader.
            /// </summary>
            public int RecordsAffected => -1;

            /// <summary>
            /// Returns a <see cref="DataTable"/> describing the column metadata for this reader.
            /// Implemented to support SqlBulkCopy scenarios that rely on schema information.
            /// </summary>
            [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2111",
                Justification = "The DataType schema column stores Type instances only as SqlBulkCopy metadata; no member of those types is invoked through the schema table.")]
            public DataTable? GetSchemaTable()
            {
                var schemaTable = new DataTable("SchemaTable");

                // Add standard schema columns used by SqlBulkCopy and other data readers
                schemaTable.Columns.Add("ColumnName", typeof(string));
                schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
                schemaTable.Columns.Add("ColumnSize", typeof(int));
                schemaTable.Columns.Add("DataType", typeof(Type));
                schemaTable.Columns.Add("AllowDBNull", typeof(bool));
                schemaTable.Columns.Add("IsKey", typeof(bool));
                schemaTable.Columns.Add("IsUnique", typeof(bool));
                schemaTable.Columns.Add("IsReadOnly", typeof(bool));

                // Populate schema rows from column metadata
                for (int i = 0; i < _columns.Count; i++)
                {
                    var column = _columns[i];
                    var row = schemaTable.NewRow();

                    row["ColumnName"] = column.PropName;
                    row["ColumnOrdinal"] = i;
                    row["ColumnSize"] = -1; // Unknown/variable size
                    row["DataType"] = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
                    row["AllowDBNull"] = Nullable.GetUnderlyingType(column.Prop.PropertyType) != null || !column.Prop.PropertyType.IsValueType;
                    row["IsKey"] = column.IsKey;
                    row["IsUnique"] = column.IsKey;
                    row["IsReadOnly"] = false;

                    schemaTable.Rows.Add(row);
                }

                return schemaTable;
            }

            /// <summary>
            /// Closes the reader and releases the underlying enumerator.
            /// </summary>
            public void Close() => Dispose();

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="bool"/>.
            /// </summary>
            public bool GetBoolean(int i) => (bool)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="byte"/>.
            /// </summary>
            public byte GetByte(int i) => (byte)GetValue(i);

            /// <summary>
            /// Reads a stream of bytes from the specified column.
            /// Supports bulk copying entities with byte[] properties.
            /// </summary>
            public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
            {
                if (_current == null)
                    throw new InvalidOperationException("No current record");

                var value = _columns[i].Getter(_current);
                if (value == null || value == DBNull.Value)
                    return 0;

                if (value is not byte[] bytes)
                    throw new InvalidCastException($"Column {i} is not a byte array");

                // If buffer is null, return the total length of the data
                if (buffer == null)
                    return bytes.Length;

                // Calculate how many bytes to copy
                var bytesToCopy = Math.Min(length, bytes.Length - (int)fieldOffset);
                if (bytesToCopy <= 0)
                    return 0;

                // Copy the data
                Array.Copy(bytes, (int)fieldOffset, buffer, bufferoffset, bytesToCopy);
                return bytesToCopy;
            }

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="char"/>.
            /// </summary>
            public char GetChar(int i) => (char)GetValue(i);

            /// <summary>
            /// Reads a stream of characters from the specified column.
            /// Supports bulk copying entities with char[] or string properties.
            /// </summary>
            public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
            {
                if (_current == null)
                    throw new InvalidOperationException("No current record");

                var value = _columns[i].Getter(_current);
                if (value == null || value == DBNull.Value)
                    return 0;

                // Use string.CopyTo to avoid allocating char[] for entire string.
                // ToCharArray() allocates even if SqlBulkCopy only needs a small chunk.
                if (value is string str)
                {
                    // If buffer is null, return the total length of the string
                    if (buffer == null)
                        return str.Length;

                    // Calculate how many characters to copy from the string
                    var startIndex = (int)fieldoffset;
                    if (startIndex >= str.Length)
                        return 0;

                    var charsToCopy = Math.Min(length, str.Length - startIndex);
                    if (charsToCopy <= 0)
                        return 0;

                    // Copy directly from string to buffer without intermediate allocation
                    str.CopyTo(startIndex, buffer, bufferoffset, charsToCopy);
                    return charsToCopy;
                }
                else if (value is char[] charArray)
                {
                    // If buffer is null, return the total length of the char array
                    if (buffer == null)
                        return charArray.Length;

                    // Calculate how many characters to copy from the char array
                    var charsToCopy = Math.Min(length, charArray.Length - (int)fieldoffset);
                    if (charsToCopy <= 0)
                        return 0;

                    // Copy the data from char array
                    Array.Copy(charArray, (int)fieldoffset, buffer, bufferoffset, charsToCopy);
                    return charsToCopy;
                }
                else
                {
                    throw new InvalidCastException($"Column {i} is not a string or char array");
                }
            }

            /// <summary>
            /// Gets an <see cref="IDataReader"/> for the specified column. Not supported in this reader.
            /// </summary>
            public IDataReader GetData(int i) => throw new NotSupportedException();

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="DateTime"/>.
            /// </summary>
            public DateTime GetDateTime(int i) => (DateTime)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="decimal"/>.
            /// </summary>
            public decimal GetDecimal(int i) => (decimal)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="double"/>.
            /// </summary>
            public double GetDouble(int i) => (double)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="float"/>.
            /// </summary>
            public float GetFloat(int i) => (float)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="Guid"/>.
            /// </summary>
            public Guid GetGuid(int i) => (Guid)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="short"/>.
            /// </summary>
            public short GetInt16(int i) => (short)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="int"/>.
            /// </summary>
            public int GetInt32(int i) => (int)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="long"/>.
            /// </summary>
            public long GetInt64(int i) => (long)GetValue(i);

            /// <summary>
            /// Gets the value of the specified column cast to <see cref="string"/>.
            /// </summary>
            public string GetString(int i) => (string)GetValue(i);

            /// <summary>
            /// Releases resources associated with the reader.
            /// </summary>
            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;
                _enumerator.Dispose();
            }
        }
        #endregion
    }
}
