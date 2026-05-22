using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class PostgresProvider
    {
        /// <summary>
        /// Inserts entities using PostgreSQL's <c>COPY</c> binary protocol when available, falling back to batched inserts otherwise.
        /// </summary>
        /// <typeparam name="T">Type of entity being inserted.</typeparam>
        /// <param name="ctx">Context providing the connection.</param>
        /// <param name="m">Mapping metadata for the entity.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The number of rows inserted.</returns>
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
            if (cols.Length == 0) return 0;

            var operationKey = $"Postgres_BulkInsert_{m.Type.Name}";

            var npgConnType = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
            if (npgConnType != null && npgConnType.IsInstanceOfType(ctx.Connection))
            {
                var copySql = $"COPY {m.EscTable} ({string.Join(", ", cols.Select(c => c.EscCol))}) FROM STDIN (FORMAT BINARY)";
                var beginMethod = ctx.Connection.GetType().GetMethod("BeginBinaryImport", new[] { typeof(string) });
                var importerObj = beginMethod?.Invoke(ctx.Connection, new object[] { copySql });
                if (importerObj == null) throw new InvalidOperationException("BeginBinaryImport not available");
                try
                {
                    dynamic importer = importerObj;
                    foreach (var entity in entityList)
                    {
                        importer.StartRow();
                        foreach (var col in cols)
                        {
                            var val = col.Getter(entity);
                            if (val == null) importer.WriteNull();
                            else importer.Write(val);
                        }
                    }
                    await importer.CompleteAsync(ct).ConfigureAwait(false);
                }
                finally
                {
                    if (importerObj is IAsyncDisposable iad) await iad.DisposeAsync().ConfigureAwait(false);
                    else if (importerObj is IDisposable id) id.Dispose();
                }
                BatchSizer.RecordBatchPerformance(operationKey, entityList.Count, sw.Elapsed, entityList.Count);
                ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // Invalidate query cache after bulk write to prevent stale reads
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, entityList.Count, sw.Elapsed);
                return entityList.Count;
            }

            var recordsAffected = await ExecuteBulkOperationAsync(ctx, m, entityList, operationKey,
                (batch, tx, token) => ExecutePostgresBatchInsert(ctx, tx, m, cols, batch, token), ct).ConfigureAwait(false);

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }

        /// <summary>
        /// Executes a single batch of inserts using PostgreSQL's multi-row VALUES syntax.
        /// </summary>
        private async Task<int> ExecutePostgresBatchInsert<T>(DbContext ctx, DbTransaction transaction,
            TableMapping mapping, Column[] cols, List<T> batch, CancellationToken ct) where T : class
        {
            var sql = BuildPostgresBatchInsertSql(mapping, cols, batch.Count);

            await using var cmd = ctx.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

            var paramIndex = 0;
            foreach (var entity in batch)
            {
                foreach (var col in cols)
                {
                    var paramName = $"{ParamPrefix}p{paramIndex++}";
                    var value = col.Getter(entity) ?? DBNull.Value;
                    cmd.AddParam(paramName, value);
                }
            }

            return await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Builds a multi-row INSERT INTO ... VALUES statement with parameterized placeholders.
        /// </summary>
        internal string BuildPostgresBatchInsertSql(TableMapping mapping, Column[] cols, int batchSize)
        {
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.Append("INSERT INTO ").Append(mapping.EscTable)
                  .Append(" (").Append(colNames).Append(") VALUES ");

                var paramIndex = 0;
                for (int i = 0; i < batchSize; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append('(');

                    for (int j = 0; j < cols.Length; j++)
                    {
                        if (j > 0) sb.Append(", ");
                        sb.Append(ParamPrefix).Append('p').Append(paramIndex++);
                    }

                    sb.Append(')');
                }

                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Updates a sequence of entities using parameterized <c>UPDATE</c> statements executed in batches.
        /// </summary>
        /// <typeparam name="T">Entity type being updated.</typeparam>
        /// <param name="ctx">Active database context.</param>
        /// <param name="m">Entity-table mapping information.</param>
        /// <param name="entities">Entities with new values to persist.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Count of rows updated.</returns>
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var nonKeyCols = m.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToList();
            var keyCols = m.KeyColumns.ToList();
            var valueCols = keyCols.Concat(nonKeyCols).ToList();
            if (m.TimestampColumn != null) valueCols.Add(m.TimestampColumn);

            var paramsPerEntity = valueCols.Count;
            var operationKey = $"Postgres_BulkUpdate_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);
            var maxBatchForProvider = MaxParameters / Math.Max(1, paramsPerEntity);
            var batchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));

            var totalUpdated = 0;
            var ownedTx = ctx.CurrentTransaction == null;
            DbTransaction? transaction = ownedTx
                ? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false)
                : ctx.CurrentTransaction;
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                    await using var cmd = ctx.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                    var sb = _stringBuilderPool.Get();
                    try
                    {
                        sb.Append("UPDATE ").Append(m.EscTable).Append(" AS t SET ");
                        sb.Append(string.Join(", ", nonKeyCols.Select(c => $"{c.EscCol} = v.{c.EscCol}")));
                        sb.Append(" FROM (VALUES ");

                        var valueRows = new List<string>();
                        var paramIndex = 0;
                        foreach (var entity in batch)
                        {
                            var paramNames = new List<string>();
                            foreach (var col in valueCols)
                            {
                                var pName = $"{ParamPrefix}p{paramIndex++}";
                                cmd.AddParam(pName, col.Getter(entity));
                                paramNames.Add(pName);
                            }
                            valueRows.Add($"({string.Join(", ", paramNames)})");
                        }

                        sb.Append(string.Join(", ", valueRows));
                        sb.Append(") AS v (");
                        sb.Append(string.Join(", ", valueCols.Select(c => c.EscCol)));
                        sb.Append(") WHERE ");

                        // Add tenant predicate to prevent cross-tenant bulk updates
                        var tenantParam = ctx.Options.TenantProvider != null && m.TenantColumn != null
                            ? $"{ParamPrefix}__tenant_bulk"
                            : null;
                        sb.Append(BuildBulkUpdateWhereClause(
                            keyCols.Select(c => c.EscCol),
                            m.TimestampColumn?.EscCol,
                            m.TenantColumn?.EscCol,
                            tenantParam));
                        if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                        {
                            cmd.AddParam(tenantParam!, ctx.Options.TenantProvider.GetCurrentTenantId());
                        }

                        cmd.CommandText = sb.ToString();

                        var batchSw = Stopwatch.StartNew();
                        totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                        batchSw.Stop();
                        BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                    }
                    finally
                    {
                        sb.Clear();
                        _stringBuilderPool.Return(sb);
                    }
                }

                if (ownedTx) await transaction!.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ownedTx)
                {
                    try { await transaction!.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                    catch (Exception rollbackEx) { throw new AggregateException(ex, rollbackEx); }
                }
                throw;
            }
            finally
            {
                if (ownedTx && transaction != null)
                    await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        internal static string BuildBulkUpdateWhereClause(
            IEnumerable<string> keyColumns,
            string? timestampColumn,
            string? tenantColumn,
            string? tenantParameter)
        {
            var conditions = keyColumns.Select(c => $"t.{c} = v.{c}").ToList();
            if (timestampColumn != null)
                conditions.Add($"t.{timestampColumn} = v.{timestampColumn}");
            if (tenantColumn != null && tenantParameter != null)
                conditions.Add($"t.{tenantColumn} = {tenantParameter}");
            return string.Join(" AND ", conditions);
        }

        /// <summary>
        /// Deletes entities by primary key using batched <c>DELETE</c> statements.
        /// </summary>
        /// <typeparam name="T">Type of entity to delete.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/>.</param>
        /// <param name="m">Table mapping containing key metadata.</param>
        /// <param name="entities">Entities whose keys identify rows to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows deleted.</returns>
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            if (m.KeyColumns.Length != 1)
                return await base.BatchedDeleteAsync(ctx, m, entityList, ct).ConfigureAwait(false);

            var keyCol = m.KeyColumns[0];
            var operationKey = $"Postgres_BulkDelete_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);
            var batchSize = Math.Max(1, sizing.OptimalBatchSize);

            var totalDeleted = 0;
            var ownedTx = ctx.CurrentTransaction == null;
            DbTransaction? transaction = ownedTx
                ? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false)
                : ctx.CurrentTransaction;
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                    var keys = CreateTypedArray(batch.Select(e => keyCol.Getter(e)).ToArray());
                    await using var cmd = ctx.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    var pName = $"{ParamPrefix}p0";
                    var p = cmd.CreateParameter();
                    p.ParameterName = pName;
                    p.Value = keys;
                    cmd.Parameters.Add(p);
                    var deleteSql = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} = ANY({pName})";
                    // Add tenant predicate to prevent cross-tenant bulk deletes
                    if (ctx.Options.TenantProvider != null && m.TenantColumn != null)
                    {
                        var tenantParam = $"{ParamPrefix}__tenant_bulk";
                        var tp = cmd.CreateParameter();
                        tp.ParameterName = tenantParam;
                        tp.Value = ctx.Options.TenantProvider.GetCurrentTenantId() ?? (object)DBNull.Value;
                        cmd.Parameters.Add(tp);
                        deleteSql += $" AND {m.TenantColumn.EscCol} = {tenantParam}";
                    }
                    cmd.CommandText = deleteSql;
                    var batchSw = Stopwatch.StartNew();
                    totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    batchSw.Stop();
                    BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                }

                if (ownedTx) await transaction!.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ownedTx)
                {
                    try { await transaction!.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                    catch (Exception rollbackEx) { throw new AggregateException(ex, rollbackEx); }
                }
                throw;
            }
            finally
            {
                if (ownedTx && transaction != null)
                    await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // Invalidate query cache after bulk write to prevent stale reads
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }

        /// <summary>
        /// Maps a CLR type to its PostgreSQL column type equivalent.
        /// Used when generating history tables from CLR metadata without live introspection data.
        /// </summary>
        /// <param name="t">The CLR type to map (unwraps <see cref="Nullable{T}"/> and enum types internally).</param>
        /// <returns>A PostgreSQL type name string (e.g., <c>"INTEGER"</c>, <c>"TEXT"</c>, <c>"UUID"</c>).</returns>
        private static string GetPostgresType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t.IsEnum) t = Enum.GetUnderlyingType(t);
            if (t == typeof(int)) return "INTEGER";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(string)) return "TEXT";
            if (t == typeof(DateTime)) return "TIMESTAMP";
            if (t == typeof(DateOnly)) return "DATE";
            if (t == typeof(TimeOnly)) return "TIME";
            if (t == typeof(DateTimeOffset)) return "TIMESTAMPTZ";
            if (t == typeof(TimeSpan)) return "INTERVAL";
            if (t == typeof(bool)) return "BOOLEAN";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(Guid)) return "UUID";
            if (t == typeof(byte[])) return "BYTEA";
            if (t == typeof(float)) return "REAL";
            if (t == typeof(double)) return "DOUBLE PRECISION";
            if (t == typeof(short)) return "SMALLINT";
            if (t == typeof(byte)) return "SMALLINT";
            if (t == typeof(char)) return "CHAR(1)";
            if (t == typeof(sbyte)) return "SMALLINT";
            if (t == typeof(ushort)) return "INTEGER";
            if (t == typeof(uint)) return "BIGINT";
            if (t == typeof(ulong)) return "NUMERIC(20,0)";
            return "TEXT";
        }
    }
}
