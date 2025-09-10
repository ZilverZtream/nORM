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

#nullable enable

namespace nORM.Providers
{
    public abstract class DatabaseProvider : IFastProvider
    {
        private readonly ConcurrentLruCache<(Type Type, string Operation), string> _sqlCache = new(maxSize: 1000);
        protected static readonly DynamicBatchSizer BatchSizer = new();

        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        public virtual char ParameterPrefixChar => '@';
        public virtual string ParamPrefix => ParameterPrefixChar.ToString();
        public virtual int MaxSqlLength => int.MaxValue;
        public virtual int MaxParameters => int.MaxValue;

        /// <summary>
        /// Escapes an identifier (such as a table or column name) for inclusion in SQL statements.
        /// </summary>
        /// <param name="id">The identifier to escape.</param>
        /// <returns>The escaped identifier.</returns>
        public abstract string Escape(string id);

        /// <summary>
        /// Applies provider-specific paging clauses to the supplied SQL builder.
        /// </summary>
        /// <param name="sb">The builder containing the base SQL statement.</param>
        /// <param name="limit">The maximum number of rows to return.</param>
        /// <param name="offset">The number of rows to skip before starting to return rows.</param>
        /// <param name="limitParameterName">The parameter name used for the limit value.</param>
        /// <param name="offsetParameterName">The parameter name used for the offset value.</param>
        public abstract void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName);

        /// <summary>
        /// Returns SQL that retrieves the identity value generated for an inserted row.
        /// </summary>
        /// <param name="m">The mapping for the table being inserted into.</param>
        /// <returns>A SQL fragment that retrieves the generated identity.</returns>
        public abstract string GetIdentityRetrievalString(TableMapping m);

        /// <summary>
        /// Creates a database parameter with the given name and value.
        /// </summary>
        /// <param name="name">The parameter name, including prefix.</param>
        /// <param name="value">The parameter value.</param>
        /// <returns>A parameter configured for the underlying provider.</returns>
        public abstract DbParameter CreateParameter(string name, object? value);

        /// <summary>
        /// Translates a .NET method invocation into its SQL equivalent for the provider.
        /// </summary>
        /// <param name="name">The name of the method being translated.</param>
        /// <param name="declaringType">The type that declares the method.</param>
        /// <param name="args">The SQL arguments to the function.</param>
        /// <returns>The translated SQL expression, or <c>null</c> if the method is not supported.</returns>
        public abstract string? TranslateFunction(string name, Type declaringType, params string[] args);

        /// <summary>
        /// Translates a JSON path access expression for the provider.
        /// </summary>
        /// <param name="columnName">The name of the JSON column.</param>
        /// <param name="jsonPath">The JSON path to access within the column.</param>
        /// <returns>The SQL fragment that accesses the specified JSON path.</returns>
        public abstract string TranslateJsonPathAccess(string columnName, string jsonPath);

        /// <summary>
        /// Generates the SQL required to create a history table for temporal table support.
        /// </summary>
        /// <param name="mapping">The table mapping representing the entity.</param>
        /// <returns>The SQL statement that creates the history table.</returns>
        public abstract string GenerateCreateHistoryTableSql(TableMapping mapping);

        /// <summary>
        /// Generates the SQL required to create triggers for maintaining the temporal history table.
        /// </summary>
        /// <param name="mapping">The table mapping representing the entity.</param>
        /// <returns>The SQL script containing the trigger definitions.</returns>
        public abstract string GenerateTemporalTriggersSql(TableMapping mapping);

        /// <summary>
        /// Character used to escape wildcards in patterns passed to SQL <c>LIKE</c> clauses.
        /// Defaults to a backslash but can be overridden by providers with different
        /// escaping semantics.
        /// </summary>
        public virtual char LikeEscapeChar => '\\';

        /// <summary>
        /// Escapes occurrences of wildcard characters (<c>%</c> and <c>_</c>) in a
        /// pattern so that they are treated as literals in <c>LIKE</c> expressions.
        /// </summary>
        /// <param name="value">The raw pattern value supplied by the user.</param>
        /// <returns>The escaped pattern safe for inclusion in a <c>LIKE</c> clause.</returns>
        public virtual string EscapeLikePattern(string value)
        {
            var esc = NormValidator.ValidateLikeEscapeChar(LikeEscapeChar).ToString();
            return value
                .Replace(esc, esc + esc)
                .Replace("%", esc + "%")
                .Replace("_", esc + "_");
        }

        protected virtual void ValidateConnection(DbConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                var safeConnStr = NormValidator.MaskSensitiveConnectionStringData(connection.ConnectionString);
                throw new InvalidOperationException($"Connection must be open for {GetType().Name}. Connection: {safeConnStr}");
            }
        }

        protected void EnsureValidParameterName(string? parameterName, string argumentName)
        {
            if (parameterName != null && !parameterName.StartsWith(ParamPrefix, StringComparison.Ordinal))
                throw new ArgumentException($"Parameter name must start with '{ParamPrefix}'", argumentName);
        }

        /// <summary>
        /// Determines whether the provider can operate in the current environment. The
        /// base implementation simply returns <c>true</c> but derived providers may
        /// perform runtime checks for required assemblies or database availability.
        /// </summary>
        public virtual Task<bool> IsAvailableAsync()
        {
            return Task.FromResult(true);
        }

        /// <summary>
        /// Creates a database savepoint within the given transaction. The default
        /// implementation throws as savepoints are provider specific and may not be
        /// supported.
        /// </summary>
        /// <param name="transaction">The transaction in which to create the savepoint.</param>
        /// <param name="name">Name of the savepoint.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            throw new NotSupportedException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Rolls the specified transaction back to a previously created savepoint. The
        /// default implementation throws as savepoints are provider specific.
        /// </summary>
        /// <param name="transaction">The active transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            throw new NotSupportedException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        /// <summary>
        /// Performs provider-specific initialization when a connection is opened.
        /// </summary>
        /// <param name="connection">The open connection to initialize.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public virtual Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;

        /// <summary>
        /// Synchronous counterpart to <see cref="InitializeConnectionAsync"/> allowing
        /// providers to perform connection initialization without asynchronous overhead.
        /// </summary>
        /// <param name="connection">The open connection to initialize.</param>
        public virtual void InitializeConnection(DbConnection connection) { }

        public virtual CommandType StoredProcedureCommandType => CommandType.StoredProcedure;

        /// <summary>
        /// Constructs a minimal <c>SELECT</c> statement directly into the provided
        /// character buffer to avoid intermediate string allocations.
        /// </summary>
        /// <param name="buffer">Destination buffer that receives the generated SQL.</param>
        /// <param name="table">Name of the table to select from.</param>
        /// <param name="columns">Comma separated list of columns to select.</param>
        /// <param name="length">Outputs the number of characters written to the buffer.</param>
        public virtual void BuildSimpleSelect(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns, out int length)
        {
            length = BuildSimpleSelectSlow(buffer, table, columns);
        }

        /// <summary>
        /// Fallback implementation of <see cref="BuildSimpleSelect"/> that uses string
        /// concatenation. Providers can override <see cref="BuildSimpleSelect"/> to
        /// supply more efficient implementations.
        /// </summary>
        protected virtual int BuildSimpleSelectSlow(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns)
        {
            var sql = string.Concat("SELECT ", columns.ToString(), " FROM ", table.ToString());
            sql.AsSpan().CopyTo(buffer);
            return sql.Length;
        }

        /// <summary>
        /// Builds an SQL <c>IN</c> clause for the specified column and parameter values.
        /// Each value is added as a parameter to the provided command to guard against
        /// SQL injection.
        /// </summary>
        /// <param name="cmd">The command that will execute the generated SQL.</param>
        /// <param name="columnName">Column to apply the <c>IN</c> filter to.</param>
        /// <param name="values">Values to include in the <c>IN</c> list.</param>
        /// <returns>SQL fragment representing the <c>IN</c> clause.</returns>
        public virtual string BuildContainsClause(DbCommand cmd, string columnName, IReadOnlyList<object?> values)
        {
            var paramNames = new List<string>(values.Count);
            for (int i = 0; i < values.Count; i++)
            {
                var pn = $"{ParamPrefix}p{i}";
                cmd.AddParam(pn, values[i]);
                paramNames.Add(pn);
            }
            return $"{columnName} IN ({string.Join(",", paramNames)})";
        }

        protected virtual Task<bool> IsTransactionLogNearCapacityAsync(DbContext ctx, CancellationToken ct)
            => Task.FromResult(false);

        #region Bulk Operations (Abstract & Fallback)
        public virtual async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any())
            {
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, 0, sw.Elapsed);
                return 0;
            }

            var operationKey = $"BulkInsert_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);
            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var maxBatchForProvider = MaxParameters == int.MaxValue
                ? 1000
                : Math.Max(1, Math.Min(1000, (MaxParameters - 10) / Math.Max(1, cols.Count)));
            var effectiveBatchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));
            // Logging infrastructure doesn't support arbitrary info; batch size can be inferred from performance metrics.

            var recordsAffected = 0;
            var index = 0;
            while (index < entityList.Count)
            {
                var availableMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
                if (availableMemory < sizing.EstimatedMemoryUsage * 2)
                    effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);

                if (await IsTransactionLogNearCapacityAsync(ctx, ct).ConfigureAwait(false))
                    effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);

                var batch = entityList.Skip(index).Take(effectiveBatchSize).ToList();
                var batchSw = Stopwatch.StartNew();
                recordsAffected += await ExecuteInsertBatch(ctx, m, batch, ct).ConfigureAwait(false);
                batchSw.Stop();
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                index += batch.Count;
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }

        protected async Task<int> ExecuteInsertBatch<T>(DbContext ctx, TableMapping m, List<T> batch, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sb = _stringBuilderPool.Get();
            try
            {
                var cols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                sb.Append($"INSERT INTO {m.EscTable} ({colNames}) VALUES ");

                await using var cmd = ctx.Connection.CreateCommand();
                var pIndex = 0;
                for (int i = 0; i < batch.Count; i++)
                {
                    sb.Append(i > 0 ? ",(" : "(");
                    for (int j = 0; j < cols.Count; j++)
                    {
                        var pName = $"{ParamPrefix}p{pIndex++}";
                        cmd.AddParam(pName, cols[j].Getter(batch[i]));
                        sb.Append(j > 0 ? $",{pName}" : pName);
                    }
                    sb.Append(")");
                }
                cmd.CommandText = sb.ToString();
                return await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        public virtual Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedUpdateAsync(ctx, m, e, ct);
            throw new NotImplementedException("This provider does not have a native bulk update implementation.");
        }

        public virtual Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedDeleteAsync(ctx, m, e, ct);
            throw new NotImplementedException("This provider does not have a native bulk delete implementation.");
        }

        protected async Task<int> BatchedUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var totalUpdated = 0;
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                foreach (var entity in entities)
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = BuildUpdate(m);
                    foreach (var col in m.Columns.Where(c => !c.IsTimestamp)) cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    if (m.TimestampColumn != null) cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                    totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }
            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        protected async Task<int> BatchedDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            if (!m.KeyColumns.Any())
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var totalDeleted = 0;
            var keyColumns = m.KeyColumns.ToList();

            // Determine maximum entities per batch based on provider parameter limit
            var batchSize = Math.Min(ctx.Options.BulkBatchSize, 1000);
            if (MaxParameters != int.MaxValue)
            {
                var paramsPerEntity = Math.Max(1, keyColumns.Count);
                var maxBatchByParams = Math.Max(1, (MaxParameters - 10) / paramsPerEntity);
                batchSize = Math.Min(batchSize, maxBatchByParams);
            }

            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.Skip(i).Take(batchSize).ToList();
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                    var paramNames = new List<string>();
                    var paramIndex = 0;

                    string whereClause;

                    if (keyColumns.Count == 1)
                    {
                        var keyCol = keyColumns[0];
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var pName = $"{ParamPrefix}p{paramIndex++}";
                            paramNames.Add(pName);
                            cmd.AddParam(pName, keyCol.Getter(batch[j]));
                        }
                        whereClause = $"{keyCol.EscCol} IN ({string.Join(",", paramNames)})";
                    }
                    else
                    {
                        var orConditions = new List<string>();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var keyValues = keyColumns.Select(c =>
                            {
                                var pName = $"{ParamPrefix}p{paramIndex++}";
                                cmd.AddParam(pName, c.Getter(batch[j]));
                                return $"{c.EscCol} = {pName}";
                            });
                            orConditions.Add($"({string.Join(" AND ", keyValues)})");
                        }
                        whereClause = string.Join(" OR ", orConditions);
                    }

                    cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {whereClause}";
                    var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    totalDeleted += deleted;
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
        #endregion

        #region SQL Generation
        /// <summary>
        /// Builds a parameterized <c>INSERT</c> statement for the specified table
        /// mapping, caching the generated SQL for future use.
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <returns>An <c>INSERT</c> statement ready for parameter substitution.</returns>
        public string BuildInsert(TableMapping m)
        {
            return _sqlCache.GetOrAdd((m.Type, "INSERT"), _ => {
                var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
                if (cols.Length == 0)
                {
                    return $"INSERT INTO {m.EscTable} DEFAULT VALUES{GetIdentityRetrievalString(m)}";
                }
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                var valParams = string.Join(", ", cols.Select(c => ParamPrefix + c.PropName));
                return $"INSERT INTO {m.EscTable} ({colNames}) VALUES ({valParams}){GetIdentityRetrievalString(m)}";
            });
        }

        /// <summary>
        /// Builds a parameterized <c>UPDATE</c> statement that updates all non-key
        /// columns and filters by the entity's primary key (and timestamp when present).
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <returns>An <c>UPDATE</c> SQL statement.</returns>
        public string BuildUpdate(TableMapping m)
        {
            return _sqlCache.GetOrAdd((m.Type, "UPDATE"), _ =>
            {
                var set = string.Join(", ", m.Columns
                    .Where(c => !c.IsKey && !c.IsTimestamp)
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}"));

                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                if (m.TimestampColumn != null)
                    whereCols.Add($"{m.TimestampColumn.EscCol}={ParamPrefix}{m.TimestampColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"UPDATE {m.EscTable} SET {set} WHERE {where}";
            });
        }

        /// <summary>
        /// Builds a parameterized <c>DELETE</c> statement that filters by the primary
        /// key (and timestamp when applicable) to ensure a single row is targeted.
        /// </summary>
        /// <param name="m">The table mapping describing the entity.</param>
        /// <returns>A <c>DELETE</c> SQL statement.</returns>
        public string BuildDelete(TableMapping m)
        {
            return _sqlCache.GetOrAdd((m.Type, "DELETE"), _ =>
            {
                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                if (m.TimestampColumn != null)
                    whereCols.Add($"{m.TimestampColumn.EscCol}={ParamPrefix}{m.TimestampColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"DELETE FROM {m.EscTable} WHERE {where}";
            });
        }
        #endregion
    }
}