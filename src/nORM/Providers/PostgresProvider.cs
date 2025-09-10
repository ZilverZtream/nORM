using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Database provider implementation optimized for PostgreSQL.
    /// Handles SQL dialect translation, bulk operations and temporal table support.
    /// </summary>
    public sealed class PostgresProvider : BulkOperationProvider
    {
        private readonly IDbParameterFactory _parameterFactory;
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        public PostgresProvider(IDbParameterFactory parameterFactory)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
        }

        public override int MaxSqlLength => int.MaxValue;
        public override int MaxParameters => 32_767;

        /// <inheritdoc />
        public override string Escape(string id) => $"\"{id}\"";

        /// <summary>
        /// Adds PostgreSQL-specific <c>LIMIT</c> and <c>OFFSET</c> clauses to the SQL builder.
        /// </summary>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            if (limitParameterName != null)
                sb.Append(" LIMIT ").Append(limitParameterName);

            if (offsetParameterName != null)
                sb.Append(" OFFSET ").Append(offsetParameterName);
        }
        
        /// <summary>
        /// Generates SQL for retrieving the value of an identity column after an insert.
        /// </summary>
        public override string GetIdentityRetrievalString(TableMapping m)
        {
            var keyCol = m.KeyColumns.FirstOrDefault(c => c.IsDbGenerated);
            return keyCol != null ? $" RETURNING {keyCol.EscCol}" : string.Empty;
        }

        /// <summary>
        /// Creates a <see cref="DbParameter"/> instance for use with Npgsql commands.
        /// </summary>
        public override DbParameter CreateParameter(string name, object? value) =>
            _parameterFactory.CreateParameter(name, value);

        /// <summary>
        /// Attempts to translate a .NET method invocation into its PostgreSQL equivalent.
        /// </summary>
        /// <param name="name">Name of the .NET method being translated.</param>
        /// <param name="declaringType">Type that declares the method.</param>
        /// <param name="args">SQL fragments representing the method arguments.</param>
        /// <returns>The translated SQL expression or <c>null</c> if the method is not supported.</returns>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"LENGTH({args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"EXTRACT(YEAR FROM {args[0]})",
                    nameof(DateTime.Month) => $"EXTRACT(MONTH FROM {args[0]})",
                    nameof(DateTime.Day) => $"EXTRACT(DAY FROM {args[0]})",
                    nameof(DateTime.Hour) => $"EXTRACT(HOUR FROM {args[0]})",
                    nameof(DateTime.Minute) => $"EXTRACT(MINUTE FROM {args[0]})",
                    nameof(DateTime.Second) => $"EXTRACT(SECOND FROM {args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEILING({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    _ => null
                };
            }

            return null;
        }

        /// <summary>
        /// Produces a SQL fragment that accesses a JSON value using PostgreSQL's <c>jsonb_extract_path_text</c>.
        /// </summary>
        /// <param name="columnName">The JSON column being accessed.</param>
        /// <param name="jsonPath">The JSON path expression (dot-delimited).</param>
        /// <returns>SQL fragment that retrieves the JSON value as text.</returns>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            var pgPath = string.Join(",", jsonPath.Split('.').Skip(1).Select(p => $"'{p}'"));
            return $"jsonb_extract_path_text({columnName}, {pgPath})";
        }

        /// <summary>
        /// Builds an optimized <c>ANY</c> expression for arrays to implement a <c>Contains</c> filter.
        /// </summary>
        /// <param name="cmd">Command to which parameters are added.</param>
        /// <param name="columnName">Name of the column being filtered.</param>
        /// <param name="values">Values to check for containment.</param>
        /// <returns>SQL fragment implementing the containment check.</returns>
        public override string BuildContainsClause(DbCommand cmd, string columnName, IReadOnlyList<object?> values)
        {
            var pName = ParamPrefix + "p0";
            var p = cmd.CreateParameter();
            p.ParameterName = pName;
            p.Value = values.ToArray();
            cmd.Parameters.Add(p);
            return $"{columnName} = ANY({pName})";
        }

        /// <summary>
        /// Generates the SQL definition for the temporal history table corresponding to the entity mapping.
        /// </summary>
        /// <param name="mapping">The entity mapping to create history storage for.</param>
        /// <returns>DDL statement that creates the history table.</returns>
        public override string GenerateCreateHistoryTableSql(TableMapping mapping)
        {
            var historyTable = Escape(mapping.TableName + "_History");
            var columns = string.Join(",\n    ", mapping.Columns.Select(c => $"{Escape(c.PropName)} {GetPostgresType(c.Prop.PropertyType)}"));

            return $@"
CREATE TABLE {historyTable} (
    ""__VersionId"" BIGSERIAL PRIMARY KEY,
    ""__ValidFrom"" TIMESTAMP NOT NULL,
    ""__ValidTo"" TIMESTAMP NOT NULL,
    ""__Operation"" CHAR(1) NOT NULL,
    {columns}
);";
        }

        /// <summary>
        /// Produces the trigger definitions required to track changes in the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the target table.</param>
        /// <returns>DDL statements that create the temporal triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.PropName)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.PropName)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.PropName)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.PropName)} = OLD.{Escape(c.PropName)}"));
            var keyConditionH2 = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h2.{Escape(c.PropName)} = OLD.{Escape(c.PropName)}"));
            var functionName = Escape(mapping.TableName + "_TemporalFunction");

            return $@"
CREATE OR REPLACE FUNCTION {functionName}() RETURNS TRIGGER AS $$
DECLARE v_now TIMESTAMP := (now() at time zone 'utc');
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'I', {newColumns});
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition}
          AND NOT EXISTS (
             SELECT 1 FROM {history} h2 WHERE h2.""__ValidFrom"" = v_now AND {keyConditionH2}
          );
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'U', {newColumns});
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition}
          AND NOT EXISTS (
             SELECT 1 FROM {history} h2 WHERE h2.""__ValidFrom"" = v_now AND {keyConditionH2}
          );
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, v_now, 'D', {oldColumns});
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalTrigger")} ON {table};
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalTrigger")}
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW EXECUTE FUNCTION {functionName}();";
        }

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection.GetType().FullName != "Npgsql.NpgsqlConnection")
                throw new InvalidOperationException("A NpgsqlConnection is required for PostgresProvider.");
        }

        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Host=localhost;Database=postgres;Username=postgres;Password=;Timeout=1";
            try
            {
                await cn.OpenAsync();
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "SHOW server_version";
                var result = await cmd.ExecuteScalarAsync();
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr.Split(' ')[0]);
                return version >= new Version(9, 5);
            }
            catch
            {
                return false;
            }
        }

        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("CreateSavepoint", new[] { typeof(string) });
            if (saveMethod != null)
            {
                saveMethod.Invoke(transaction, new object[] { name });
                return Task.CompletedTask;
            }
            throw new NotSupportedException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            var rollbackMethod = transaction.GetType().GetMethod("Rollback", new[] { typeof(string) }) ??
                                 transaction.GetType().GetMethod("RollbackToSavepoint", new[] { typeof(string) });
            if (rollbackMethod != null)
            {
                rollbackMethod.Invoke(transaction, new object[] { name });
                return Task.CompletedTask;
            }
            throw new NotSupportedException($"Savepoints are not supported for transactions of type {transaction.GetType().FullName}.");
        }

        // PostgreSQL-optimized bulk operations
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
            if (!cols.Any()) return 0;

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
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, entityList.Count, sw.Elapsed);
                return entityList.Count;
            }

            var recordsAffected = await ExecuteBulkOperationAsync(ctx, m, entityList, operationKey,
                (batch, tx, token) => ExecutePostgresBatchInsert(ctx, tx, m, cols, batch, token), ct).ConfigureAwait(false);

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }

        private async Task<int> ExecutePostgresBatchInsert<T>(DbContext ctx, System.Data.Common.DbTransaction transaction,
            TableMapping mapping, Column[] cols, List<T> batch, CancellationToken ct) where T : class
        {
            // Use PostgreSQL's multi-row VALUES syntax with RETURNING for identity columns
            var sql = BuildPostgresBatchInsertSql(mapping, cols, batch.Count);
            
            await using var cmd = ctx.Connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.CommandTimeout = 30;
            
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

        private string BuildPostgresBatchInsertSql(TableMapping mapping, Column[] cols, int batchSize)
        {
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.Append($"INSERT INTO {mapping.EscTable} ({colNames}) VALUES ");

                var paramIndex = 0;
                for (int i = 0; i < batchSize; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append("(");

                    for (int j = 0; j < cols.Length; j++)
                    {
                        if (j > 0) sb.Append(", ");
                        sb.Append($"{ParamPrefix}p{paramIndex++}");
                    }

                    sb.Append(")");
                }

                // Add ON CONFLICT clause for upsert scenarios if needed
                sb.Append(" ON CONFLICT DO NOTHING");

                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var nonKeyCols = m.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToList();
            var keyCols = m.KeyColumns.ToList();
            var valueCols = keyCols.Concat(nonKeyCols).ToList();
            if (m.TimestampColumn != null) valueCols.Add(m.TimestampColumn);

            var paramsPerEntity = valueCols.Count;
            var operationKey = $"Postgres_BulkUpdate_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);
            var maxBatchForProvider = MaxParameters / Math.Max(1, paramsPerEntity);
            var batchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));

            var totalUpdated = 0;
            for (int i = 0; i < entityList.Count; i += batchSize)
            {
                var batch = entityList.Skip(i).Take(batchSize).ToList();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                var sb = _stringBuilderPool.Get();
                try
                {
                    sb.Append($"UPDATE {m.EscTable} AS t SET ");
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

                    var joinConditions = keyCols.Select(c => $"t.{c.EscCol} = v.{c.EscCol}").ToList();
                    if (m.TimestampColumn != null)
                        joinConditions.Add($"t.{m.TimestampColumn.EscCol} = v.{m.TimestampColumn.EscCol}");
                    sb.Append(string.Join(" AND ", joinConditions));

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

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            if (m.KeyColumns.Length != 1)
                return await base.BatchedDeleteAsync(ctx, m, entityList, ct).ConfigureAwait(false);

            var keyCol = m.KeyColumns[0];
            var operationKey = $"Postgres_BulkDelete_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);
            var batchSize = Math.Max(1, sizing.OptimalBatchSize);

            var totalDeleted = 0;
            for (int i = 0; i < entityList.Count; i += batchSize)
            {
                var batch = entityList.Skip(i).Take(batchSize).ToList();
                var keys = batch.Select(e => keyCol.Getter(e)).ToArray();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                var pName = $"{ParamPrefix}p0";
                var p = cmd.CreateParameter();
                p.ParameterName = pName;
                p.Value = keys;
                cmd.Parameters.Add(p);
                cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} = ANY({pName})";
                var batchSw = Stopwatch.StartNew();
                totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                batchSw.Stop();
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }

        private static string GetPostgresType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int)) return "INTEGER";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(string)) return "TEXT";
            if (t == typeof(DateTime)) return "TIMESTAMP";
            if (t == typeof(bool)) return "BOOLEAN";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(Guid)) return "UUID";
            if (t == typeof(byte[])) return "BYTEA";
            if (t == typeof(float)) return "REAL";
            if (t == typeof(double)) return "DOUBLE PRECISION";
            if (t == typeof(short)) return "SMALLINT";
            if (t == typeof(byte)) return "SMALLINT";
            return "TEXT";
        }
    }
}