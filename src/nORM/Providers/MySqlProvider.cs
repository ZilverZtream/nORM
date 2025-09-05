using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    public sealed class MySqlProvider : DatabaseProvider
    {
        private static readonly ConcurrentLruCache<Type, DataTable> _tableSchemas = new(maxSize: 100);
        private readonly IDbParameterFactory _parameterFactory;

        public MySqlProvider(IDbParameterFactory parameterFactory)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
        }
        public override int MaxSqlLength => 4_194_304;
        public override int MaxParameters => 65_535;
        public override string Escape(string id) => $"`{id}`";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            // MySQL uses a single LIMIT clause for both limit and offset in the form
            // "LIMIT offset, limit". The previous implementation only emitted the clause
            // when a limit was present which meant that queries using only Skip() would
            // ignore the offset. MySQL requires a limit value when an offset is specified,
            // so when only an offset is provided we use the maximum unsigned BIGINT value
            // to effectively indicate "no limit".
            if (limitParameterName != null || offsetParameterName != null)
            {
                sb.Append(" LIMIT ");
                if (offsetParameterName != null)
                {
                    sb.Append(offsetParameterName).Append(", ");
                }

                sb.Append(limitParameterName ?? "18446744073709551615");
            }
        }
        
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT LAST_INSERT_ID();";
        
        public override DbParameter CreateParameter(string name, object? value) =>
            _parameterFactory.CreateParameter(name, value);

        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            if (declaringType == typeof(string))
            {
                return name switch
                {
                    nameof(string.ToUpper) => $"UPPER({args[0]})",
                    nameof(string.ToLower) => $"LOWER({args[0]})",
                    nameof(string.Length) when args.Length == 1 => $"CHAR_LENGTH({args[0]})",
                    _ => null
                };
            }

            if (declaringType == typeof(DateTime))
            {
                return name switch
                {
                    nameof(DateTime.Year) => $"YEAR({args[0]})",
                    nameof(DateTime.Month) => $"MONTH({args[0]})",
                    nameof(DateTime.Day) => $"DAY({args[0]})",
                    nameof(DateTime.Hour) => $"HOUR({args[0]})",
                    nameof(DateTime.Minute) => $"MINUTE({args[0]})",
                    nameof(DateTime.Second) => $"SECOND({args[0]})",
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

        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
            => $"JSON_UNQUOTE(JSON_EXTRACT({columnName}, '{jsonPath}'))";

        public override string GenerateCreateHistoryTableSql(TableMapping mapping)
        {
            var historyTable = Escape(mapping.TableName + "_History");
            var columns = string.Join(",\n    ", mapping.Columns.Select(c => $"{Escape(c.PropName)} {GetSqlType(c.Prop.PropertyType)}"));

            return $@"
CREATE TABLE {historyTable} (
    `__VersionId` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `__ValidFrom` DATETIME NOT NULL,
    `__ValidTo` DATETIME NOT NULL,
    `__Operation` CHAR(1) NOT NULL,
    {columns}
) ENGINE=InnoDB;";
        }

        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.PropName)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.PropName)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.PropName)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.PropName)} = OLD.{Escape(c.PropName)}"));

            return $@"
CREATE TRIGGER {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
FOR EACH ROW
BEGIN
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(), '9999-12-31', 'I', {newColumns});
END;
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP() WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(), '9999-12-31', 'U', {newColumns});
END;
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP() WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'D', {oldColumns});
END;";
        }

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            var name = connection.GetType().FullName;
            if (name != "MySqlConnector.MySqlConnection" && name != "MySql.Data.MySqlClient.MySqlConnection")
                throw new InvalidOperationException("A MySqlConnection is required for MySqlProvider. Please install MySqlConnector or MySql.Data.");
        }

        public override Task<bool> IsAvailableAsync()
        {
            return Task.FromResult(
                Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector") != null ||
                Type.GetType("MySql.Data.MySqlClient.MySqlConnection, MySql.Data") != null);
        }

        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            var saveMethod = transaction.GetType().GetMethod("Save", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("CreateSavepoint", new[] { typeof(string) }) ??
                             transaction.GetType().GetMethod("Savepoint", new[] { typeof(string) });
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

        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var operationKey = $"MySql_BulkInsert_{m.Type.Name}";
            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);

            var bulkCopyType = Type.GetType("MySqlConnector.MySqlBulkCopy, MySqlConnector");
            if (bulkCopyType != null && ctx.Connection.GetType().FullName == "MySqlConnector.MySqlConnection")
            {
                await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
                try
                {
                    dynamic bulkCopy = Activator.CreateInstance(bulkCopyType, ctx.Connection)!;
                    bulkCopy.DestinationTableName = m.EscTable.Trim('`');
                    bulkCopy.BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    bulkCopy.Transaction = transaction;

                    var totalInserted = 0;
                    for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
                    {
                        var batch = entityList.Skip(i).Take(sizing.OptimalBatchSize).ToList();
                        using var table = GetDataTable(m, insertableCols);
                        foreach (var entity in batch)
                            table.Rows.Add(insertableCols.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());

                        var batchSw = Stopwatch.StartNew();
                        await bulkCopy.WriteToServerAsync(table, ct).ConfigureAwait(false);
                        batchSw.Stop();
                        totalInserted += table.Rows.Count;
                        BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                    }

                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                    ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
                    return totalInserted;
                }
                catch (Exception ex)
                {
                    try
                    {
                        await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(ex, rollbackEx);
                    }
                    throw;
                }
            }

            var affected = await base.BulkInsertAsync(ctx, m, entityList, ct).ConfigureAwait(false);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, affected, sw.Elapsed);
            return affected;
        }

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"`BulkUpdate_{Guid.NewGuid():N}`";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();

            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TEMPORARY TABLE {tempTableName} ({colDefs})";
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }

            var tempMapping = new TableMapping(m.Type, this, ctx, null) { EscTable = tempTableName };
            await BulkInsertAsync(ctx, tempMapping, entities, ct).ConfigureAwait(false);

            var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"UPDATE {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause} SET {setClause}";
                var updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
                return updatedCount;
            }
        }
        
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var keyCols = m.KeyColumns.ToList();
            if (!keyCols.Any())
                throw new Exception($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var operationKey = $"MySql_BulkDelete_{m.Type.Name}";
            var paramsPerEntity = keyCols.Count;
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);
            var maxBatchForProvider = MaxParameters / Math.Max(1, paramsPerEntity);
            var batchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));

            var totalDeleted = 0;
            for (int i = 0; i < entityList.Count; i += batchSize)
            {
                var batch = entityList.Skip(i).Take(batchSize).ToList();
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                var valueClauses = new List<string>();
                var paramIndex = 0;
                foreach (var entity in batch)
                {
                    var paramNames = new List<string>();
                    foreach (var col in keyCols)
                    {
                        var pName = $"{ParamPrefix}p{paramIndex++}";
                        cmd.AddParam(pName, col.Getter(entity));
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

                cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {whereClause}";
                var batchSw = Stopwatch.StartNew();
                totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                batchSw.Stop();
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }

        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(string)) return "TEXT";
            if (t == typeof(DateTime)) return "DATETIME";
            if (t == typeof(bool)) return "BIT";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(Guid)) return "CHAR(36)";
            if (t == typeof(byte[])) return "BLOB";
            return "TEXT";
        }

        private static DataTable GetDataTable(TableMapping m, List<Column> cols)
        {
            var schema = _tableSchemas.GetOrAdd(m.Type, _ =>
            {
                var dt = new DataTable();
                foreach (var c in cols)
                {
                    var propType = c.Prop.PropertyType;
                    dt.Columns.Add(c.PropName, Nullable.GetUnderlyingType(propType) ?? propType);
                }
                return dt;
            });
            return schema.Clone();
        }
    }
}