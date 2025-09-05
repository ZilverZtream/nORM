using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

#nullable enable

namespace nORM.Providers
{
    public sealed class SqliteProvider : DatabaseProvider
    {
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        public override int MaxSqlLength => 1_000_000;
        public override int MaxParameters => 999;
        public override string Escape(string id) => $"\"{id}\"";

        public override CommandType StoredProcedureCommandType => CommandType.Text;

        public override async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        {
            ValidateConnection(connection);
            await using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            await pragmaCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        public override void InitializeConnection(DbConnection connection)
        {
            ValidateConnection(connection);
            using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            pragmaCmd.ExecuteNonQuery();
        }
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            if (limitParameterName != null)
                sb.Append(" LIMIT ").Append(limitParameterName);

            if (offsetParameterName != null)
                sb.Append(" OFFSET ").Append(offsetParameterName);
        }
        
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT last_insert_rowid();";
        
        public override DbParameter CreateParameter(string name, object? value)
        {
            return new SqliteParameter(name, value ?? DBNull.Value);
        }

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
                    nameof(DateTime.Year) => $"CAST(strftime('%Y', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Month) => $"CAST(strftime('%m', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Day) => $"CAST(strftime('%d', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Hour) => $"CAST(strftime('%H', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Minute) => $"CAST(strftime('%M', {args[0]}) AS INTEGER)",
                    nameof(DateTime.Second) => $"CAST(strftime('%S', {args[0]}) AS INTEGER)",
                    _ => null
                };
            }

            if (declaringType == typeof(Math))
            {
                return name switch
                {
                    nameof(Math.Abs) => $"ABS({args[0]})",
                    nameof(Math.Ceiling) => $"CEIL({args[0]})",
                    nameof(Math.Floor) => $"FLOOR({args[0]})",
                    nameof(Math.Round) when args.Length > 1 => $"ROUND({args[0]}, {args[1]})",
                    nameof(Math.Round) => $"ROUND({args[0]})",
                    _ => null
                };
            }

            return null;
        }

        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
            => $"json_extract({columnName}, '{jsonPath}')";

        public override string GenerateCreateHistoryTableSql(TableMapping mapping)
        {
            var columns = string.Join(", ", mapping.Columns.Select(c => $"{Escape(c.PropName)} TEXT"));
            return @$"CREATE TABLE IF NOT EXISTS {Escape(mapping.TableName + "_History")} (
                __VersionId INTEGER PRIMARY KEY AUTOINCREMENT,
                __ValidFrom TEXT NOT NULL,
                __ValidTo TEXT NOT NULL,
                __Operation TEXT NOT NULL,
                {columns}
            );";
        }

        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columnList = string.Join(", ", mapping.Columns.Select(c => Escape(c.PropName)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.PropName)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.PropName)));
            var keyCondition = mapping.KeyColumns.Length > 0
                ? string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.PropName)} = OLD.{Escape(c.PropName)}"))
                : "1=1";

            return @$"
CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
BEGIN
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), '9999-12-31', 'I', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = datetime('now') WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), '9999-12-31', 'U', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = datetime('now') WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES (datetime('now'), datetime('now'), 'D', {oldColumns});
END;";
        }

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for SqliteProvider.");
        }

        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Data Source=:memory:";
            try
            {
                await cn.OpenAsync();
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "select sqlite_version()";
                var result = await cmd.ExecuteScalarAsync();
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr);
                return version >= new Version(3, 9);
            }
            catch
            {
                return false;
            }
        }

        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Save(name);
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Rollback(name);
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        // Optimized bulk insert for SQLite using batched multi-row commands
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
            if (cols.Length == 0) return 0;

            var operationKey = $"Sqlite_BulkInsert_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);
            var maxByParams = Math.Max(1, MaxParameters / cols.Length);
            var batchSize = Math.Min(sizing.OptimalBatchSize, maxByParams);

            var totalInserted = 0;

            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.Skip(i).Take(batchSize).ToList();

                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = 30;
                    cmd.CommandText = BuildBatchInsertSql(m, cols, batch.Count);

                    var paramIndex = 0;
                    foreach (var entity in batch)
                    {
                        foreach (var col in cols)
                        {
                            var value = col.Getter(entity) ?? DBNull.Value;
                            cmd.AddParam($"{ParamPrefix}p{paramIndex++}", value);
                        }
                    }

                    var batchSw = Stopwatch.StartNew();
                    totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    batchSw.Stop();
                    BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
            return totalInserted;
        }

        private string BuildBatchInsertSql(TableMapping m, Column[] cols, int batchSize)
        {
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.Append($"INSERT INTO {m.EscTable} ({colNames}) VALUES ");

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

                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }
        
        // Optimized bulk update for SQLite
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;
            
            var totalUpdated = 0;
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                await using var cmd = ctx.Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = BuildUpdate(m);
                cmd.CommandTimeout = 30;
                
                await cmd.PrepareAsync(ct).ConfigureAwait(false);
                
                foreach (var entity in entityList)
                {
                    cmd.Parameters.Clear();
                    
                    foreach (var col in m.Columns.Where(c => !c.IsKey && !c.IsTimestamp))
                    {
                        cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    }
                    
                    foreach (var col in m.KeyColumns)
                    {
                        cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    }
                    
                    if (m.TimestampColumn != null)
                    {
                        cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                    }
                    
                    totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }
            
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }
        
        // Optimized bulk delete for SQLite using IN clauses where possible
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;
            
            if (!m.KeyColumns.Any())
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");
            
            var totalDeleted = 0;
            // Respect provider parameter limits when batching deletes
            var batchSize = ctx.Options.BulkBatchSize;
            if (MaxParameters != int.MaxValue)
                batchSize = Math.Min(batchSize, MaxParameters);
            if (batchSize <= 0) batchSize = 1;
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                if (m.KeyColumns.Length == 1)
                {
                    var keyCol = m.KeyColumns[0];
                    
                    for (int i = 0; i < entityList.Count; i += batchSize)
                    {
                        var batch = entityList.Skip(i).Take(batchSize).ToList();
                        await using var cmd = ctx.Connection.CreateCommand();
                        cmd.Transaction = transaction;
                        cmd.CommandTimeout = 30;
                        
                        var paramNames = new List<string>();
                        var paramIndex = 0;
                        
                        foreach (var entity in batch)
                        {
                            var paramName = $"{ParamPrefix}p{paramIndex++}";
                            paramNames.Add(paramName);
                            cmd.AddParam(paramName, keyCol.Getter(entity));
                        }
                        
                        cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} IN ({string.Join(",", paramNames)})";
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }
                else
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = BuildDelete(m);
                    cmd.CommandTimeout = 30;
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);
                    
                    foreach (var entity in entityList)
                    {
                        cmd.Parameters.Clear();
                        
                        foreach (var col in m.KeyColumns)
                        {
                            cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                        }
                        
                        if (m.TimestampColumn != null)
                        {
                            cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                        }
                        
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }
                
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                throw;
            }
            
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
    }
}