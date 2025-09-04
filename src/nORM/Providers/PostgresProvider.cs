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

#nullable enable

namespace nORM.Providers
{
    public sealed class PostgresProvider : DatabaseProvider
    {
        private readonly IDbParameterFactory _parameterFactory;

        public PostgresProvider(IDbParameterFactory parameterFactory)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
        }

        public override int MaxSqlLength => int.MaxValue;
        public override int MaxParameters => 32_767;
        public override string Escape(string id) => $"\"{id}\"";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            if (limitParameterName != null)
                sb.Append(" LIMIT ").Append(limitParameterName);

            if (offsetParameterName != null)
                sb.Append(" OFFSET ").Append(offsetParameterName);
        }
        
        public override string GetIdentityRetrievalString(TableMapping m)
        {
            var keyCol = m.KeyColumns.FirstOrDefault(c => c.IsDbGenerated);
            return keyCol != null ? $" RETURNING {keyCol.EscCol}" : string.Empty;
        }
        
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

        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            var pgPath = string.Join(",", jsonPath.Split('.').Skip(1).Select(p => $"'{p}'"));
            return $"jsonb_extract_path_text({columnName}, {pgPath})";
        }

        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection.GetType().FullName != "Npgsql.NpgsqlConnection")
                throw new InvalidOperationException("A NpgsqlConnection is required for PostgresProvider.");
        }

        public override Task<bool> IsAvailableAsync()
        {
            return Task.FromResult(Type.GetType("Npgsql.NpgsqlConnection, Npgsql") != null);
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
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), m, operationKey, entityList.Count);

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
                    await importer.CompleteAsync(ct);
                }
                finally
                {
                    if (importerObj is IAsyncDisposable iad) await iad.DisposeAsync();
                    else if (importerObj is IDisposable id) id.Dispose();
                }
                BatchSizer.RecordBatchPerformance(operationKey, entityList.Count, sw.Elapsed, entityList.Count);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, entityList.Count, sw.Elapsed);
                return entityList.Count;
            }

            var recordsAffected = 0;

            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
                {
                    var batch = entityList.Skip(i).Take(sizing.OptimalBatchSize).ToList();
                    var batchSw = Stopwatch.StartNew();
                    recordsAffected += await ExecutePostgresBatchInsert(ctx, transaction, m, cols, batch, ct);
                    batchSw.Stop();
                    BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                }

                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }

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
            
            return await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
        }

        private string BuildPostgresBatchInsertSql(TableMapping mapping, Column[] cols, int batchSize)
        {
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var sb = new StringBuilder();
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

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            // Use PostgreSQL's UPDATE ... FROM with VALUES for efficient bulk updates
            var tempTableName = $"temp_update_{Guid.NewGuid():N}";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToList();

            var totalUpdated = 0;
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                // Create temporary table
                var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetPostgresType(c.Prop.PropertyType)}"));
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = 30;
                    cmd.CommandText = $"CREATE TEMP TABLE {tempTableName} ({colDefs}) ON COMMIT DROP";
                    await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                }

                // Insert data into temp table
                var tempMapping = new TableMapping(m.Type, this, ctx, null) { EscTable = tempTableName };
                await BulkInsertIntoTable(ctx, tempMapping, entities, transaction, ct);

                // Perform update join
                var setClause = string.Join(", ", nonKeyCols.Select(c => $"{c.EscCol} = temp.{c.EscCol}"));
                var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"{m.EscTable}.{c.EscCol} = temp.{c.EscCol}"));
                
                await using (var cmd = ctx.Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = 30;
                    cmd.CommandText = $"UPDATE {m.EscTable} SET {setClause} FROM {tempTableName} temp WHERE {joinClause}";
                    totalUpdated = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                }

                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        private async Task<int> BulkInsertIntoTable<T>(DbContext ctx, TableMapping mapping, IEnumerable<T> entities, 
            System.Data.Common.DbTransaction transaction, CancellationToken ct) where T : class
        {
            var entityList = entities.ToList();
            var cols = mapping.Columns.Where(c => !c.IsDbGenerated).ToArray();
            var npgConnType = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
            if (npgConnType != null && npgConnType.IsInstanceOfType(ctx.Connection))
            {
                var copySql = $"COPY {mapping.EscTable} ({string.Join(", ", cols.Select(c => c.EscCol))}) FROM STDIN (FORMAT BINARY)";
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
                    await importer.CompleteAsync(ct);
                }
                finally
                {
                    if (importerObj is IAsyncDisposable iad) await iad.DisposeAsync();
                    else if (importerObj is IDisposable id) id.Dispose();
                }
                BatchSizer.RecordBatchPerformance($"Postgres_BulkInsert_{mapping.Type.Name}", entityList.Count, TimeSpan.Zero, entityList.Count);
                return entityList.Count;
            }

            var operationKey = $"Postgres_BulkInsert_{mapping.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(100), mapping, operationKey, entityList.Count);

            var totalInserted = 0;

            for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
            {
                var batch = entityList.Skip(i).Take(sizing.OptimalBatchSize).ToList();
                var sql = BuildPostgresBatchInsertSql(mapping, cols, batch.Count).Replace(" ON CONFLICT DO NOTHING", "");

                await using var cmd = ctx.Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = sql;

                var paramIndex = 0;
                foreach (var entity in batch)
                {
                    foreach (var col in cols)
                    {
                        cmd.AddParam($"{ParamPrefix}p{paramIndex++}", col.Getter(entity) ?? DBNull.Value);
                    }
                }

                var batchSw = Stopwatch.StartNew();
                totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                batchSw.Stop();
                BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
            }

            return totalInserted;
        }

        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            if (!m.KeyColumns.Any())
                throw new Exception($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var totalDeleted = 0;
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                // Use PostgreSQL's DELETE with VALUES clause for efficient bulk deletes
                if (m.KeyColumns.Length == 1)
                {
                    var keyCol = m.KeyColumns[0];
                    var batchSize = 1000;
                    
                    for (int i = 0; i < entityList.Count; i += batchSize)
                    {
                        var batch = entityList.Skip(i).Take(batchSize).ToList();
                        await using var cmd = ctx.Connection.CreateCommand();
                        cmd.Transaction = transaction;
                        cmd.CommandTimeout = 30;
                        
                        var paramNames = new List<string>();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var paramName = $"{ParamPrefix}p{j}";
                            paramNames.Add(paramName);
                            cmd.AddParam(paramName, keyCol.Getter(batch[j]));
                        }
                        
                        cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} = ANY(ARRAY[{string.Join(",", paramNames)}])";
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                    }
                }
                else
                {
                    // For composite keys, use individual deletes with prepared statement
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = BuildDelete(m);
                    await cmd.PrepareAsync(ct);
                    
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
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                    }
                }

                await transaction.CommitAsync(ct);
            }
            catch
            {
                await transaction.RollbackAsync(ct);
                throw;
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