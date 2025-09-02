using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Providers
{
    public sealed class PostgresProvider : DatabaseProvider
    {
        public override string Escape(string id) => $"\"{id}\"";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset)
        {
            if (limit.HasValue) sb.Append($" LIMIT {limit}");
            if (offset.HasValue) sb.Append($" OFFSET {offset}");
        }
        
        public override string GetIdentityRetrievalString(TableMapping m)
        {
            var keyCol = m.KeyColumns.FirstOrDefault(c => c.IsDbGenerated);
            return keyCol != null ? $" RETURNING {keyCol.EscCol}" : string.Empty;
        }
        
        public override System.Data.Common.DbParameter CreateParameter(string name, object? value)
        {
            // PostgreSQL uses Npgsql parameters
            // Since we don't have Npgsql reference, create a generic parameter
            // In a real implementation, this would be: new NpgsqlParameter(name, value ?? DBNull.Value)
            var paramType = Type.GetType("Npgsql.NpgsqlParameter, Npgsql");
            if (paramType != null)
            {
                return (System.Data.Common.DbParameter)Activator.CreateInstance(paramType, name, value ?? DBNull.Value)!;
            }
            else
            {
                // Fallback for when Npgsql is not available
                throw new InvalidOperationException("Npgsql package is required for PostgreSQL support. Please install the Npgsql NuGet package.");
            }
        }

        // PostgreSQL-optimized bulk operations
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();
            if (!cols.Any()) return 0;

            var recordsAffected = 0;
            
            // PostgreSQL supports large batch sizes efficiently
            var batchSize = CalculateOptimalBatchSize(cols.Length);
            
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.Skip(i).Take(batchSize).ToList();
                    recordsAffected += await ExecutePostgresBatchInsert(ctx.Connection, transaction, m, cols, batch, ct);
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

        private static int CalculateOptimalBatchSize(int columnCount)
        {
            // PostgreSQL handles large parameter counts well
            var maxParams = 32767; // PostgreSQL parameter limit
            var maxRowsPerBatch = maxParams / columnCount;
            
            // Cap at reasonable batch size for memory efficiency
            return Math.Min(maxRowsPerBatch, 1000);
        }

        private async Task<int> ExecutePostgresBatchInsert<T>(System.Data.Common.DbConnection connection, System.Data.Common.DbTransaction transaction,
            TableMapping mapping, Column[] cols, List<T> batch, CancellationToken ct) where T : class
        {
            // Use PostgreSQL's multi-row VALUES syntax with RETURNING for identity columns
            var sql = BuildPostgresBatchInsertSql(mapping, cols, batch.Count);
            
            await using var cmd = connection.CreateCommand();
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
            
            return await cmd.ExecuteNonQueryAsync(ct);
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
                    await cmd.ExecuteNonQueryAsync(ct);
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
                    totalUpdated = await cmd.ExecuteNonQueryAsync(ct);
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
            var batchSize = CalculateOptimalBatchSize(cols.Length);
            var totalInserted = 0;

            for (int i = 0; i < entityList.Count; i += batchSize)
            {
                var batch = entityList.Skip(i).Take(batchSize).ToList();
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
                
                totalInserted += await cmd.ExecuteNonQueryAsync(ct);
            }

            return totalInserted;
        }

        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
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
                        totalDeleted += await cmd.ExecuteNonQueryAsync(ct);
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
                        totalDeleted += await cmd.ExecuteNonQueryAsync(ct);
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