using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Providers
{
    public sealed class SqlServerProvider : DatabaseProvider
    {
        private static readonly ConcurrentLruCache<Type, DataTable> _tableSchemas = new(maxSize: 100);
        private static readonly ConcurrentLruCache<Type, DataTable> _keyTableSchemas = new(maxSize: 100);
        public override string Escape(string id) => $"[{id}]";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset)
        {
            if (offset.HasValue || limit.HasValue)
            {
                if (!sb.ToString().Contains("ORDER BY")) sb.Append(" ORDER BY (SELECT NULL)");
                sb.Append($" OFFSET {offset ?? 0} ROWS FETCH NEXT {limit ?? int.MaxValue} ROWS ONLY");
            }
        }
        
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT SCOPE_IDENTITY();";

        public override System.Data.Common.DbParameter CreateParameter(string name, object? value)
        {
            var param = new SqlParameter(name, value ?? DBNull.Value);
            return param;
        }

        public override string EscapeLikePattern(string value)
        {
            var escaped = base.EscapeLikePattern(value);
            var esc = LikeEscapeChar.ToString();
            return escaped
                .Replace("[", esc + "[")
                .Replace("]", esc + "]")
                .Replace("^", esc + "^");
        }

        #region SQL Server Bulk Operations
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var sw = Stopwatch.StartNew();
            using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection)
            {
                DestinationTableName = m.EscTable,
                BatchSize = ctx.Options.BulkBatchSize,
                EnableStreaming = true
            };

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            foreach (var col in insertableCols)
                bulkCopy.ColumnMappings.Add(col.PropName, col.EscCol.Trim('[', ']'));

            using var table = GetDataTable(m, insertableCols);

            var batchCount = 0;
            var total = 0;
            foreach (var entity in entities)
            {
                table.Rows.Add(insertableCols.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                batchCount++;
                if (batchCount >= ctx.Options.BulkBatchSize)
                {
                    await bulkCopy.WriteToServerAsync(table, ct);
                    total += batchCount;
                    table.Clear();
                    batchCount = 0;
                }
            }

            if (batchCount > 0)
            {
                await bulkCopy.WriteToServerAsync(table, ct);
                total += batchCount;
            }

            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, total, sw.Elapsed);
            return total;
        }

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"#BulkUpdate_{Guid.NewGuid():N}";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();

            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TABLE {tempTableName} ({colDefs})";
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
            }

            await BulkInsertInternalAsync(ctx, m, entities, tempTableName, ct);

            var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"UPDATE T1 SET {setClause} FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                var updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
                return updatedCount;
            }
        }

        private async Task<int> BulkInsertInternalAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, string destinationTableName, CancellationToken ct) where T : class
        {
            using var bulkCopy = new SqlBulkCopy(ctx.Connection as SqlConnection)
            {
                DestinationTableName = destinationTableName,
                BatchSize = ctx.Options.BulkBatchSize,
                EnableStreaming = true
            };

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            foreach (var col in insertableCols)
                bulkCopy.ColumnMappings.Add(col.PropName, col.EscCol.Trim('[', ']'));

            using var table = GetDataTable(m, insertableCols);

            var batchCount = 0;
            var total = 0;
            foreach (var entity in entities)
            {
                table.Rows.Add(insertableCols.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                batchCount++;
                if (batchCount >= ctx.Options.BulkBatchSize)
                {
                    await bulkCopy.WriteToServerAsync(table, ct);
                    total += batchCount;
                    table.Clear();
                    batchCount = 0;
                }
            }

            if (batchCount > 0)
            {
                await bulkCopy.WriteToServerAsync(table, ct);
                total += batchCount;
            }

            return total;
        }

        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedDeleteAsync(ctx, m, entities, ct);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"#BulkDelete_{Guid.NewGuid():N}";
            var keyColDefs = string.Join(", ", m.KeyColumns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));

            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TABLE {tempTableName} ({keyColDefs})";
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
            }

            using (var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection)
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
                    table.Rows.Add(m.KeyColumns.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                    batchCount++;
                    if (batchCount >= ctx.Options.BulkBatchSize)
                    {
                        await bulkCopy.WriteToServerAsync(table, ct);
                        table.Clear();
                        batchCount = 0;
                    }
                }
                if (batchCount > 0)
                    await bulkCopy.WriteToServerAsync(table, ct);
            }

            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"DELETE T1 FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                var deletedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, deletedCount, sw.Elapsed);
                return deletedCount;
            }
        }

        private static string GetSqlType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int)) return "INT";
            if (t == typeof(long)) return "BIGINT";
            if (t == typeof(string)) return "NVARCHAR(MAX)";
            if (t == typeof(DateTime)) return "DATETIME2";
            if (t == typeof(bool)) return "BIT";
            if (t == typeof(decimal)) return "DECIMAL(18,2)";
            if (t == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (t == typeof(byte[])) return "VARBINARY(MAX)";
            return "NVARCHAR(MAX)";
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

        private static DataTable GetKeyTable(TableMapping m)
        {
            var schema = _keyTableSchemas.GetOrAdd(m.Type, _ =>
            {
                var dt = new DataTable();
                foreach (var c in m.KeyColumns)
                {
                    var propType = c.Prop.PropertyType;
                    dt.Columns.Add(c.PropName, Nullable.GetUnderlyingType(propType) ?? propType);
                }
                return dt;
            });
            return schema.Clone();
        }
        #endregion
    }
}