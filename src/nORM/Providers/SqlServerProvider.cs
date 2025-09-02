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

        #region SQL Server Bulk Operations
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var sw = Stopwatch.StartNew();
            using var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection);
            bulkCopy.DestinationTableName = m.EscTable;
            bulkCopy.BatchSize = ctx.Options.BulkBatchSize;
            using var table = new DataTable();

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            foreach (var col in insertableCols)
            {
                var propType = col.Prop.PropertyType;
                table.Columns.Add(col.PropName, Nullable.GetUnderlyingType(propType) ?? propType);
                bulkCopy.ColumnMappings.Add(col.PropName, col.EscCol.Trim('[', ']'));
            }

            var count = 0;
            foreach (var entity in entities)
            {
                table.Rows.Add(insertableCols.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                count++;
            }

            await bulkCopy.WriteToServerAsync(table, ct);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, count, sw.Elapsed);
            return count;
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
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await BulkInsertInternalAsync(ctx, m, entities, tempTableName, ct);

            var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"UPDATE T1 SET {setClause} FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                var updatedCount = await cmd.ExecuteNonQueryAsync(ct);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
                return updatedCount;
            }
        }

        private async Task<int> BulkInsertInternalAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, string destinationTableName, CancellationToken ct) where T : class
        {
            using var bulkCopy = new SqlBulkCopy(ctx.Connection as SqlConnection);
            bulkCopy.DestinationTableName = destinationTableName;
            bulkCopy.BatchSize = ctx.Options.BulkBatchSize;
            using var table = new DataTable();

            var insertableCols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            foreach (var col in insertableCols)
            {
                var propType = col.Prop.PropertyType;
                table.Columns.Add(col.PropName, Nullable.GetUnderlyingType(propType) ?? propType);
                bulkCopy.ColumnMappings.Add(col.PropName, col.EscCol.Trim('[', ']'));
            }

            var count = 0;
            foreach (var entity in entities)
            {
                table.Rows.Add(insertableCols.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                count++;
            }

            await bulkCopy.WriteToServerAsync(table, ct);
            return count;
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
                await cmd.ExecuteNonQueryAsync(ct);
            }

            using (var bulkCopy = new SqlBulkCopy((SqlConnection)ctx.Connection))
            {
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.BatchSize = ctx.Options.BulkBatchSize;
                using var table = new DataTable();
                foreach (var col in m.KeyColumns) table.Columns.Add(col.PropName, Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType);
                foreach (var entity in entities) table.Rows.Add(m.KeyColumns.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());
                await bulkCopy.WriteToServerAsync(table, ct);
            }

            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"DELETE T1 FROM {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause}";
                var deletedCount = await cmd.ExecuteNonQueryAsync(ct);
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
        #endregion
    }
}