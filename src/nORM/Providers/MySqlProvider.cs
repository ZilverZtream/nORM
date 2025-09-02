using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Mapping;

#nullable enable

namespace nORM.Providers
{
    public sealed class MySqlProvider : DatabaseProvider
    {
        public override string Escape(string id) => $"`{id}`";
        
        public override void ApplyPaging(StringBuilder sb, int? limit, int? offset)
        {
            if (limit.HasValue) sb.Append($" LIMIT {offset ?? 0}, {limit}");
        }
        
        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT LAST_INSERT_ID();";
        
        public override System.Data.Common.DbParameter CreateParameter(string name, object? value)
        {
            // MySQL uses MySqlConnector or MySql.Data parameters
            // Try MySqlConnector first (recommended), then MySql.Data
            var paramType = Type.GetType("MySqlConnector.MySqlParameter, MySqlConnector") ?? 
                           Type.GetType("MySql.Data.MySqlClient.MySqlParameter, MySql.Data");
            
            if (paramType != null)
            {
                return (System.Data.Common.DbParameter)Activator.CreateInstance(paramType, name, value ?? DBNull.Value)!;
            }
            else
            {
                // Fallback for when MySQL package is not available
                throw new InvalidOperationException("MySQL package is required for MySQL support. Please install either MySqlConnector or MySql.Data NuGet package.");
            }
        }
        
        public override Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
            => base.BulkInsertAsync(ctx, m, e, ct);

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"`BulkUpdate_{Guid.NewGuid():N}`";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();

            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TEMPORARY TABLE {tempTableName} ({colDefs})";
                await cmd.ExecuteNonQueryAsync(ct);
            }

            var tempMapping = new TableMapping(m.Type, this, ctx, null) { EscTable = tempTableName };
            await base.BulkInsertAsync(ctx, tempMapping, entities, ct);

            var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;
                cmd.CommandText = $"UPDATE {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause} SET {setClause}";
                var updatedCount = await cmd.ExecuteNonQueryAsync(ct);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
                return updatedCount;
            }
        }
        
        public override Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
            => base.BatchedDeleteAsync(ctx, m, e, ct);

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
    }
}