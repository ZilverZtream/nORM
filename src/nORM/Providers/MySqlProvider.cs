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
                dynamic bulkCopy = Activator.CreateInstance(bulkCopyType, ctx.Connection)!;
                bulkCopy.DestinationTableName = m.EscTable.Trim('`');
                bulkCopy.BulkCopyTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                var totalInserted = 0;
                for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
                {
                    var batch = entityList.Skip(i).Take(sizing.OptimalBatchSize).ToList();
                    using var table = GetDataTable(m, insertableCols);
                    foreach (var entity in batch)
                        table.Rows.Add(insertableCols.Select(c => c.Getter(entity) ?? DBNull.Value).ToArray());

                    var batchSw = Stopwatch.StartNew();
                    await bulkCopy.WriteToServerAsync(table, ct);
                    batchSw.Stop();
                    totalInserted += table.Rows.Count;
                    BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                }

                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw.Elapsed);
                return totalInserted;
            }

            var affected = await base.BulkInsertAsync(ctx, m, entityList, ct);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, affected, sw.Elapsed);
            return affected;
        }

        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            if (ctx.Options.UseBatchedBulkOps) return await base.BatchedUpdateAsync(ctx, m, entities, ct);

            var sw = Stopwatch.StartNew();
            var tempTableName = $"`BulkUpdate_{Guid.NewGuid():N}`";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey).ToList();

            var colDefs = string.Join(", ", m.Columns.Select(c => $"{c.EscCol} {GetSqlType(c.Prop.PropertyType)}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"CREATE TEMPORARY TABLE {tempTableName} ({colDefs})";
                await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
            }

            var tempMapping = new TableMapping(m.Type, this, ctx, null) { EscTable = tempTableName };
            await BulkInsertAsync(ctx, tempMapping, entities, ct);

            var setClause = string.Join(", ", nonKeyCols.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            var joinClause = string.Join(" AND ", m.KeyColumns.Select(c => $"T1.{c.EscCol} = T2.{c.EscCol}"));
            await using (var cmd = ctx.Connection.CreateCommand())
            {
                cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                cmd.CommandText = $"UPDATE {m.EscTable} T1 JOIN {tempTableName} T2 ON {joinClause} SET {setClause}";
                var updatedCount = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, updatedCount, sw.Elapsed);
                return updatedCount;
            }
        }
        
        public override Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.Connection);
            return base.BatchedDeleteAsync(ctx, m, e, ct);
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