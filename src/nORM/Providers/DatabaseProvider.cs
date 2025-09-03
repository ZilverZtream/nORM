using System;
using System.Collections.Generic;
using System.Data.Common;
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
    public abstract class DatabaseProvider
    {
        private static readonly ConcurrentLruCache<(Type Type, string Operation), string> _sqlCache = new(maxSize: 1000);
        
        public string ParamPrefix { get; protected init; } = "@";
        public abstract string Escape(string id);
        public abstract void ApplyPaging(StringBuilder sb, int? limit, int? offset, string? limitParam, string? offsetParam);
        public abstract string GetIdentityRetrievalString(TableMapping m);
        public abstract DbParameter CreateParameter(string name, object? value);
        public abstract string? TranslateFunction(string name, Type declaringType, params string[] args);

        public virtual char LikeEscapeChar => '\\';

        public virtual string EscapeLikePattern(string value)
        {
            var esc = LikeEscapeChar.ToString();
            return value
                .Replace(esc, esc + esc)
                .Replace("%", esc + "%")
                .Replace("_", esc + "_");
        }

        #region Bulk Operations (Abstract & Fallback)
        public virtual async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var sw = Stopwatch.StartNew();
            int recordsAffected = 0;
            var batchSize = ctx.Options.BulkBatchSize;
            var batch = new List<T>(batchSize);

            foreach (var entity in entities)
            {
                batch.Add(entity);
                if (batch.Count >= batchSize)
                {
                    recordsAffected += await ExecuteInsertBatch(ctx, m, batch, ct);
                    batch.Clear();
                }
            }
            if (batch.Count > 0)
            {
                recordsAffected += await ExecuteInsertBatch(ctx, m, batch, ct);
            }
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }

        protected async Task<int> ExecuteInsertBatch<T>(DbContext ctx, TableMapping m, List<T> batch, CancellationToken ct) where T : class
        {
            var sb = new StringBuilder();
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
            return await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
        }

        public virtual Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedUpdateAsync(ctx, m, e, ct);
            throw new NotImplementedException("This provider does not have a native bulk update implementation.");
        }
        
        public virtual Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedDeleteAsync(ctx, m, e, ct);
            throw new NotImplementedException("This provider does not have a native bulk delete implementation.");
        }

        protected async Task<int> BatchedUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var sw = Stopwatch.StartNew();
            var totalUpdated = 0;
            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                foreach (var entity in entities)
                {
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = BuildUpdate(m);
                    foreach (var col in m.Columns.Where(c => !c.IsTimestamp)) cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    if (m.TimestampColumn != null) cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                    totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
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

        protected async Task<int> BatchedDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (!entityList.Any()) return 0;

            if (!m.KeyColumns.Any())
                throw new Exception($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var totalDeleted = 0;
            var batchSize = ctx.Options.BulkBatchSize;
            var keyColumns = m.KeyColumns.ToList();

            await using var transaction = await ctx.Connection.BeginTransactionAsync(ct);
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.Skip(i).Take(batchSize).ToList();
                    await using var cmd = ctx.Connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.CommandTimeout.TotalSeconds;

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
                    var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct);
                    totalDeleted += deleted;
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
        #endregion

        #region SQL Generation
        public string BuildInsert(TableMapping m)
        {
            return _sqlCache.GetOrAdd((m.Type, "INSERT"), _ => {
                var cols = m.Columns.Where(c => !c.IsDbGenerated);
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                var valParams = string.Join(", ", cols.Select(c => ParamPrefix + c.PropName));
                return $"INSERT INTO {m.EscTable} ({colNames}) VALUES ({valParams}){GetIdentityRetrievalString(m)}";
            });
        }

        public string BuildUpdate(TableMapping m)
        {
            return _sqlCache.GetOrAdd((m.Type, "UPDATE"), _ =>
            {
                var set = string.Join(", ", m.Columns
                    .Where(c => !c.IsKey && !c.IsTimestamp)
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}"));

                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}");
                if (m.TimestampColumn != null)
                    whereCols = whereCols.Append($"{m.TimestampColumn.EscCol}={ParamPrefix}{m.TimestampColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"UPDATE {m.EscTable} SET {set} WHERE {where}";
            });
        }

        public string BuildDelete(TableMapping m)
        {
            return _sqlCache.GetOrAdd((m.Type, "DELETE"), _ =>
            {
                var whereCols = m.KeyColumns
                    .Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}");
                if (m.TimestampColumn != null)
                    whereCols = whereCols.Append($"{m.TimestampColumn.EscCol}={ParamPrefix}{m.TimestampColumn.PropName}");
                var where = string.Join(" AND ", whereCols);

                return $"DELETE FROM {m.EscTable} WHERE {where}";
            });
        }
        #endregion
    }
}