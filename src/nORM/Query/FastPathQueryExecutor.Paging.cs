using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal static partial class FastPathQueryExecutor
    {
        private static string SqlOperator(ExpressionType operation)
            => operation switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                _ => throw new NormUnsupportedFeatureException($"Unsupported predicate operation '{operation}'.")
            };

        /// <summary>
        /// Rewrites predicate values through their column's value converter so the paged fast path
        /// binds the provider representation (a converter storing 42 as -42 must compare against -42).
        /// Returns the same instance when no predicate column has a converter, to avoid allocation on
        /// the common path.
        /// </summary>
        private static ComplexQueryInfo ApplyConvertersToPredicates(TableMapping map, ComplexQueryInfo info)
        {
            PredicateInfo[]? rewritten = null;
            for (var i = 0; i < info.Predicates.Length; i++)
            {
                var predicate = info.Predicates[i];
                if (!map.ColumnsByName.TryGetValue(predicate.Property, out var column))
                    continue;
                var converted = ConvertPredicateValue(column, predicate.Value);
                if (ReferenceEquals(converted, predicate.Value))
                    continue;
                rewritten ??= (PredicateInfo[])info.Predicates.Clone();
                rewritten[i] = predicate with { Value = converted };
            }
            return rewritten == null ? info : info with { Predicates = rewritten };
        }

        private static string BuildFilteredOrderedPageCacheKey<T>(ComplexQueryInfo info, DbContext ctx) where T : class
        {
            var key = new StringBuilder(typeof(T).FullName);
            key.Append('|').Append(ctx.RawProvider.GetType().FullName).Append('|').Append(ctx.GetMappingHash());
            foreach (var predicate in info.Predicates)
            {
                key.Append('|').Append(predicate.Property).Append(':').Append((int)predicate.Operation);
                if (predicate.Value == null || predicate.Value == DBNull.Value)
                    key.Append(":NULL");
                else if (predicate.Value is bool boolValue)
                    key.Append(boolValue ? ":TRUE" : ":FALSE");
                else
                    key.Append(":PARAM");
            }
            key.Append("|ORDER:").Append(info.OrderProperty).Append(info.OrderDescending ? ":DESC" : ":ASC");
            key.Append("|SKIP:").Append(info.SkipCount).Append("|TAKE:").Append(info.TakeCount);
            return key.ToString();
        }

        private static string BuildFilteredOrderedPageSql<T>(DbContext ctx, TableMapping map, ComplexQueryInfo info) where T : class
        {
            var sql = new StringBuilder(GetSqlTemplate<T>(ctx));
            var paramIndex = 0;

            for (var i = 0; i < info.Predicates.Length; i++)
            {
                var predicate = info.Predicates[i];
                if (!map.ColumnsByName.TryGetValue(predicate.Property, out var column))
                    throw new InvalidOperationException($"Fast path failed: column '{predicate.Property}' not found in mapping for '{typeof(T).Name}'.");

                sql.Append(i == 0 ? " WHERE " : " AND ");
                if (predicate.Value == null || predicate.Value == DBNull.Value)
                {
                    sql.Append(column.EscCol).Append(predicate.Operation == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL");
                }
                else if (predicate.Operation == ExpressionType.Equal &&
                         predicate.Value is bool boolValue &&
                         !ctx.RawProvider.ParameterizeFastPathBooleanPredicates)
                {
                    sql.Append(ctx.RawProvider.FormatBooleanPredicate(column.EscCol, boolValue));
                }
                else
                {
                    // Mirror the ETSV DateTime mixed-TZ normalization: wrap the COLUMN side
                    // with datetime() so stored ISO strings with mixed offsets ('+02:00' /
                    // 'Z' / '-02:00') compare chronologically. Parameter side is bound from
                    // .NET DateTime and already in a canonical comparable form -- wrapping
                    // it would break for DateTime.MaxValue ('9999-12-31 23:59:59.9999999')
                    // because SQLite's datetime() returns empty for the .9999999 fractional
                    // overflow. Other providers' override is identity (native DATETIME).
                    var colType = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
                    var paramName = ctx.RawProvider.ParamPrefix + "p" + paramIndex++;
                    string colSql = column.EscCol;
                    // Only wrap on ORDER-sensitive operators -- equality matches storage
                    // format directly (see ETSV comment).
                    bool isOrderCmp = predicate.Operation is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                                       or ExpressionType.LessThan or ExpressionType.LessThanOrEqual;
                    if (isOrderCmp && (colType == typeof(DateTime) || colType == typeof(DateTimeOffset)))
                        colSql = ctx.RawProvider.NormalizeDateTimeForCompare(colSql);
                    // C# string equality is ordinal; providers whose default collation folds case
                    // (MySQL, SQL Server) need the sargable ordinal wrap here too, or the fast path
                    // would match different rows than the full translator.
                    if (predicate.Operation == ExpressionType.Equal
                        && colType == typeof(string)
                        && ctx.RawProvider.DefaultStringEqualityIsCaseInsensitive)
                        sql.Append(ctx.RawProvider.OrdinalStringEqualSql(colSql, paramName));
                    else
                        sql.Append(colSql).Append(' ').Append(SqlOperator(predicate.Operation)).Append(' ').Append(paramName);
                }
            }

            if (info.OrderProperty != null)
            {
                if (!map.ColumnsByName.TryGetValue(info.OrderProperty, out var orderColumn))
                    throw new InvalidOperationException($"Fast path failed: order column '{info.OrderProperty}' not found in mapping for '{typeof(T).Name}'.");
                sql.Append(" ORDER BY ").Append(orderColumn.EscCol);
                if (info.OrderDescending)
                    sql.Append(" DESC");
            }

            if (ctx.RawProvider.UsesFetchOffsetPaging)
            {
                if (info.SkipCount.HasValue || info.TakeCount.HasValue)
                {
                    if (info.OrderProperty == null)
                        sql.Append(" ORDER BY (SELECT NULL)");
                    sql.Append(" OFFSET ").Append(info.SkipCount.GetValueOrDefault()).Append(" ROWS");
                    if (info.TakeCount.HasValue)
                        sql.Append(" FETCH NEXT ").Append(info.TakeCount.Value).Append(" ROWS ONLY");
                }
            }
            else
            {
                if (info.TakeCount.HasValue)
                    sql.Append(" LIMIT ").Append(info.TakeCount.Value);
                if (info.SkipCount.HasValue)
                    sql.Append(" OFFSET ").Append(info.SkipCount.Value);
            }

            return sql.ToString();
        }

        private static Task<List<T>> ExecuteFilteredOrderedPageList<T>(DbContext ctx, ComplexQueryInfo info, bool track, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            info = ApplyConvertersToPredicates(map, info);
            var cacheKey = BuildFilteredOrderedPageCacheKey<T>(info, ctx);
            var sql = _pageSqlCache.GetOrAdd(cacheKey, static (_, state) =>
                BuildFilteredOrderedPageSql<T>(state.Context, state.Mapping, state.Info), (Context: ctx, Mapping: map, Info: info));

            var ensureTask = ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteFilteredOrderedPageListSlowAsync<T>(ensureTask, ctx, sql, info, map, track, ct);

            var timeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (ctx.RawProvider.SupportsFastPathPreparedCommandCache &&
                ctx.Options.CommandInterceptors.Count == 0 &&
                ctx.CurrentTransaction == null)
            {
                // Key by the SQL itself (already computed, unique per shape) to avoid a per-call key-string
                // allocation on the hot path; the paging SQL is structurally distinct from the simple path's.
                var prepared = ctx.GetOrCreateFastPathPreparedCommand(
                    sql,
                    sql,
                    timeout,
                    command => BindFilteredOrderedPageParameters(command, ctx, info));
                return ExecuteFilteredOrderedPagePreparedListAsync<T>(prepared, ctx, info, map, track, ct);
            }

            var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = timeout;
            BindFilteredOrderedPageParameters(cmd, ctx, info);

            if (ctx.RawProvider.PrefersSyncFastPathExecution)
                return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, info.TakeCount, map, track, ct, sync: true);

            return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, info.TakeCount, map, track, ct, sync: false);
        }

        private static async Task<List<T>> ExecuteFilteredOrderedPageListSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, ComplexQueryInfo info, TableMapping map, bool track, CancellationToken ct) where T : class, new()
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            BindFilteredOrderedPageParameters(cmd, ctx, info);

            var results = new List<T>(info.TakeCount ?? QueryExecutor.DefaultListCapacity);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));

            if (track)
                TrackMaterializedResults(ctx, map, results);
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        private static async Task<List<T>> ExecuteFilteredOrderedPagePreparedListAsync<T>(DbContext.FastPathPreparedCommand prepared, DbContext ctx, ComplexQueryInfo info, TableMapping map, bool track, CancellationToken ct) where T : class, new()
        {
            await prepared.Gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cmd = prepared.Command;
                UpdateFilteredOrderedPageParameters(cmd, ctx, info);

                var results = new List<T>(info.TakeCount ?? QueryExecutor.DefaultListCapacity);
                var materializer = GetSyncMaterializer<T>(ctx);
                if (ctx.RawProvider.PrefersSyncFastPathExecution)
                {
                    ct.ThrowIfCancellationRequested();
                    using var reader = cmd.ExecuteReaderWithInterception(ctx, CommandBehavior.SingleResult);
                    while (reader.Read())
                        results.Add(materializer(reader));
                }
                else
                {
                    await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                        results.Add(materializer(reader));
                }

                if (track)
                    TrackMaterializedResults(ctx, map, results);
                if (map.OwnedCollections.Count > 0 && results.Count > 0)
                    await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
                return results;
            }
            finally
            {
                prepared.Gate.Release();
            }
        }

        private static void BindFilteredOrderedPageParameters(System.Data.Common.DbCommand cmd, DbContext ctx, ComplexQueryInfo info)
        {
            var paramIndex = 0;
            foreach (var predicate in info.Predicates)
            {
                if (predicate.Value == null || predicate.Value is DBNull)
                    continue;
                if (predicate.Operation == ExpressionType.Equal &&
                    predicate.Value is bool &&
                    !ctx.RawProvider.ParameterizeFastPathBooleanPredicates)
                    continue;

                cmd.AddOptimizedParam(ctx.RawProvider.ParamPrefix + "p" + paramIndex++, predicate.Value);
            }
        }

        private static void UpdateFilteredOrderedPageParameters(System.Data.Common.DbCommand cmd, DbContext ctx, ComplexQueryInfo info)
        {
            var paramIndex = 0;
            foreach (var predicate in info.Predicates)
            {
                if (predicate.Value == null || predicate.Value is DBNull)
                    continue;
                if (predicate.Operation == ExpressionType.Equal &&
                    predicate.Value is bool &&
                    !ctx.RawProvider.ParameterizeFastPathBooleanPredicates)
                    continue;

                ParameterAssign.AssignValue(cmd.Parameters[paramIndex++], predicate.Value);
            }
        }
    }
}
