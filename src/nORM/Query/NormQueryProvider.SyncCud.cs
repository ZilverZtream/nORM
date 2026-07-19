using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {        /// <summary>
        /// THREAD STARVATION FIX: True synchronous execution path for complex queries.
        /// Uses synchronous ADO.NET methods instead of blocking on async code.
        /// </summary>
        private TResult ExecuteInternalSync<TResult>(Expression expression)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            IReadOnlyDictionary<string, object>? parameterDictionary = null;
            IReadOnlyDictionary<string, object> GetParameterDictionary()
            {
                parameterDictionary ??= EnsureParameterDictionary(plan, paramValues);
                return parameterDictionary;
            }

            Func<TResult> queryExecutorFactory = () =>
            {
                _ctx.EnsureConnection();
                using var cmd = _ctx.CreateCommand();
                cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
                cmd.CommandText = plan.Sql;
                BindPlanParameters(cmd, plan, paramValues);

                object? result;
                if (plan.IsScalar)
                {
                    var scalarResult = cmd.ExecuteScalarWithInterceptionSerializedAndDispose(_ctx);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw?.Elapsed ?? default, scalarResult == null || scalarResult is DBNull ? 0 : 1);
                    if (scalarResult == null || scalarResult is DBNull)
                    {
                        if (plan.MethodName is "Min" or "Max" or "Average" &&
                            typeof(TResult).IsValueType && Nullable.GetUnderlyingType(typeof(TResult)) == null)
                            throw new InvalidOperationException("Sequence contains no elements");
                        if (plan.MethodName == "Sum")
                            return GetZeroOfTargetType<TResult>();
                        return default(TResult)!;
                    }
                    // Min/Max over a value-converter column returns the stored provider value; convert it
                    // back to the model representation (Sum/Average carry no converter).
                    if (plan.ScalarResultConverter != null)
                        scalarResult = plan.ScalarResultConverter.ConvertFromProvider(scalarResult) ?? scalarResult;
                    result = ConvertScalarResult<TResult>(scalarResult)!;
                }
                else
                {
                    var list = _executor.Materialize(plan, cmd);
                    sw?.Stop();
                    _ctx.Options.Logger?.LogQuery(plan.Sql, GetParameterDictionary(), sw?.Elapsed ?? default, list.Count);
                    if (plan.ClientScalar)
                    {
                        // The post-materialize transform reduced the reshaped rows to a
                        // single boxed aggregate value; unwrap it as the query result,
                        // coercing numeric mismatches like the server scalar path.
                        var clientScalar = list[0];
                        result = clientScalar is TResult typedScalar
                            ? typedScalar
                            : ConvertScalarResult<TResult>(clientScalar!);
                    }
                    else if (plan.SingleResult)
                    {
                        result = plan.MethodName switch
                        {
                            "First" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                            // LINQ MinBy/MaxBy return null for an empty sequence of reference-type
                            // or nullable elements; only non-nullable value types throw.
                            "MinBy" or "MaxBy" => list.Count > 0
                                ? list[0]
                                : plan.ElementType.IsValueType && Nullable.GetUnderlyingType(plan.ElementType) == null
                                    ? throw new InvalidOperationException("Sequence contains no elements")
                                    : null,
                            "FirstOrDefault" => list.Count > 0 ? list[0] : null,
                            "Single" => list.Count == 1 ? list[0] : list.Count == 0 ? throw new InvalidOperationException("Sequence contains no elements") : throw new InvalidOperationException("Sequence contains more than one element"),
                            "SingleOrDefault" => list.Count == 0 ? null : list.Count == 1 ? list[0] : throw new InvalidOperationException("Sequence contains more than one element"),
                            "ElementAt" => list.Count > 0 ? list[0] : throw new ArgumentOutOfRangeException("index"),
                            "ElementAtOrDefault" => list.Count > 0 ? list[0] : null,
                            "Last" => list.Count > 0 ? list[0] : throw new InvalidOperationException("Sequence contains no elements"),
                            "LastOrDefault" => list.Count > 0 ? list[0] : null,
                            _ => list
                        };
                    }
                    else
                    {
                        if (typeof(TResult) == typeof(List<object>) && list is IList nonGenericList && list.GetType() != typeof(List<object>))
                        {
                            var countList = nonGenericList.Count;
                            var covariantList = new List<object>(countList);
                            for (int i = 0; i < countList; i++)
                            {
                                covariantList.Add(nonGenericList[i]!);
                            }
                            result = covariantList;
                        }
                        else
                        {
                            result = list;
                        }
                    }
                }
                return (TResult)result!;
            };

            if (ResultCacheUsable(plan))
            {
                var cacheKey = BuildCacheKeyFromPlan<TResult>(plan, GetParameterDictionary());
                var expiration = plan.CacheExpiration ?? _ctx.Options.CacheExpiration;
                return ExecuteWithCacheSync(cacheKey, plan.CacheTables ?? plan.Tables, expiration, queryExecutorFactory);
            }
            else
            {
                return queryExecutorFactory();
            }
        }

        /// <summary>
        /// Executes a DELETE statement represented by the provided LINQ expression. The method
        /// validates the generated plan, constructs the final SQL and executes it, returning the
        /// number of affected rows.
        /// </summary>
        /// <param name="expression">The LINQ expression describing the entities to delete.</param>
        /// <param name="ct">A token used to cancel the asynchronous operation.</param>
        /// <returns>The count of rows removed from the database.</returns>
        private async Task<int> ExecuteDeleteInternalAsync(Expression expression, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            ThrowIfClientMaterializedCudShape(plan, "ExecuteDeleteAsync");
            ThrowIfAsOfCudShape(plan, "ExecuteDeleteAsync");
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            EnsureWritableMapping(mapping, "ExecuteDeleteAsync");
            string finalSql;
            if (plan.Tables.Count != 1)
            {
                ValidateJoinedCudShape(plan.BulkCudShape);
                finalSql = plan.BulkCudShape!.HasPaging
                    ? BuildKeyedSubqueryCudSql("DELETE FROM " + mapping.EscTable, null, plan.Sql, mapping)
                    : BuildJoinedCudWhereInSql("DELETE FROM " + mapping.EscTable, null,
                        QueryTranslator.RemoveTrailingOrderByUnlessPaged(plan.Sql), mapping);
            }
            else
            {
                _cudBuilder.ValidateCudPlan(plan.BulkCudShape);
                if (plan.BulkCudShape!.HasPaging)
                {
                    finalSql = BuildKeyedSubqueryCudSql("DELETE FROM " + mapping.EscTable, null, plan.Sql, mapping);
                }
                else
                {
                    var whereClause = _cudBuilder.GetWhereClauseWithOuterQualifier(plan.BulkCudShape, mapping.EscTable);
                    finalSql = $"DELETE FROM {mapping.EscTable}{whereClause}";
                }
            }
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = finalSql;
            BindPlanParameters(cmd, plan, paramValues);
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(finalSql, EnsureParameterDictionary(plan, paramValues), sw?.Elapsed ?? default, affected);
            // Set-based DELETE persists changes just like SaveChanges and the Bulk* ops,
            // so the result cache for this table must be invalidated or Cacheable() queries
            // keep replaying rows this delete removed.
            _ctx.Options.CacheProvider?.InvalidateTag(mapping.TableName);
            return affected;
        }

        /// <summary>
        /// Client-materialized query shapes (sequence reshapes, streaming GroupBy,
        /// group-join results, client scalar reductions) exist only after
        /// materialization â€” no set-based DELETE/UPDATE can honor them. Emitting the
        /// statement for the underlying rows would silently affect rows the reshaped
        /// query never described (an unfiltered Append source deletes the whole table).
        /// </summary>
        private static void ThrowIfClientMaterializedCudShape(QueryPlan plan, string operation)
        {
            // PostReverse (tail paging) is deliberately absent: TakeLast/SkipLast
            // define their row SET server-side (flipped ORDER BY + paging); only the
            // client-side re-ordering is post-materialize, and CUD ignores order.
            if (plan.PostMaterializeTransform != null || plan.GroupJoinInfo != null || plan.ClientScalar)
            {
                throw new NormUnsupportedFeatureException(
                    $"{operation} over a client-materialized query shape (Append, Prepend, Chunk, Zip, DefaultIfEmpty " +
                    "with a default value, streaming GroupBy, or group-join results) has no set-based SQL " +
                    "equivalent. Express the target rows with Where(...) instead.");
            }
        }

        private static void ThrowIfAsOfCudShape(QueryPlan plan, string operation)
        {
            // Set-based writes target the LIVE table; an AsOf source would either write
            // through a history window (nonsense) or silently select the target rows by
            // historical state while mutating current rows. Fail loud instead.
            if (plan.AsOfTimestamp.HasValue)
                throw new NormUnsupportedFeatureException(
                    $"{operation} cannot be combined with AsOf: writes target the live table, " +
                    "not a historical snapshot.");
        }

        private static void ValidateJoinedCudShape(BulkCudQueryShape? shape)
        {
            if (shape == null)
                throw new NormUnsupportedFeatureException("ExecuteUpdate/Delete requires query-shape metadata.");
            if (shape.HasGroupBy || shape.HasHaving)
                throw new NormUnsupportedFeatureException(
                    "ExecuteUpdate/Delete with a join does not support grouped or aggregated queries.");
        }

        /// <summary>
        /// Resolves the target rows of an ordered/paged/windowed bulk CUD statement
        /// through a keyed subquery over the FULL query SQL: the query embeds as a
        /// derived table so its ORDER BY + paging clause survives on every provider
        /// (the derived-table materialization also lifts MySQL's update-target and
        /// LIMIT-inside-IN restrictions). A bare trailing ORDER BY re-derived by a
        /// window wrap is stripped first â€” it is row-set-neutral and invalid inside
        /// a derived table on SQL Server.
        /// </summary>
        private string BuildKeyedSubqueryCudSql(string prefix, string? setSql, string planSql, TableMapping mapping)
        {
            if (mapping.KeyColumns.Length == 0)
                throw new NormUnsupportedFeatureException(
                    $"ExecuteUpdate/Delete over an ordered or paged query requires key columns on '{mapping.Type.Name}' " +
                    "to resolve the target rows.");

            var innerSql = QueryTranslator.RemoveTrailingOrderByUnlessPaged(planSql);
            var pkCols = mapping.KeyColumns.Select(k => k.EscCol).ToArray();
            var pkSelect = string.Join(", ", pkCols);
            var subquery = "SELECT " + pkSelect + " FROM (" + innerSql + ") AS __nm_cud";

            if (pkCols.Length == 1 || _ctx.RawProvider.SupportsRowTupleComparison)
            {
                var target = pkCols.Length == 1 ? pkCols[0] : "(" + pkSelect + ")";
                var whereIn = target + " IN (" + subquery + ")";
                return setSql == null
                    ? prefix + " WHERE " + whereIn
                    : prefix + " SET " + setSql + " WHERE " + whereIn;
            }

            // SQL Server composite keys: row-tuple IN is unsupported â€” join the keyed
            // subquery to the target instead (same form as the joined-CUD path).
            const string tgtAlias = "__nm_tgt";
            var joinOn = string.Join(" AND ", pkCols.Select(pk => tgtAlias + "." + pk + " = __nm_keys." + pk));
            var keysSubquery = "(" + subquery + ") AS __nm_keys";
            return setSql == null
                ? "DELETE " + tgtAlias + " FROM " + mapping.EscTable + " AS " + tgtAlias
                    + " INNER JOIN " + keysSubquery + " ON " + joinOn
                : "UPDATE " + tgtAlias + " SET " + setSql
                    + " FROM " + mapping.EscTable + " AS " + tgtAlias
                    + " INNER JOIN " + keysSubquery + " ON " + joinOn;
        }

        private string BuildJoinedCudWhereInSql(string prefix, string? setSql, string planSql, TableMapping mapping)
        {
            var fromIdx = planSql.IndexOf(" FROM ", StringComparison.Ordinal);
            if (fromIdx < 0)
                throw new InvalidOperationException("Cannot locate FROM clause in join SQL.");

            if (mapping.KeyColumns.Length == 1)
            {
                var outerAlias = _ctx.RawProvider.Escape("T0");
                var pk = mapping.KeyColumns[0].EscCol;
                var subquery = "SELECT " + outerAlias + "." + pk + planSql[fromIdx..];
                string whereIn;
                if (_ctx.RawProvider.CudWhereInSubqueryNeedsDoubleWrap)
                    whereIn = pk + " IN (SELECT " + pk + " FROM (" + subquery + ") AS __nm_cud)";
                else
                    whereIn = pk + " IN (" + subquery + ")";
                return setSql == null
                    ? prefix + " WHERE " + whereIn
                    : prefix + " SET " + setSql + " WHERE " + whereIn;
            }

            // Composite-PK path
            var cudOuterAlias = _ctx.RawProvider.Escape("T0");
            var pkCols = mapping.KeyColumns.Select(k => k.EscCol).ToArray();
            var subquerySelect = string.Join(", ", pkCols.Select(pk => cudOuterAlias + "." + pk));
            var subquerySql = "SELECT " + subquerySelect + planSql[fromIdx..];

            if (!_ctx.RawProvider.SupportsRowTupleComparison)
            {
                // SQL Server: row-tuple IN is unsupported - use JOIN-based DELETE/UPDATE.
                // DELETE __nm_tgt FROM Table AS __nm_tgt INNER JOIN (...) AS __nm_cud ON T.pk1 = cud.pk1 ...
                const string tgtAlias = "__nm_tgt";
                var joinOn = string.Join(" AND ", pkCols.Select(pk => tgtAlias + "." + pk + " = __nm_cud." + pk));
                var cudSubquery = "(" + subquerySql + ") AS __nm_cud";
                if (setSql == null)
                    return "DELETE " + tgtAlias + " FROM " + mapping.EscTable + " AS " + tgtAlias
                        + " INNER JOIN " + cudSubquery + " ON " + joinOn;
                else
                    return "UPDATE " + tgtAlias + " SET " + setSql
                        + " FROM " + mapping.EscTable + " AS " + tgtAlias
                        + " INNER JOIN " + cudSubquery + " ON " + joinOn;
            }

            // Row-tuple comparison: (pk1, pk2) IN (SELECT T0.pk1, T0.pk2 FROM ...)
            var pkTuple = "(" + string.Join(", ", pkCols) + ")";
            if (_ctx.RawProvider.CudWhereInSubqueryNeedsDoubleWrap)
            {
                var outerSelect = string.Join(", ", pkCols);
                var whereIn = pkTuple + " IN (SELECT " + outerSelect + " FROM (" + subquerySql + ") AS __nm_cud)";
                return setSql == null
                    ? prefix + " WHERE " + whereIn
                    : prefix + " SET " + setSql + " WHERE " + whereIn;
            }
            else
            {
                var whereIn = pkTuple + " IN (" + subquerySql + ")";
                return setSql == null
                    ? prefix + " WHERE " + whereIn
                    : prefix + " SET " + setSql + " WHERE " + whereIn;
            }
        }
        private async Task<int> ExecuteUpdateInternalAsync<T>(Expression expression, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            var plan = GetPlan(expression, out var filtered, out var paramValues);
            ThrowIfClientMaterializedCudShape(plan, "ExecuteUpdateAsync");
            ThrowIfAsOfCudShape(plan, "ExecuteUpdateAsync");
            var rootType = GetElementType(filtered);
            var mapping = _ctx.GetMapping(rootType);
            EnsureWritableMapping(mapping, "ExecuteUpdateAsync");
            string finalSql;
            Dictionary<string, object> setParams;
            if (plan.Tables.Count != 1)
            {
                ValidateJoinedCudShape(plan.BulkCudShape);
                var (setClauseJ, setParamsJ) = _cudBuilder.BuildSetClause(mapping, set);
                setParams = setParamsJ;
                finalSql = plan.BulkCudShape!.HasPaging
                    ? BuildKeyedSubqueryCudSql("UPDATE " + mapping.EscTable, setClauseJ, plan.Sql, mapping)
                    : BuildJoinedCudWhereInSql("UPDATE " + mapping.EscTable, setClauseJ,
                        QueryTranslator.RemoveTrailingOrderByUnlessPaged(plan.Sql), mapping);
            }
            else
            {
                _cudBuilder.ValidateCudPlan(plan.BulkCudShape);
                var (setClause, setParamsSingle) = _cudBuilder.BuildSetClause(mapping, set);
                setParams = setParamsSingle;
                if (plan.BulkCudShape!.HasPaging)
                {
                    finalSql = BuildKeyedSubqueryCudSql("UPDATE " + mapping.EscTable, setClause, plan.Sql, mapping);
                }
                else
                {
                    var whereClause = _cudBuilder.GetWhereClauseWithOuterQualifier(plan.BulkCudShape, mapping.EscTable);
                    finalSql = $"UPDATE {mapping.EscTable} SET {setClause}{whereClause}";
                }
            }
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = finalSql;
            BindPlanParameters(cmd, plan, paramValues);
            foreach (var p in setParams)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var baseDict = EnsureParameterDictionary(plan, paramValues);
            var allParams = baseDict is Dictionary<string, object> mutableDict && !ReferenceEquals(baseDict, plan.Parameters)
                ? mutableDict
                : new Dictionary<string, object>(baseDict);
            foreach (var p in setParams)
                allParams[p.Key] = p.Value;
            var affected = await cmd.ExecuteNonQueryWithInterceptionAsync(_ctx, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(finalSql, allParams, sw?.Elapsed ?? default, affected);
            // Set-based UPDATE persists changes; invalidate the result cache for this table
            // so Cacheable() queries do not keep returning pre-update values.
            _ctx.Options.CacheProvider?.InvalidateTag(mapping.TableName);
            return affected;
        }

        private static void EnsureWritableMapping(TableMapping mapping, string operation)
        {
            if (!mapping.IsReadOnly)
                return;

            throw new NormUnsupportedFeatureException(
                $"{operation} for '{mapping.Type.Name}' is not supported because the entity is configured as read-only/query-only. " +
                "Use Query<T>() or raw SQL query APIs for read access, and map a keyed writable table for generated writes.");
        }
    }
}
