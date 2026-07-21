using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
        /// <summary>
        /// Applies a column's value converter to a predicate value so the fast path binds the
        /// provider representation (e.g. a converter storing 42 as -42 must compare against -42, not
        /// 42). Mirrors what the write path does with ConvertToProvider; without it a filter on a
        /// converted column silently returns the wrong rows. Null is left untouched so the caller's
        /// IS NULL handling still applies.
        /// </summary>
        private static object? ConvertPredicateValue(Column column, object? value)
        {
            if (column.Converter == null || value == null || value is DBNull)
                return value;
            return column.Converter.ConvertToProvider(value);
        }

        /// <summary>
        /// Non-async entry point - does SQL lookup and command setup synchronously,
        /// then dispatches to async materialization. Avoids one async state machine allocation.
        /// </summary>
        private static Task<object> ExecuteSimpleWhereAsObject<T>(DbContext ctx, WhereInfo info, int? takeCount, bool track, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            if (!map.ColumnsByName.TryGetValue(info.Property, out var column))
                throw new InvalidOperationException($"Fast path failed: column '{info.Property}' not found in mapping for '{typeof(T).Name}'.");
            info = info with { Value = ConvertPredicateValue(column, info.Value) };

            // Cache full SQL (SELECT + WHERE + LIMIT) using ValueTuple key to avoid string allocation
            bool isNull = info.Value == null || info.Value == DBNull.Value;
            bool isBoolTrue = !isNull && info.Value is bool bv2 && bv2;
            bool isBoolFalse = !isNull && info.Value is bool bv3 && !bv3;
            string whereKind = isNull ? "N" : isBoolTrue ? "BT" : isBoolFalse ? "BF" : "P";
            var cacheKey = (typeof(T).FullName!, info.Property, whereKind, takeCount,
                            ctx.RawProvider.GetType().FullName!, ctx.GetMappingHash());

            if (!_fullSqlCache.TryGetValue(cacheKey, out var sql))
            {
                sql = GetSqlTemplate<T>(ctx);
                if (isNull)
                    sql += $" WHERE {column.EscCol} IS NULL";
                else if (isBoolTrue)
                    sql += $" WHERE {ctx.RawProvider.FormatBooleanPredicate(column.EscCol, expectedValue: true)}";
                else if (isBoolFalse)
                    sql += $" WHERE {ctx.RawProvider.FormatBooleanPredicate(column.EscCol, expectedValue: false)}";
                else
                    sql += $" WHERE {BuildEqualityPredicate(ctx, column)}";
                if (takeCount.HasValue)
                    sql = ApplyLimit(sql, takeCount.Value, ctx.RawProvider);
                _fullSqlCache[cacheKey] = sql;
            }

            // Skip EnsureConnectionAsync await when connection is already ready
            var ensureTask = ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteSimpleWhereSlowAsync<T>(ensureTask, ctx, sql, info, isNull, isBoolTrue, isBoolFalse, takeCount, track, ct);

            var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.RawProvider.ParamPrefix + "p0", info.Value!);

            // Sync materialization for providers without true async I/O
            if (ctx.RawProvider.PrefersSyncFastPathExecution)
                return ExecuteSimpleWhereMaterializeWithOwnedAsync<T>(cmd, ctx, takeCount, track, ct, sync: true);

            return ExecuteSimpleWhereMaterializeAsync<T>(cmd, ctx, takeCount, track, ct);
        }

        private static Task<List<T>> ExecuteSimpleWhereList<T>(DbContext ctx, WhereInfo info, int? takeCount, bool track, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            if (!map.ColumnsByName.TryGetValue(info.Property, out var column))
                throw new InvalidOperationException($"Fast path failed: column '{info.Property}' not found in mapping for '{typeof(T).Name}'.");
            info = info with { Value = ConvertPredicateValue(column, info.Value) };

            bool isNull = info.Value == null || info.Value == DBNull.Value;
            bool isBoolTrue = !isNull && info.Value is bool bv2 && bv2;
            bool isBoolFalse = !isNull && info.Value is bool bv3 && !bv3;
            string whereKind = isNull ? "N" : isBoolTrue ? "BT" : isBoolFalse ? "BF" : "P";
            var cacheKey = (typeof(T).FullName!, info.Property, whereKind, takeCount,
                            ctx.RawProvider.GetType().FullName!, ctx.GetMappingHash());

            if (!_fullSqlCache.TryGetValue(cacheKey, out var sql))
            {
                sql = GetSqlTemplate<T>(ctx);
                if (isNull)
                    sql += $" WHERE {column.EscCol} IS NULL";
                else if (isBoolTrue)
                    sql += $" WHERE {ctx.RawProvider.FormatBooleanPredicate(column.EscCol, expectedValue: true)}";
                else if (isBoolFalse)
                    sql += $" WHERE {ctx.RawProvider.FormatBooleanPredicate(column.EscCol, expectedValue: false)}";
                else
                    sql += $" WHERE {BuildEqualityPredicate(ctx, column)}";
                if (takeCount.HasValue)
                    sql = ApplyLimit(sql, takeCount.Value, ctx.RawProvider);
                _fullSqlCache[cacheKey] = sql;
            }

            var ensureTask = ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteSimpleWhereListSlowAsync<T>(ensureTask, ctx, sql, info, isNull, isBoolTrue, isBoolFalse, takeCount, map, track, ct);

            var timeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (ctx.RawProvider.SupportsFastPathPreparedCommandCache &&
                ctx.Options.CommandInterceptors.Count == 0 &&
                ctx.CurrentTransaction == null)
            {
                // Key the pooled command by the SQL itself (already computed + cached, unique per shape) so
                // the hot path allocates no per-call key string. The SQL is 1:1 with its shape cache key, and
                // structurally distinct from the paging path's SQL, so it cannot collide. Pass parameter state
                // to a STATIC initializer so no per-call closure is allocated (the initializer runs once, on
                // the cache miss).
                var prepared = ctx.GetOrCreateFastPathPreparedCommand(
                    sql,
                    sql,
                    timeout,
                    (ctx, info, hasParam: !isNull && !isBoolTrue && !isBoolFalse),
                    static (command, s) =>
                    {
                        if (s.hasParam)
                            command.AddOptimizedParam(s.ctx.RawProvider.ParamPrefix + "p0", s.info.Value!);
                    });
                return ExecuteSimpleWherePreparedListAsync<T>(prepared, ctx, info, isNull, isBoolTrue, isBoolFalse, takeCount, map, track, ct);
            }

            var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = timeout;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.RawProvider.ParamPrefix + "p0", info.Value!);

            if (ctx.RawProvider.PrefersSyncFastPathExecution)
                return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, takeCount, map, track, ct, sync: true);

            return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, takeCount, map, track, ct, sync: false);
        }

        private static async Task<List<T>> ExecuteSimpleWherePreparedListAsync<T>(
            DbContext.FastPathPreparedCommand prepared,
            DbContext ctx,
            WhereInfo info,
            bool isNull,
            bool isBoolTrue,
            bool isBoolFalse,
            int? takeCount,
            TableMapping map,
            bool track,
            CancellationToken ct) where T : class, new()
        {
            await prepared.Gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cmd = prepared.Command;
                if (!isNull && !isBoolTrue && !isBoolFalse)
                    ParameterAssign.AssignValue(cmd.Parameters[0], info.Value);

                var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
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

        private static async Task<object> ExecuteSimpleWhereSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, WhereInfo info, bool isNull, bool isBoolTrue, bool isBoolFalse, int? takeCount, bool track, CancellationToken ct) where T : class, new()
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.RawProvider.ParamPrefix + "p0", info.Value!);
            var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));
            var map = ctx.GetMapping(typeof(T));
            if (track)
                TrackMaterializedResults(ctx, map, results);
            // Load owned collections (OwnsMany) if configured
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        /// <summary>Materializes WHERE results and loads owned collections (sync read + async owned-collection load).</summary>
        private static async Task<object> ExecuteSimpleWhereMaterializeWithOwnedAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, bool track, CancellationToken ct, bool sync) where T : class, new()
        {
            var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
            var materializer = GetSyncMaterializer<T>(ctx);
            if (sync)
            {
                var commandLifetimeTransferred = false;
                try
                {
                    commandLifetimeTransferred = true;
                    using var reader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(ctx, CommandBehavior.SingleResult);
                    while (reader.Read())
                        results.Add(materializer(reader));
                }
                catch
                {
                    if (!commandLifetimeTransferred)
                        cmd.Dispose();
                    throw;
                }
            }
            else
            {
                await using var command = cmd;
                await using var asyncReader = await command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
                while (await asyncReader.ReadAsync(ct).ConfigureAwait(false))
                    results.Add(materializer(asyncReader));
            }
            var map = ctx.GetMapping(typeof(T));
            if (track)
                TrackMaterializedResults(ctx, map, results);
            // Load owned collections (OwnsMany) if configured
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        private static async Task<object> ExecuteSimpleWhereMaterializeAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, bool track, CancellationToken ct) where T : class, new()
        {
            var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var command = cmd;
            await using var reader = await command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));

            var map = ctx.GetMapping(typeof(T));
            if (track)
                TrackMaterializedResults(ctx, map, results);
            // Load owned collections (OwnsMany) if configured
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);

            return results;
        }

        private static async Task<List<T>> ExecuteSimpleWhereListSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, WhereInfo info, bool isNull, bool isBoolTrue, bool isBoolFalse, int? takeCount, TableMapping map, bool track, CancellationToken ct) where T : class, new()
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.RawProvider.ParamPrefix + "p0", info.Value!);

            var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
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

        private static async Task<List<T>> ExecuteSimpleWhereListMaterializeAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, TableMapping map, bool track, CancellationToken ct, bool sync) where T : class, new()
        {
            var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
            var materializer = GetSyncMaterializer<T>(ctx);
            if (sync)
            {
                var commandLifetimeTransferred = false;
                try
                {
                    commandLifetimeTransferred = true;
                    using var reader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(ctx, CommandBehavior.SingleResult);
                    while (reader.Read())
                        results.Add(materializer(reader));
                }
                catch
                {
                    if (!commandLifetimeTransferred)
                        cmd.Dispose();
                    throw;
                }
            }
            else
            {
                await using var command = cmd;
                await using var reader = await command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    results.Add(materializer(reader));
            }

            if (track)
                TrackMaterializedResults(ctx, map, results);
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }
        /// <summary>
        /// Single async method - eliminates the extra async state machine from a wrapper approach.
        /// </summary>
        private static async Task<object> ExecuteSimpleTakeAsObject<T>(DbContext ctx, int? takeCount, bool track, CancellationToken ct) where T : class, new()
            => await ExecuteSimpleTakeList<T>(ctx, takeCount, track, ct).ConfigureAwait(false);

        /// <summary>
        /// Executes a First/FirstOrDefault fast-path read (WHERE ... LIMIT 1, or LIMIT 1 with no filter) and
        /// returns the single element as <see cref="object"/>: the materialized row, or — when the sequence is
        /// empty — either a thrown <see cref="InvalidOperationException"/> (First) or <c>null</c> (FirstOrDefault).
        /// Reuses the list executors' pooled/prepared command and cached SQL/materializer; the LIMIT 1 keeps the
        /// intermediate list a single element.
        /// </summary>
        private static async Task<object> ExecuteWhereFirstAsObject<T>(DbContext ctx, WhereInfo info, bool hasFilter, bool track, bool throwOnEmpty, CancellationToken ct) where T : class, new()
        {
            var list = hasFilter
                ? await ExecuteSimpleWhereList<T>(ctx, info, 1, track, ct).ConfigureAwait(false)
                : await ExecuteSimpleTakeList<T>(ctx, 1, track, ct).ConfigureAwait(false);
            if (list.Count > 0)
                return list[0];
            if (throwOnEmpty)
                throw new InvalidOperationException("Sequence contains no elements.");
            return null!;
        }

        private static async Task<List<T>> ExecuteSimpleTakeList<T>(DbContext ctx, int? takeCount, bool track, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            string sql = GetSqlTemplate<T>(ctx);
            if (takeCount.HasValue)
            {
                sql = ApplyLimit(sql, takeCount.Value, ctx.RawProvider);
            }
            await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            var results = new List<T>(QueryExecutor.ClampTakeCapacity(takeCount));
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                results.Add(materializer(reader));
            }
            if (track)
                TrackMaterializedResults(ctx, map, results);
            // Load owned collections (OwnsMany) if configured
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }
        private static async Task<object> ExecuteSimpleCount<T>(DbContext ctx, CancellationToken ct) where T : class
        {
            var map = ctx.GetMapping(typeof(T));
            var sql = $"SELECT COUNT(*) FROM {map.EscTable}";
            await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            var result = await cmd.ExecuteScalarWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            // Guard against null/DBNull from empty tables or provider-specific edge cases
            if (result == null || result is DBNull)
                return (object)0;
            return (object)Convert.ToInt32(result);
        }
    }
}
