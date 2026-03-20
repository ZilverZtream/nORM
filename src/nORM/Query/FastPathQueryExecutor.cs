using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
#nullable enable
namespace nORM.Query
{
    internal static class FastPathQueryExecutor
    {
        private readonly record struct WhereInfo(string Property, object Value);

        /// <summary>
        /// Cached delegates to avoid MakeGenericMethod and Invoke on every query,
        /// eliminating the reflection overhead in the fast path.
        /// </summary>
        private delegate bool TryExecuteDelegate(Expression expr, DbContext ctx, CancellationToken ct, out Task<object> result);
        private static readonly ConcurrentDictionary<Type, TryExecuteDelegate> _cachedExecutors = new();

        /// <summary>
        /// Singleton MaterializerFactory — it only wraps static caches, no instance state.
        /// Eliminates one heap allocation per fast-path query.
        /// </summary>
        private static readonly MaterializerFactory _materializer = new();

        /// <summary>
        /// Cached sync materializer delegates keyed by entity type and mapping hash.
        /// The mapping hash distinguishes contexts whose fluent configuration differs
        /// for the same CLR type, preventing stale delegate reuse across divergent schemas.
        /// </summary>
        private static readonly ConcurrentDictionary<(Type EntityType, int MappingHash), Delegate> _syncMaterializerCache = new();

        /// <summary>
        /// Cached full SQL strings (SELECT + WHERE + LIMIT/TOP) for fast-path queries.
        /// Key includes the fully-qualified type name (avoids same-short-name collision across
        /// namespaces), provider type (avoids quoting/literal poisoning across provider switches),
        /// and mapping hash (avoids wrong SQL reuse when fluent config differs across contexts).
        /// </summary>
        private static readonly ConcurrentDictionary<(string TypeFullName, string Property, string WhereKind, int? TakeCount, string ProviderType, int MappingHash), string> _fullSqlCache = new();

        /// <summary>
        /// Cache whether a type is eligible for fast-path execution (class with parameterless ctor).
        /// Avoids GetConstructor reflection call on every query for types that fail the check (e.g. anonymous types from joins).
        /// </summary>
        private static readonly ConcurrentDictionary<Type, bool> _eligibleTypeCache = new();

        /// <summary>
        /// Non-generic entry point that uses cached delegates to avoid reflection overhead.
        /// </summary>
        public static bool TryExecuteNonGeneric(Type elementType, Expression expr, DbContext ctx, CancellationToken ct, out Task<object> result)
        {
            result = default!;

            // Cached check — avoids GetConstructor reflection on every call for ineligible types
            if (!_eligibleTypeCache.GetOrAdd(elementType, static t => t.IsClass && t.GetConstructor(Type.EmptyTypes) != null))
                return false;

            var executor = _cachedExecutors.GetOrAdd(elementType, t =>
            {
                // Compile the generic TryExecute<T> method once per type
                var method = typeof(FastPathQueryExecutor)
                    .GetMethod(nameof(TryExecute), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(t);

                return (TryExecuteDelegate)Delegate.CreateDelegate(typeof(TryExecuteDelegate), method);
            });

            return executor(expr, ctx, ct, out result);
        }

        public static bool TryExecute<T>(Expression expr, DbContext ctx, CancellationToken ct, out Task<object> result) where T : class, new()
        {
            result = default!;
            if (ctx.Options.GlobalFilters.Count > 0 || ctx.Options.TenantProvider != null)
                return false;
            if (IsSimpleCountPattern(expr, out var hasPredicate))
            {
                if (hasPredicate)
                {
                    return false;
                }
                result = ExecuteSimpleCount<T>(ctx, ct);
                return true;
            }
            if (IsSimpleWherePattern(expr, out var whereInfo, out var takeCount))
            {
                // Use async/await wrapper instead of ContinueWith to avoid closure allocation
                result = ExecuteSimpleWhereAsObject<T>(ctx, whereInfo, takeCount, ct);
                return true;
            }
            if (IsSimpleTakePattern(expr, out takeCount))
            {
                // Use async/await wrapper instead of ContinueWith to avoid closure allocation
                result = ExecuteSimpleTakeAsObject<T>(ctx, takeCount, ct);
                return true;
            }
            return false;
        }
        private static bool IsSimpleCountPattern(Expression expr, out bool hasPredicate)
        {
            hasPredicate = false;
            if (expr is not MethodCallExpression countCall ||
                (countCall.Method.Name != nameof(Queryable.Count) && countCall.Method.Name != nameof(Queryable.LongCount)))
            {
                return false;
            }
            if (countCall.Arguments.Count == 2)
            {
                if (Unwrap(countCall.Arguments[0]) is not ConstantExpression) return false;
                if (countCall.Arguments[1] is not LambdaExpression) return false;
                hasPredicate = true;
                return true;
            }
            if (countCall.Arguments.Count == 1)
            {
                if (Unwrap(countCall.Arguments[0]) is not ConstantExpression) return false;
                hasPredicate = false;
                return true;
            }
            return false;
        }
        private static bool IsSimpleWherePattern(Expression expr, out WhereInfo info, out int? takeCount)
        {
            info = default;
            takeCount = null;
            if (expr is MethodCallExpression takeCall && takeCall.Method.Name == nameof(Queryable.Take))
            {
                if (takeCall.Arguments[1] is ConstantExpression ce)
                    takeCount = (int)ce.Value!;
                else
                    return false;
                expr = takeCall.Arguments[0];
            }
            // Unwrap AsNoTracking between Take and Where so that
            // queries like .Where(...).AsNoTracking().Take(10) hit the fast path.
            expr = Unwrap(expr);
            if (expr is not MethodCallExpression whereCall || whereCall.Method.Name != nameof(Queryable.Where))
                return false;
            if (Unwrap(whereCall.Arguments[0]) is not ConstantExpression)
                return false;
            var lambdaArg = whereCall.Arguments[1];
            while (lambdaArg is UnaryExpression { NodeType: ExpressionType.Quote } q)
                lambdaArg = q.Operand;
            if (lambdaArg is not LambdaExpression lambda)
                return false;
            var body = lambda.Body;
            // Support boolean member access: u => u.IsActive
            if (body is MemberExpression meBoolean && meBoolean.Type == typeof(bool))
            {
                info = new WhereInfo(meBoolean.Member.Name, true);
                return true;
            }
            if (body is BinaryExpression be && be.NodeType == ExpressionType.Equal && be.Left is MemberExpression me)
            {
                // Only accept ConstantExpression or simple MemberExpression.
                // Never compile and execute arbitrary expressions (would be an RCE vulnerability).
                // Complex expressions fall back to the safe ExpressionToSqlVisitor.
                if (!TryGetSimpleValue(be.Right, out var value))
                    return false;

                info = new WhereInfo(me.Member.Name, value!);
                return true;
            }
            return false;
        }
        /// <summary>
        /// REFACTOR (TASK 19): Use shared ExpressionValueExtractor utility.
        /// Eliminates duplicate logic and ensures consistent behavior across the codebase.
        /// </summary>
        private static bool TryGetSimpleValue(Expression expr, out object? value)
            => ExpressionValueExtractor.TryGetConstantValue(expr, out value);

        private static bool IsSimpleTakePattern(Expression expr, out int? takeCount)
        {
            takeCount = null;
            if (expr is MethodCallExpression takeCall && takeCall.Method.Name == nameof(Queryable.Take))
            {
                if (takeCall.Arguments[1] is ConstantExpression ce && Unwrap(takeCall.Arguments[0]) is ConstantExpression)
                {
                    takeCount = (int)ce.Value!;
                    return true;
                }
            }
            return false;
        }
        private static Expression Unwrap(Expression e)
        {
            while (e is MethodCallExpression m)
            {
                if (m.Method.Name == "AsNoTracking" && m.Arguments.Count == 1)
                {
                    e = m.Arguments[0];
                    continue;
                }
                break;
            }
            return e;
        }
        /// <summary>
        /// Returns a cached sync materializer delegate for the given entity type.
        /// </summary>
        private static Func<System.Data.Common.DbDataReader, T> GetSyncMaterializer<T>(DbContext ctx) where T : class
        {
            var key = (typeof(T), ctx.GetMappingHash());
            return (Func<System.Data.Common.DbDataReader, T>)_syncMaterializerCache.GetOrAdd(key, _ =>
            {
                var map = ctx.GetMapping(typeof(T));
                return (Delegate)_materializer.CreateSyncMaterializer<T>(map);
            });
        }

        private static string GetSqlTemplate<T>(DbContext ctx) where T : class
        {
            var type = typeof(T);
            return ctx.FastPathSqlCache.GetOrAdd(type, static (t, context) =>
            {
                var map = context.GetMapping(t);
                var cols = string.Join(", ", map.Columns.Select(c => c.EscCol));
                return $"SELECT {cols} FROM {map.EscTable}";
            }, ctx);
        }
        private static string ApplyLimit(string sql, int limit, DatabaseProvider provider)
        {
            if (provider.UsesFetchOffsetPaging)
                return sql.Replace("SELECT ", $"SELECT TOP({limit}) ");
            return sql + $" LIMIT {limit}";
        }
        /// <summary>
        /// Non-async entry point — does SQL lookup and command setup synchronously,
        /// then dispatches to async materialization. Avoids one async state machine allocation.
        /// </summary>
        private static Task<object> ExecuteSimpleWhereAsObject<T>(DbContext ctx, WhereInfo info, int? takeCount, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            if (!map.ColumnsByName.TryGetValue(info.Property, out var column))
                throw new InvalidOperationException("Fast path failed - unknown column");

            // Cache full SQL (SELECT + WHERE + LIMIT) using ValueTuple key to avoid string allocation
            bool isNull = info.Value == null || info.Value == DBNull.Value;
            bool isBoolTrue = !isNull && info.Value is bool bv2 && bv2;
            bool isBoolFalse = !isNull && info.Value is bool bv3 && !bv3;
            string whereKind = isNull ? "N" : isBoolTrue ? "BT" : isBoolFalse ? "BF" : "P";
            var cacheKey = (typeof(T).FullName!, info.Property, whereKind, takeCount,
                            ctx.Provider.GetType().FullName!, ctx.GetMappingHash());

            if (!_fullSqlCache.TryGetValue(cacheKey, out var sql))
            {
                sql = GetSqlTemplate<T>(ctx);
                if (isNull)
                    sql += $" WHERE {column.EscCol} IS NULL";
                else if (isBoolTrue)
                    sql += $" WHERE {column.EscCol} = {ctx.Provider.BooleanTrueLiteral}";
                else if (isBoolFalse)
                    sql += $" WHERE {column.EscCol} = {ctx.Provider.BooleanFalseLiteral}";
                else
                    sql += $" WHERE {column.EscCol} = {ctx.Provider.ParamPrefix}p0";
                if (takeCount.HasValue)
                    sql = ApplyLimit(sql, takeCount.Value, ctx.Provider);
                _fullSqlCache[cacheKey] = sql;
            }

            // Skip EnsureConnectionAsync await when connection is already ready
            var ensureTask = ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteSimpleWhereSlowAsync<T>(ensureTask, ctx, sql, info, isNull, isBoolTrue, isBoolFalse, takeCount, ct);

            var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value!);

            // Sync materialization for providers without true async I/O
            if (ctx.Provider.PrefersSyncExecution)
                return ExecuteSimpleWhereMaterializeWithOwnedAsync<T>(cmd, ctx, takeCount, ct, sync: true);

            return ExecuteSimpleWhereMaterializeAsync<T>(cmd, ctx, takeCount, ct);
        }

        private static async Task<object> ExecuteSimpleWhereSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, WhereInfo info, bool isNull, bool isBoolTrue, bool isBoolFalse, int? takeCount, CancellationToken ct) where T : class, new()
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value!);
            var results = new List<T>(takeCount ?? 16);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));
            // Load owned collections (OwnsMany) if configured
            var map = ctx.GetMapping(typeof(T));
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        /// <summary>PERF: Fully sync materialization — no async state machine overhead for SQLite.</summary>
        private static Task<object> ExecuteSimpleWhereMaterializeSync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount) where T : class, new()
        {
            var results = new List<T>(takeCount ?? 16);
            var materializer = GetSyncMaterializer<T>(ctx);
            using var command = cmd;
            using var reader = command.ExecuteReader();
            while (reader.Read())
                results.Add(materializer(reader));
            return Task.FromResult<object>(results);
        }

        /// <summary>Materializes WHERE results and loads owned collections (sync read + async owned-collection load).</summary>
        private static async Task<object> ExecuteSimpleWhereMaterializeWithOwnedAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, CancellationToken ct, bool sync) where T : class, new()
        {
            var results = new List<T>(takeCount ?? 16);
            var materializer = GetSyncMaterializer<T>(ctx);
            using var command = cmd;
            if (sync)
            {
                using var reader = command.ExecuteReader();
                while (reader.Read())
                    results.Add(materializer(reader));
            }
            else
            {
                await using var asyncReader = await command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, ct).ConfigureAwait(false);
                while (await asyncReader.ReadAsync(ct).ConfigureAwait(false))
                    results.Add(materializer(asyncReader));
            }
            // Load owned collections (OwnsMany) if configured
            var map = ctx.GetMapping(typeof(T));
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        private static async Task<object> ExecuteSimpleWhereMaterializeAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, CancellationToken ct) where T : class, new()
        {
            var results = new List<T>(takeCount ?? 16);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var command = cmd;
            await using var reader = await command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));

            // Load owned collections (OwnsMany) if configured
            var map = ctx.GetMapping(typeof(T));
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);

            return results;
        }
        /// <summary>
        /// Single async method — eliminates the extra async state machine from a wrapper approach.
        /// </summary>
        private static async Task<object> ExecuteSimpleTakeAsObject<T>(DbContext ctx, int? takeCount, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            string sql = GetSqlTemplate<T>(ctx);
            if (takeCount.HasValue)
            {
                sql = ApplyLimit(sql, takeCount.Value, ctx.Provider);
            }
            await ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            var results = new List<T>();
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.Default, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                results.Add(materializer(reader));
            }
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
            return (object)Convert.ToInt32(result);
        }
    }
}
