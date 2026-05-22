using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#nullable enable
namespace nORM.Query
{
    internal static class FastPathQueryExecutor
    {
        private readonly record struct WhereInfo(string Property, object? Value);
        private readonly record struct PredicateInfo(string Property, ExpressionType Operation, object? Value);
        private sealed record ComplexQueryInfo(PredicateInfo[] Predicates, string? OrderProperty, bool OrderDescending, int? SkipCount, int? TakeCount);

        /// <summary>
        /// Cached delegates to avoid MakeGenericMethod and Invoke on every query,
        /// eliminating the reflection overhead in the fast path.
        /// </summary>
        private delegate bool TryExecuteDelegate(Expression expr, DbContext ctx, CancellationToken ct, out Task<object> result);
        private delegate bool TryExecuteListDelegate(Expression expr, DbContext ctx, CancellationToken ct, out object result);
        private static readonly ConcurrentDictionary<Type, TryExecuteDelegate> _cachedExecutors = new();
        private static readonly ConcurrentDictionary<Type, TryExecuteListDelegate> _cachedListExecutors = new();

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
        private static readonly ConcurrentDictionary<string, string> _pageSqlCache = new();
        private static readonly ConcurrentLruCache<ExpressionFingerprint, bool> _unsupportedListFastPathCache =
            new(maxSize: 2_048, timeToLive: TimeSpan.FromMinutes(30));

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

        public static bool TryExecuteListNonGeneric(Type elementType, Expression expr, DbContext ctx, CancellationToken ct, out object result)
        {
            result = default!;

            if (!_eligibleTypeCache.GetOrAdd(elementType, static t => t.IsClass && t.GetConstructor(Type.EmptyTypes) != null))
                return false;

            var executor = _cachedListExecutors.GetOrAdd(elementType, t =>
            {
                var method = typeof(FastPathQueryExecutor)
                    .GetMethod(nameof(TryExecuteList), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(t);

                return (TryExecuteListDelegate)Delegate.CreateDelegate(typeof(TryExecuteListDelegate), method);
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

        public static bool TryExecuteList<T>(Expression expr, DbContext ctx, CancellationToken ct, out object result) where T : class, new()
        {
            result = default!;
            if (ctx.Options.GlobalFilters.Count > 0 || ctx.Options.TenantProvider != null)
                return false;

            var cacheUnsupportedMiss = ShouldCacheUnsupportedListMiss(expr);
            var unsupportedKey = default(ExpressionFingerprint);
            if (cacheUnsupportedMiss)
            {
                unsupportedKey = BuildUnsupportedListMissKey<T>(expr, ctx);
                if (_unsupportedListFastPathCache.TryGet(unsupportedKey, out _))
                    return false;
            }

            if (IsSimpleWherePattern(expr, out var whereInfo, out var takeCount))
            {
                result = ExecuteSimpleWhereList<T>(ctx, whereInfo, takeCount, ct);
                return true;
            }
            if (IsSimpleTakePattern(expr, out takeCount))
            {
                result = ExecuteSimpleTakeList<T>(ctx, takeCount, ct);
                return true;
            }
            if (ctx.Options.CommandInterceptors.Count == 0 &&
                IsFilteredOrderedPagePattern(expr, out var pageInfo))
            {
                result = ExecuteFilteredOrderedPageList<T>(ctx, pageInfo, ct);
                return true;
            }
            if (cacheUnsupportedMiss)
                _unsupportedListFastPathCache.Set(unsupportedKey, true);
            return false;
        }

        private static ExpressionFingerprint BuildUnsupportedListMissKey<T>(Expression expr, DbContext ctx) where T : class
            => ExpressionFingerprint.ComputeForPlanCache(expr)
                .Extend(ctx.Provider.GetType().GetHashCode(), ctx.GetMappingHash(), typeof(T).GetHashCode(),
                    expr.Type.GetHashCode(), ctx.Options.CommandInterceptors.Count);

        private static bool ShouldCacheUnsupportedListMiss(Expression expr)
        {
            while (true)
            {
                expr = Unwrap(expr);
                if (expr is not MethodCallExpression call)
                    return false;

                if (call.Method.DeclaringType != typeof(Queryable))
                    return true;

                switch (call.Method.Name)
                {
                    case nameof(Queryable.Where):
                    case nameof(Queryable.Take):
                    case nameof(Queryable.Skip):
                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                        expr = call.Arguments[0];
                        continue;
                    default:
                        return true;
                }
            }
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
                // StripQuotes: LINQ wraps lambda args in UnaryExpression{Quote()}.
                if (StripQuotes(countCall.Arguments[1]) is not LambdaExpression) return false;
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
            // Unwrap AsNoTracking/AsSplitQuery between Take and Where so that
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

                info = new WhereInfo(me.Member.Name, value);
                return true;
            }
            return false;
        }
        /// <summary>
        /// Delegates to shared <see cref="ExpressionValueExtractor"/> utility.
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

        private static bool IsFilteredOrderedPagePattern(Expression expr, out ComplexQueryInfo info)
        {
            info = default!;
            var predicates = new List<PredicateInfo>(4);
            string? orderProperty = null;
            bool orderDescending = false;
            int? skipCount = null;
            int? takeCount = null;
            var sawPagingOrOrdering = false;

            while (expr is MethodCallExpression call)
            {
                if (call.Method.Name is "AsNoTracking" or "AsSplitQuery" && call.Arguments.Count == 1)
                {
                    expr = call.Arguments[0];
                    continue;
                }

                if (call.Method.DeclaringType != typeof(Queryable))
                    return false;

                switch (call.Method.Name)
                {
                    case nameof(Queryable.Take):
                        if (takeCount.HasValue || call.Arguments[1] is not ConstantExpression takeConst || takeConst.Value is not int take || take < 0)
                            return false;
                        takeCount = take;
                        sawPagingOrOrdering = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.Skip):
                        if (skipCount.HasValue || call.Arguments[1] is not ConstantExpression skipConst || skipConst.Value is not int skip || skip < 0)
                            return false;
                        skipCount = skip;
                        sawPagingOrOrdering = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                        if (orderProperty != null)
                            return false;
                        if (StripQuotes(call.Arguments[1]) is not LambdaExpression orderLambda ||
                            orderLambda.Body is not MemberExpression orderMember)
                            return false;
                        orderProperty = orderMember.Member.Name;
                        orderDescending = call.Method.Name == nameof(Queryable.OrderByDescending);
                        sawPagingOrOrdering = true;
                        expr = call.Arguments[0];
                        break;

                    case nameof(Queryable.Where):
                        if (predicates.Count > 0)
                            return false;
                        if (StripQuotes(call.Arguments[1]) is not LambdaExpression whereLambda ||
                            !TryCollectPredicates(whereLambda.Body, predicates))
                            return false;
                        expr = call.Arguments[0];
                        break;

                    default:
                        return false;
                }
            }

            expr = Unwrap(expr);
            if (expr is not ConstantExpression || predicates.Count == 0 || !sawPagingOrOrdering)
                return false;

            info = new ComplexQueryInfo(predicates.ToArray(), orderProperty, orderDescending, skipCount, takeCount);
            return true;
        }

        private static bool TryCollectPredicates(Expression expression, List<PredicateInfo> predicates)
        {
            if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
            {
                return TryCollectPredicates(and.Left, predicates) &&
                       TryCollectPredicates(and.Right, predicates);
            }

            if (expression is MemberExpression boolMember && boolMember.Type == typeof(bool))
            {
                predicates.Add(new PredicateInfo(boolMember.Member.Name, ExpressionType.Equal, true));
                return true;
            }

            if (expression is UnaryExpression { NodeType: ExpressionType.Not, Operand: MemberExpression negatedMember } &&
                negatedMember.Type == typeof(bool))
            {
                predicates.Add(new PredicateInfo(negatedMember.Member.Name, ExpressionType.Equal, false));
                return true;
            }

            if (expression is not BinaryExpression binary)
                return false;

            if (binary.NodeType is not (ExpressionType.Equal or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                or ExpressionType.LessThan or ExpressionType.LessThanOrEqual))
                return false;

            if (binary.Left is not MemberExpression member)
                return false;

            if (!TryGetSimpleValue(binary.Right, out var value))
                return false;
            if ((value == null || value == DBNull.Value) && binary.NodeType != ExpressionType.Equal)
                return false;

            predicates.Add(new PredicateInfo(member.Member.Name, binary.NodeType, value));
            return true;
        }

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

        private static string BuildFilteredOrderedPageCacheKey<T>(ComplexQueryInfo info, DbContext ctx) where T : class
        {
            var key = new StringBuilder(typeof(T).FullName);
            key.Append('|').Append(ctx.Provider.GetType().FullName).Append('|').Append(ctx.GetMappingHash());
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
                         !ctx.Provider.ParameterizeFastPathBooleanPredicates)
                {
                    sql.Append(ctx.Provider.FormatBooleanPredicate(column.EscCol, boolValue));
                }
                else
                {
                    sql.Append(column.EscCol).Append(' ').Append(SqlOperator(predicate.Operation)).Append(' ')
                        .Append(ctx.Provider.ParamPrefix).Append('p').Append(paramIndex++);
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

            if (ctx.Provider.UsesFetchOffsetPaging)
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

        private static Task<List<T>> ExecuteFilteredOrderedPageList<T>(DbContext ctx, ComplexQueryInfo info, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            var cacheKey = BuildFilteredOrderedPageCacheKey<T>(info, ctx);
            var sql = _pageSqlCache.GetOrAdd(cacheKey, static (_, state) =>
                BuildFilteredOrderedPageSql<T>(state.Context, state.Mapping, state.Info), (Context: ctx, Mapping: map, Info: info));

            var ensureTask = ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteFilteredOrderedPageListSlowAsync<T>(ensureTask, ctx, sql, info, map, ct);

            var timeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (ctx.Provider.SupportsFastPathPreparedCommandCache &&
                ctx.Options.CommandInterceptors.Count == 0 &&
                ctx.CurrentTransaction == null)
            {
                var prepared = ctx.GetOrCreateFastPathPreparedCommand(
                    "page:" + cacheKey,
                    sql,
                    timeout,
                    command => BindFilteredOrderedPageParameters(command, ctx, info));
                return ExecuteFilteredOrderedPagePreparedListAsync<T>(prepared, ctx, info, map, ct);
            }

            var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = timeout;
            BindFilteredOrderedPageParameters(cmd, ctx, info);

            if (ctx.Provider.PrefersSyncFastPathExecution)
                return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, info.TakeCount, map, ct, sync: true);

            return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, info.TakeCount, map, ct, sync: false);
        }

        private static async Task<List<T>> ExecuteFilteredOrderedPageListSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, ComplexQueryInfo info, TableMapping map, CancellationToken ct) where T : class, new()
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

            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        private static async Task<List<T>> ExecuteFilteredOrderedPagePreparedListAsync<T>(DbContext.FastPathPreparedCommand prepared, DbContext ctx, ComplexQueryInfo info, TableMapping map, CancellationToken ct) where T : class, new()
        {
            await prepared.Gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cmd = prepared.Command;
                UpdateFilteredOrderedPageParameters(cmd, ctx, info);

                var results = new List<T>(info.TakeCount ?? QueryExecutor.DefaultListCapacity);
                var materializer = GetSyncMaterializer<T>(ctx);
                if (ctx.Provider.PrefersSyncFastPathExecution)
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
                    !ctx.Provider.ParameterizeFastPathBooleanPredicates)
                    continue;

                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p" + paramIndex++, predicate.Value);
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
                    !ctx.Provider.ParameterizeFastPathBooleanPredicates)
                    continue;

                ParameterAssign.AssignValue(cmd.Parameters[paramIndex++], predicate.Value);
            }
        }
        /// <summary>
        /// Unwraps a <see cref="UnaryExpression"/> with <see cref="ExpressionType.Quote"/> node type
        /// to extract the inner lambda expression.
        /// </summary>
        private static Expression StripQuotes(Expression e)
            => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

        private static Expression Unwrap(Expression e)
        {
            while (e is MethodCallExpression m)
            {
                if (m.Method.Name is "AsNoTracking" or "AsSplitQuery" && m.Arguments.Count == 1)
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
                throw new InvalidOperationException($"Fast path failed: column '{info.Property}' not found in mapping for '{typeof(T).Name}'.");

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
                    sql += $" WHERE {ctx.Provider.FormatBooleanPredicate(column.EscCol, expectedValue: true)}";
                else if (isBoolFalse)
                    sql += $" WHERE {ctx.Provider.FormatBooleanPredicate(column.EscCol, expectedValue: false)}";
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
            if (ctx.Provider.PrefersSyncFastPathExecution)
                return ExecuteSimpleWhereMaterializeWithOwnedAsync<T>(cmd, ctx, takeCount, ct, sync: true);

            return ExecuteSimpleWhereMaterializeAsync<T>(cmd, ctx, takeCount, ct);
        }

        private static Task<List<T>> ExecuteSimpleWhereList<T>(DbContext ctx, WhereInfo info, int? takeCount, CancellationToken ct) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            if (!map.ColumnsByName.TryGetValue(info.Property, out var column))
                throw new InvalidOperationException($"Fast path failed: column '{info.Property}' not found in mapping for '{typeof(T).Name}'.");

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
                    sql += $" WHERE {ctx.Provider.FormatBooleanPredicate(column.EscCol, expectedValue: true)}";
                else if (isBoolFalse)
                    sql += $" WHERE {ctx.Provider.FormatBooleanPredicate(column.EscCol, expectedValue: false)}";
                else
                    sql += $" WHERE {column.EscCol} = {ctx.Provider.ParamPrefix}p0";
                if (takeCount.HasValue)
                    sql = ApplyLimit(sql, takeCount.Value, ctx.Provider);
                _fullSqlCache[cacheKey] = sql;
            }

            var ensureTask = ctx.EnsureConnectionAsync(ct);
            if (!ensureTask.IsCompletedSuccessfully)
                return ExecuteSimpleWhereListSlowAsync<T>(ensureTask, ctx, sql, info, isNull, isBoolTrue, isBoolFalse, takeCount, map, ct);

            var timeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (ctx.Provider.SupportsFastPathPreparedCommandCache &&
                ctx.Options.CommandInterceptors.Count == 0 &&
                ctx.CurrentTransaction == null)
            {
                var prepared = ctx.GetOrCreateFastPathPreparedCommand(
                    "simple:" + cacheKey,
                    sql,
                    timeout,
                    command =>
                    {
                        if (!isNull && !isBoolTrue && !isBoolFalse)
                            command.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value!);
                    });
                return ExecuteSimpleWherePreparedListAsync<T>(prepared, ctx, info, isNull, isBoolTrue, isBoolFalse, takeCount, map, ct);
            }

            var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = timeout;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value!);

            if (ctx.Provider.PrefersSyncFastPathExecution)
                return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, takeCount, map, ct, sync: true);

            return ExecuteSimpleWhereListMaterializeAsync<T>(cmd, ctx, takeCount, map, ct, sync: false);
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
            CancellationToken ct) where T : class, new()
        {
            await prepared.Gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cmd = prepared.Command;
                if (!isNull && !isBoolTrue && !isBoolFalse)
                    ParameterAssign.AssignValue(cmd.Parameters[0], info.Value);

                var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
                var materializer = GetSyncMaterializer<T>(ctx);
                if (ctx.Provider.PrefersSyncFastPathExecution)
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

                if (map.OwnedCollections.Count > 0 && results.Count > 0)
                    await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
                return results;
            }
            finally
            {
                prepared.Gate.Release();
            }
        }

        private static async Task<object> ExecuteSimpleWhereSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, WhereInfo info, bool isNull, bool isBoolTrue, bool isBoolFalse, int? takeCount, CancellationToken ct) where T : class, new()
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value!);
            var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));
            // Load owned collections (OwnsMany) if configured
            var map = ctx.GetMapping(typeof(T));
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        /// <summary>Materializes WHERE results and loads owned collections (sync read + async owned-collection load).</summary>
        private static async Task<object> ExecuteSimpleWhereMaterializeWithOwnedAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, CancellationToken ct, bool sync) where T : class, new()
        {
            var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
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
            // Load owned collections (OwnsMany) if configured
            var map = ctx.GetMapping(typeof(T));
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        private static async Task<object> ExecuteSimpleWhereMaterializeAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, CancellationToken ct) where T : class, new()
        {
            var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var command = cmd;
            await using var reader = await command.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));

            // Load owned collections (OwnsMany) if configured
            var map = ctx.GetMapping(typeof(T));
            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);

            return results;
        }

        private static async Task<List<T>> ExecuteSimpleWhereListSlowAsync<T>(Task<System.Data.Common.DbConnection> ensureTask, DbContext ctx, string sql, WhereInfo info, bool isNull, bool isBoolTrue, bool isBoolFalse, int? takeCount, TableMapping map, CancellationToken ct) where T : class, new()
        {
            await ensureTask.ConfigureAwait(false);
            await using var cmd = ctx.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            if (!isNull && !isBoolTrue && !isBoolFalse)
                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value!);

            var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                results.Add(materializer(reader));

            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }

        private static async Task<List<T>> ExecuteSimpleWhereListMaterializeAsync<T>(System.Data.Common.DbCommand cmd, DbContext ctx, int? takeCount, TableMapping map, CancellationToken ct, bool sync) where T : class, new()
        {
            var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
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

            if (map.OwnedCollections.Count > 0 && results.Count > 0)
                await ctx.LoadOwnedCollectionsAsync(results.Cast<object>().ToList(), map, ct).ConfigureAwait(false);
            return results;
        }
        /// <summary>
        /// Single async method — eliminates the extra async state machine from a wrapper approach.
        /// </summary>
        private static async Task<object> ExecuteSimpleTakeAsObject<T>(DbContext ctx, int? takeCount, CancellationToken ct) where T : class, new()
            => await ExecuteSimpleTakeList<T>(ctx, takeCount, ct).ConfigureAwait(false);

        private static async Task<List<T>> ExecuteSimpleTakeList<T>(DbContext ctx, int? takeCount, CancellationToken ct) where T : class, new()
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
            var results = new List<T>(takeCount ?? QueryExecutor.DefaultListCapacity);
            var materializer = GetSyncMaterializer<T>(ctx);
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(ctx, CommandBehavior.SingleResult, ct).ConfigureAwait(false);
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
            // Guard against null/DBNull from empty tables or provider-specific edge cases
            if (result == null || result is DBNull)
                return (object)0;
            return (object)Convert.ToInt32(result);
        }
    }
}
