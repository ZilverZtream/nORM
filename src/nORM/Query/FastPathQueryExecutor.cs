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
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("FastPathQueryExecutor builds materializers via reflection; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("FastPathQueryExecutor reflects over entity types; trimming may remove the required members.")]
    internal static partial class FastPathQueryExecutor
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
        private delegate bool TryExecuteFirstDelegate(Expression expr, DbContext ctx, CancellationToken ct, bool throwOnEmpty, out Task<object> result);
        private static readonly ConcurrentDictionary<Type, TryExecuteDelegate> _cachedExecutors = new();
        private static readonly ConcurrentDictionary<Type, TryExecuteListDelegate> _cachedListExecutors = new();
        private static readonly ConcurrentDictionary<Type, TryExecuteFirstDelegate> _cachedFirstExecutors = new();

        /// <summary>
        /// Singleton MaterializerFactory - it only wraps static caches, no instance state.
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

            // Cached check - avoids GetConstructor reflection on every call for ineligible types
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

        /// <summary>
        /// Non-generic entry point for the scalar First/FirstOrDefault fast path — the single most common
        /// query shape (a point read). Without it First/FirstOrDefault fall to the higher-overhead simple-query
        /// path even though the underlying WHERE ... LIMIT 1 is the same pooled/prepared read the list fast path
        /// already runs.
        /// </summary>
        public static bool TryExecuteFirstNonGeneric(Type elementType, Expression expr, DbContext ctx, bool throwOnEmpty, CancellationToken ct, out Task<object> result)
        {
            result = default!;
            if (!_eligibleTypeCache.GetOrAdd(elementType, static t => t.IsClass && t.GetConstructor(Type.EmptyTypes) != null))
                return false;

            var executor = _cachedFirstExecutors.GetOrAdd(elementType, t =>
            {
                var method = typeof(FastPathQueryExecutor)
                    .GetMethod(nameof(TryExecuteFirst), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(t);

                return (TryExecuteFirstDelegate)Delegate.CreateDelegate(typeof(TryExecuteFirstDelegate), method);
            });

            return executor(expr, ctx, ct, throwOnEmpty, out result);
        }

        /// <summary>
        /// Fast path for <c>First()</c> / <c>FirstOrDefault()</c> and their predicate-lowered form
        /// <c>First(Where(source, col == value))</c>: emits WHERE ... LIMIT 1, materializes the one row through
        /// the same pooled/prepared/cached machinery the list fast path uses, and returns the single element
        /// (or default / an empty-sequence throw). Falls back for any shape it does not recognize.
        /// </summary>
        public static bool TryExecuteFirst<T>(Expression expr, DbContext ctx, CancellationToken ct, bool throwOnEmpty, out Task<object> result) where T : class, new()
        {
            result = default!;
            if (ctx.Options.GlobalFilters.Count > 0 || ctx.Options.TenantProvider != null || ctx.Options.RetryPolicy != null)
                return false;
            if (QueryTranslator.FindRootRawSource(expr) is not null)
                return false;
            if (ContainsTagWith(expr))
                return false;
            if (ContainsAsTracking(expr))
                return false;
            if (ctx.GetMapping(typeof(T)).DiscriminatorValue != null)
                return false;

            if (expr is not MethodCallExpression firstCall || firstCall.Arguments.Count != 1)
                return false;
            var inner = firstCall.Arguments[0];

            // First/FirstOrDefault(source): no filter, one row.
            if (Unwrap(inner) is ConstantExpression)
            {
                result = ExecuteWhereFirstAsObject<T>(ctx, default, hasFilter: false, ShouldTrackResults<T>(expr, ctx), throwOnEmpty, ct);
                return true;
            }
            // First/FirstOrDefault(Where(source, singleColumnEquality)): filtered, one row. Reject an inner Take
            // (First(Take(Where,n)) with n==0 must be empty, which LIMIT 1 alone would not honor).
            if (IsSimpleWherePattern(inner, ctx.RawProvider.StoresDecimalAsText, out var whereInfo, out var innerTake) && innerTake == null)
            {
                if (!ctx.GetMapping(typeof(T)).ColumnsByName.ContainsKey(whereInfo.Property))
                    return false;
                result = ExecuteWhereFirstAsObject<T>(ctx, whereInfo, hasFilter: true, ShouldTrackResults<T>(expr, ctx), throwOnEmpty, ct);
                return true;
            }
            return false;
        }

        public static bool TryExecute<T>(Expression expr, DbContext ctx, CancellationToken ct, out Task<object> result) where T : class, new()
        {
            result = default!;
            // RetryPolicy bail: the fast path executes OUTSIDE RetryingExecutionStrategy, so a
            // transient failure here would surface without any retry - the policy would be
            // silently dead for exactly the most common queries. Fall back to the full
            // pipeline, which wraps execution in the strategy.
            if (ctx.Options.GlobalFilters.Count > 0 || ctx.Options.TenantProvider != null || ctx.Options.RetryPolicy != null)
                return false;
            // A FromSqlRaw/FromSqlInterpolated root supplies its own FROM; every fast path below selects from the
            // mapped table and would silently ignore the raw SQL (and its filter). Defer to the full pipeline.
            if (QueryTranslator.FindRootRawSource(expr) is not null)
                return false;
            // TagWith injects a SQL comment the fast path does not emit; defer to the full pipeline.
            if (ContainsTagWith(expr))
                return false;
            // AsTracking overrides a NoTracking context default; the fast path's ShouldTrackResults reads
            // only the default, so defer to the full pipeline which honors the per-query force-tracking flag.
            if (ContainsAsTracking(expr))
                return false;
            // TPH subtype root: the fast-path SQL builders below select/count over the shared base table
            // and emit NO discriminator predicate, so a subtype query would silently return and count
            // sibling subtypes. Defer to the full pipeline, which appends ({disc} = value) for a subtype.
            if (ctx.GetMapping(typeof(T)).DiscriminatorValue != null)
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
            if (IsSimpleWherePattern(expr, ctx.RawProvider.StoresDecimalAsText, out var whereInfo, out var takeCount))
            {
                var map = ctx.GetMapping(typeof(T));
                if (!map.ColumnsByName.ContainsKey(whereInfo.Property))
                    return false;

                // Use async/await wrapper instead of ContinueWith to avoid closure allocation
                result = ExecuteSimpleWhereAsObject<T>(ctx, whereInfo, takeCount, ShouldTrackResults<T>(expr, ctx), ct);
                return true;
            }
            if (IsSimpleTakePattern(expr, out takeCount))
            {
                // Use async/await wrapper instead of ContinueWith to avoid closure allocation
                result = ExecuteSimpleTakeAsObject<T>(ctx, takeCount, ShouldTrackResults<T>(expr, ctx), ct);
                return true;
            }
            return false;
        }

        public static bool TryExecuteList<T>(Expression expr, DbContext ctx, CancellationToken ct, out object result) where T : class, new()
        {
            result = default!;
            // RetryPolicy bail: the fast path executes OUTSIDE RetryingExecutionStrategy, so a
            // transient failure here would surface without any retry - the policy would be
            // silently dead for exactly the most common queries. Fall back to the full
            // pipeline, which wraps execution in the strategy.
            if (ctx.Options.GlobalFilters.Count > 0 || ctx.Options.TenantProvider != null || ctx.Options.RetryPolicy != null)
                return false;
            // A FromSqlRaw/FromSqlInterpolated root supplies its own FROM; every fast path below selects from the
            // mapped table and would silently ignore the raw SQL (and its filter). Defer to the full pipeline.
            if (QueryTranslator.FindRootRawSource(expr) is not null)
                return false;
            // TagWith injects a SQL comment the fast path does not emit; defer to the full pipeline.
            if (ContainsTagWith(expr))
                return false;
            // AsTracking overrides a NoTracking context default; the fast path's ShouldTrackResults reads
            // only the default, so defer to the full pipeline which honors the per-query force-tracking flag.
            if (ContainsAsTracking(expr))
                return false;
            // TPH subtype root: the fast-path SQL builders below select/page over the shared base table
            // and emit NO discriminator predicate, so a subtype query would silently return sibling
            // subtypes (and materialize them as the wrong type). Defer to the full pipeline.
            if (ctx.GetMapping(typeof(T)).DiscriminatorValue != null)
                return false;

            var cacheUnsupportedMiss = ShouldCacheUnsupportedListMiss(expr);
            var unsupportedKey = default(ExpressionFingerprint);
            if (cacheUnsupportedMiss)
            {
                unsupportedKey = BuildUnsupportedListMissKey<T>(expr, ctx);
                if (_unsupportedListFastPathCache.TryGet(unsupportedKey, out _))
                    return false;
            }

            if (IsSimpleWherePattern(expr, ctx.RawProvider.StoresDecimalAsText, out var whereInfo, out var takeCount))
            {
                var map = ctx.GetMapping(typeof(T));
                if (!map.ColumnsByName.ContainsKey(whereInfo.Property))
                    return false;

                result = ExecuteSimpleWhereList<T>(ctx, whereInfo, takeCount, ShouldTrackResults<T>(expr, ctx), ct);
                return true;
            }
            if (IsSimpleTakePattern(expr, out takeCount))
            {
                result = ExecuteSimpleTakeList<T>(ctx, takeCount, ShouldTrackResults<T>(expr, ctx), ct);
                return true;
            }
            if (ctx.Options.CommandInterceptors.Count == 0 &&
                IsFilteredOrderedPagePattern(expr, ctx.RawProvider.StoresDecimalAsText, out var pageInfo))
            {
                var map = ctx.GetMapping(typeof(T));
                if (!HasFilteredOrderedPageColumns(map, pageInfo))
                    return false;

                result = ExecuteFilteredOrderedPageList<T>(ctx, pageInfo, ShouldTrackResults<T>(expr, ctx), ct);
                return true;
            }
            if (cacheUnsupportedMiss)
                _unsupportedListFastPathCache.Set(unsupportedKey, true);
            return false;
        }

        private static ExpressionFingerprint BuildUnsupportedListMissKey<T>(Expression expr, DbContext ctx) where T : class
            => ExpressionFingerprint.ComputeForPlanCache(expr)
                .Extend(ctx.RawProvider.GetType().GetHashCode(), ctx.GetMappingHash(), typeof(T).GetHashCode(),
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
                if (m.Method.Name is "AsNoTracking" or "AsNoTrackingWithIdentityResolution" or "AsSplitQuery" && m.Arguments.Count == 1)
                {
                    e = m.Arguments[0];
                    continue;
                }
                break;
            }
            return e;
        }

        /// <summary>True when the query chain contains a TagWith marker anywhere along its source spine.</summary>
        private static bool ContainsTagWith(Expression e)
        {
            while (e is MethodCallExpression m && m.Arguments.Count > 0)
            {
                if (m.Method.Name == "TagWith")
                    return true;
                e = m.Object ?? m.Arguments[0];
            }
            return false;
        }

        /// <summary>True when the query chain contains an AsTracking marker anywhere along its source spine.</summary>
        private static bool ContainsAsTracking(Expression e)
        {
            while (e is MethodCallExpression m && m.Arguments.Count > 0)
            {
                if (m.Method.Name == "AsTracking")
                    return true;
                e = m.Object ?? m.Arguments[0];
            }
            return false;
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
        /// Decides whether fast-path results must join the change tracker, mirroring the full
        /// pipeline's rules: the context default is TrackAll, the element type is a query root,
        /// and the expression chain does not opt out via AsNoTracking.
        /// </summary>
        private static bool ShouldTrackResults<T>(Expression expr, DbContext ctx) where T : class
            => ctx.Options.DefaultTrackingBehavior != QueryTrackingBehavior.NoTracking
               && ctx.IsMapped(typeof(T))
               && !ctx.GetMapping(typeof(T)).IsKeyless   // keyless = query-only, never tracked
               && !HasAsNoTrackingCall(expr);

        private static bool HasAsNoTrackingCall(Expression e)
        {
            while (e is MethodCallExpression m && m.Arguments.Count >= 1)
            {
                if (m.Method.Name is "AsNoTracking" or "AsNoTrackingWithIdentityResolution")
                    return true;
                e = m.Arguments[0];
            }
            return false;
        }

        /// <summary>
        /// Mirrors QueryExecutor.ProcessEntity for fast-path results: materialized query-root
        /// entities join the change tracker exactly like the full pipeline does (identity-map
        /// dedup via the entry's canonical instance, Unchanged snapshot, lazy-loading wiring).
        /// Without this, entities loaded through the fast path were invisible to SaveChanges
        /// and their edits were silently lost.
        /// </summary>
        private static void TrackMaterializedResults<T>(DbContext ctx, TableMapping map, List<T> results) where T : class
        {
            for (int i = 0; i < results.Count; i++)
            {
                var entity = results[i];
                if (entity is null) continue;
                var entry = ctx.ChangeTracker.Track(entity, EntityState.Unchanged, map);
                var tracked = (T)entry.Entity!;
                nORM.Navigation.NavigationPropertyExtensions.EnableLazyLoading(tracked, ctx);
                results[i] = tracked;
            }
        }

        // C# string equality is ordinal; providers whose default collation folds case (MySQL,
        // SQL Server) need the sargable ordinal wrap so the fast path matches the same rows the
        // full translator and LINQ-to-Objects do. Decided by the column's CLR type so the cached
        // SQL stays stable per (entity, property, provider).
        private static string BuildEqualityPredicate(DbContext ctx, Column column)
        {
            var paramName = ctx.RawProvider.ParamPrefix + "p0";
            var colType = Nullable.GetUnderlyingType(column.Prop.PropertyType) ?? column.Prop.PropertyType;
            return colType == typeof(string) && ctx.RawProvider.DefaultStringEqualityIsCaseInsensitive
                ? ctx.RawProvider.OrdinalStringEqualSql(column.EscCol, paramName)
                : $"{column.EscCol} = {paramName}";
        }
    }
}
