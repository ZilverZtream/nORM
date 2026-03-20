using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Execution;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.SourceGeneration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

#pragma warning disable IDE0130

namespace nORM.Query
{
    /// <summary>
    /// Executes <see cref="DbCommand"/> instances and materializes results.
    /// </summary>
    internal sealed class QueryExecutor
    {
        private readonly DbContext _ctx;
        private readonly IncludeProcessor _includeProcessor;
        private readonly NormExceptionHandler _exceptionHandler;
        private readonly ILogger<QueryExecutor> _logger;

        /// <summary>
        /// Default initial capacity for result lists when no Take hint is available.
        /// Avoids resize for most small result sets without over-allocating.
        /// </summary>
        internal const int DefaultListCapacity = 16;

        /// <summary>
        /// Number of parameter slots reserved for framework use (tenant filters, OCC tokens, etc.)
        /// when computing maximum batch size for dependent query IN clauses.
        /// Aligned with <see cref="Providers.DatabaseProvider"/> ParameterReserve.
        /// </summary>
        private const int DependentQueryParameterReserve = 100;

        // Cached list factory delegates to avoid Activator.CreateInstance on every materialization.
        // Take values are now passed directly from the query plan rather than parsed via regex.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<int, IList>> _listFactoryCache = new();

        /// <summary>
        /// Singleton MaterializerFactory -- wraps only static caches, no instance state.
        /// Eliminates heap allocation per dependent/group-join query.
        /// </summary>
        private static readonly MaterializerFactory _sharedMaterializerFactory = new();

        /// <summary>
        /// Pre-compiled regex for redacting single-quoted and N-prefixed string literals from SQL.
        /// Covers ANSI SQL <c>'...'</c>, SQL Server <c>N'...'</c>, and escaped <c>''</c> pairs.
        /// Static compilation avoids re-parsing the pattern on every log call.
        /// </summary>
        private static readonly Regex SingleQuoteLiteralRegex = new(@"N?'(?:[^']|'')*'", RegexOptions.Compiled);

        /// <summary>
        /// Pre-compiled regex for redacting PostgreSQL dollar-quoted string literals from SQL.
        /// Handles both bare <c>$$...$$</c> and tagged <c>$tag$...$tag$</c> forms via backreference.
        /// </summary>
        private static readonly Regex DollarQuoteLiteralRegex = new(@"\$(\w*)\$.*?\$\1\$", RegexOptions.Compiled | RegexOptions.Singleline);

        public QueryExecutor(DbContext ctx, IncludeProcessor includeProcessor, ILogger<QueryExecutor>? logger = null)
        {
            _ctx = ctx;
            _includeProcessor = includeProcessor;
            _logger = logger ?? NullLogger<QueryExecutor>.Instance;
            _exceptionHandler = new NormExceptionHandler(_logger);
        }

        /// <summary>
        /// S1: Redacts string literals from SQL before it is written to logs,
        /// preventing sensitive literal values from appearing in log sinks.
        /// Covers:
        ///   'standard single-quoted literals' (ANSI SQL, SQLite, PostgreSQL, MySQL)
        ///   N'national string literals' (SQL Server)
        ///   $$dollar-quoted blocks$$ (PostgreSQL bare dollar-quoting)
        ///   $tag$...$tag$ (PostgreSQL tagged dollar-quoting, e.g. $func$...$func$)
        /// Identifiers, parameter placeholders (@p0), and SQL keywords are preserved.
        /// </summary>
        private static string RedactSqlForLogging(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;
            // N'...' (SQL Server national strings) and '...' (ANSI SQL, including escaped '' pairs).
            var step1 = SingleQuoteLiteralRegex.Replace(sql, "'[redacted]'");
            // Dollar-quote redaction covers both bare $$...$$ (empty tag captured by \w*) and
            // named tags ($func$...$func$, $body$...$body$, etc.) via the backreference \1.
            return DollarQuoteLiteralRegex.Replace(step1, "'[redacted]'");
        }

        /// <summary>
        /// Creates a list using a cached compiled delegate instead of Activator.CreateInstance,
        /// which is significantly faster on hot paths.
        /// </summary>
        /// <param name="elementType">The element type for the list.</param>
        /// <param name="capacity">The initial capacity.</param>
        /// <returns>A new list instance.</returns>
        /// <remarks>Internal wrapper exposed for use by NormQueryProvider's pooled command path.</remarks>
        internal IList CreateListForType(Type elementType, int capacity) => CreateList(elementType, capacity);

        private static IList CreateList(Type elementType, int capacity)
        {
            var factory = _listFactoryCache.GetOrAdd(elementType, t =>
            {
                var listType = typeof(List<>).MakeGenericType(t);
                var ctor = listType.GetConstructor(new[] { typeof(int) });
                if (ctor == null)
                    throw new InvalidOperationException($"No capacity constructor found for {listType}");

                var capacityParam = System.Linq.Expressions.Expression.Parameter(typeof(int), "capacity");
                var newExpr = System.Linq.Expressions.Expression.New(ctor, capacityParam);
                var convertExpr = System.Linq.Expressions.Expression.Convert(newExpr, typeof(IList));
                return System.Linq.Expressions.Expression.Lambda<Func<int, IList>>(convertExpr, capacityParam).Compile();
            });

            return factory(capacity);
        }

        /// <summary>
        /// Materializes directly into <c>List&lt;object&gt;</c> to avoid covariant copy
        /// when the caller needs <c>List&lt;object&gt;</c> but the plan's ElementType is a concrete type.
        /// </summary>
        /// <param name="plan">Query plan describing how to materialize results.</param>
        /// <param name="cmd">Prepared database command.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>A list containing the materialized entities.</returns>
        public async Task<List<object>> MaterializeAsObjectListAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            await using var command = cmd;
            try
            {
                var capacity = plan.SingleResult ? 1 : Math.Max(1, plan.Take ?? DefaultListCapacity);
                var list = new List<object>(capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null &&
                                 _ctx.IsMapped(plan.ElementType);

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;
                bool isReadOnly = IsReadOnlyQuery();

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                var syncMaterializer = plan.SyncMaterializer;

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    ct.ThrowIfCancellationRequested();
                    var entity = syncMaterializer(reader);
                    if (plan.ClientProjection != null)
                        entity = plan.ClientProjection(entity);
                    entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                    list.Add(entity);
                }

                if (plan.SplitQuery)
                {
                    // Convert to IList for EagerLoadAsync compatibility
                    IList iList = list;
                    foreach (var include in plan.Includes)
                        await _includeProcessor.EagerLoadAsync(include, iList, ct, plan.NoTracking).ConfigureAwait(false);
                }

                // M2M eager loading runs unconditionally (no SplitQuery requirement)
                if (plan.M2MIncludes != null && plan.M2MIncludes.Count > 0)
                {
                    IList iList = list;
                    foreach (var m2mPlan in plan.M2MIncludes)
                        await _includeProcessor.LoadManyToManyAsync(m2mPlan, iList, ct, plan.NoTracking).ConfigureAwait(false);
                }

                return list;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MaterializeAsObjectListAsync failed for SQL: {Sql}", RedactSqlForLogging(cmd.CommandText));
                throw;
            }
        }

        public async Task<IList> MaterializeAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            await using var command = cmd;
            // Inline exception handling instead of wrapping in ExecuteWithExceptionHandling.
            // The wrapper allocates: Func<Task<T>> delegate, Stopwatch.StartNew(), Dictionary,
            // and calls LogInformation on EVERY successful query — all pure overhead on the hot path.
            try
            {
                if (plan.GroupJoinInfo != null)
                    return await MaterializeGroupJoinAsync(plan, command, ct).ConfigureAwait(false);

                // List capacity pre-sizing: SingleResult=1, Take=Take value, else DefaultListCapacity heuristic.
                var capacity = plan.SingleResult ? 1 : Math.Max(1, plan.Take ?? DefaultListCapacity);
                var list = CreateList(plan.ElementType, capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null &&
                                 _ctx.IsMapped(plan.ElementType);   // only mapped entity roots

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                // Hoist read-only check out of per-row loop: context options don't change during execution.
                bool isReadOnly = IsReadOnlyQuery();

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                // Use sync materializer to avoid per-row Task allocation.
                var syncMaterializer = plan.SyncMaterializer;

                // Respect SingleResult flag to avoid materializing unnecessary rows.
                // Single/SingleOrDefault must read up to 2 rows to detect duplicate-row violations;
                // First/FirstOrDefault only need 1.
                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                if (plan.SingleResult)
                {
                    for (int _row = 0; _row < maxRows; _row++)
                    {
                        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) break;
                        ct.ThrowIfCancellationRequested();
                        var entity = syncMaterializer(reader);
                        // Apply client-side projection if present (for untranslatable expressions)
                        if (plan.ClientProjection != null)
                        {
                            entity = plan.ClientProjection(entity);
                        }
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
                }
                else
                {
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        ct.ThrowIfCancellationRequested();
                        var entity = syncMaterializer(reader);
                        // Apply client-side projection if present (for untranslatable expressions)
                        if (plan.ClientProjection != null)
                        {
                            entity = plan.ClientProjection(entity);
                        }
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
                }

                if (plan.SplitQuery)
                {
                    foreach (var include in plan.Includes)
                    {
                        await _includeProcessor.EagerLoadAsync(include, list, ct, plan.NoTracking).ConfigureAwait(false);
                    }
                }

                // M2M eager loading runs unconditionally (no SplitQuery requirement)
                if (plan.M2MIncludes != null && plan.M2MIncludes.Count > 0)
                {
                    foreach (var m2mPlan in plan.M2MIncludes)
                        await _includeProcessor.LoadManyToManyAsync(m2mPlan, list, ct, plan.NoTracking).ConfigureAwait(false);
                }

                // Execute dependent queries for nested collections (split query for projections)
                if (plan.DependentQueries != null && plan.DependentQueries.Count > 0)
                {
                    await ExecuteDependentQueriesAsync(plan.DependentQueries, list, plan.NoTracking, ct).ConfigureAwait(false);
                }

                // Load owned collections (OwnsMany) for all materialized entities
                if (entityMap != null && entityMap.OwnedCollections.Count > 0 && list.Count > 0)
                {
                    await LoadOwnedCollectionsAsync(list, entityMap, ct).ConfigureAwait(false);
                }

                return list;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MaterializeAsync failed for SQL: {Sql}", RedactSqlForLogging(command.CommandText));
                throw;
            }
        }

        public IList Materialize(QueryPlan plan, DbCommand cmd)
        {
            using var command = cmd;
            try
            {
                if (plan.GroupJoinInfo != null)
                    return MaterializeGroupJoin(plan, command);

                // List capacity pre-sizing: SingleResult=1, Take=Take value, else DefaultListCapacity heuristic.
                var capacity = plan.SingleResult ? 1 : Math.Max(1, plan.Take ?? DefaultListCapacity);
                var list = CreateList(plan.ElementType, capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null &&
                                 _ctx.IsMapped(plan.ElementType);   // only mapped entity roots

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                // Hoist read-only check out of per-row loop: context options don't change during execution.
                bool isReadOnly = IsReadOnlyQuery();

                using var reader = command.ExecuteReaderWithInterception(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

                // Use sync materializer — no Task allocation, no async state machine.
                var syncMaterializer = plan.SyncMaterializer;

                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                if (plan.SingleResult)
                {
                    for (int _row = 0; _row < maxRows; _row++)
                    {
                        if (!reader.Read()) break;
                        var entity = syncMaterializer(reader);
                        if (plan.ClientProjection != null)
                        {
                            entity = plan.ClientProjection(entity);
                        }
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
                }
                else
                {
                    while (reader.Read())
                    {
                        var entity = syncMaterializer(reader);
                        if (plan.ClientProjection != null)
                        {
                            entity = plan.ClientProjection(entity);
                        }
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
                }

                if (plan.SplitQuery)
                {
                    foreach (var include in plan.Includes)
                    {
                        // Truly synchronous eager load — no GetAwaiter().GetResult().
                        _includeProcessor.EagerLoad(include, list, plan.NoTracking);
                    }
                }

                // M2M eager loading — truly synchronous, no GetAwaiter().GetResult().
                if (plan.M2MIncludes != null && plan.M2MIncludes.Count > 0)
                {
                    foreach (var m2mPlan in plan.M2MIncludes)
                        _includeProcessor.LoadManyToMany(m2mPlan, list, plan.NoTracking);
                }

                // Execute dependent queries for nested collections (split query for projections).
                if (plan.DependentQueries != null && plan.DependentQueries.Count > 0)
                {
                    // Truly synchronous dependent query execution — no GetAwaiter().GetResult().
                    ExecuteDependentQueries(plan.DependentQueries, list, plan.NoTracking);
                }

                return list;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Materialize failed for SQL: {Sql}", RedactSqlForLogging(command.CommandText));
                throw;
            }
        }

        /// <summary>
        /// Processes an entity after materialization, optionally tracking it and enabling lazy loading.
        /// </summary>
        /// <param name="entity">The materialized entity.</param>
        /// <param name="trackable">Whether the entity type is trackable.</param>
        /// <param name="entityMap">The table mapping for the entity.</param>
        /// <param name="isReadOnly">
        /// Whether this is a read-only query, hoisted out of per-row check since context options don't change during execution.
        /// </param>
        /// <returns>The processed entity (may be a tracking proxy).</returns>
        // PERFORMANCE OPTIMIZATION 14: Aggressive inlining for per-row hot path
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private object ProcessEntity(object entity, bool trackable, TableMapping? entityMap, bool isReadOnly)
        {
            if (!trackable)
                return entity;

            // Use pre-computed isReadOnly flag.
            if (isReadOnly)
            {
                return entity; // Skip all tracking setup
            }

            var actualMap = entityMap != null && entity.GetType() == entityMap.Type
                ? entityMap
                : _ctx.GetMapping(entity.GetType());
            var entry = _ctx.ChangeTracker.Track(entity, EntityState.Unchanged, actualMap);
            entity = entry.Entity!;
            NavigationPropertyExtensions.EnableLazyLoading(entity, _ctx);
            return entity;
        }

        private bool IsReadOnlyQuery()
        {
            // Determine if this is a read-only context
            return _ctx.Options.DefaultTrackingBehavior == QueryTrackingBehavior.NoTracking;
        }

        /// <summary>
        /// Materializes the results of a LINQ <c>GroupJoin</c> operation by streaming records from
        /// the provided command and constructing the grouped results in memory.
        /// </summary>
        /// <param name="plan">The query plan describing mappings and selectors.</param>
        /// <param name="cmd">The database command to execute.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>An <see cref="IList"/> containing the grouped join results.</returns>
        private async Task<IList> MaterializeGroupJoinAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var info = plan.GroupJoinInfo!;

            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
            {
                // Use cached list factory instead of Activator.CreateInstance.
                var resultList = CreateList(info.ResultType, DefaultListCapacity);

                var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
                var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

                var outerMap = _ctx.GetMapping(info.OuterType);
                var innerMap = _ctx.GetMapping(info.InnerType);

                var outerColumnCount = outerMap.Columns.Length;
                var innerKeyOffset = Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);
                if (innerKeyOffset < 0)
                    throw new InvalidOperationException(
                        $"GroupJoin inner key column '{info.InnerKeyColumn?.Name ?? "(null)"}' not found in mapping for '{info.InnerType.Name}'.");
                var innerKeyIndex = outerColumnCount + innerKeyOffset;

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                object? currentOuter = null;
                object? currentKey = null;
                List<object> currentChildren = [];

                // Use sync materializer to avoid per-row Task allocation, consistent with MaterializeAsync.
                var syncMaterializer = plan.SyncMaterializer;

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var outer = syncMaterializer(reader);
                    var key = info.OuterKeySelector(outer) ?? DBNull.Value;

                    if (currentOuter == null || !Equals(currentKey, key))
                    {
                        if (currentOuter != null)
                        {
                            var list = CreateListFromItems(info.InnerType, currentChildren);
                            // ResultSelector expects IEnumerable<object>; .Cast<object>() provides the typed wrapper.
                            var result = info.ResultSelector(currentOuter, list.Cast<object>());
                            resultList.Add(result);
                            currentChildren = [];
                        }

                        if (trackOuter)
                        {
                            var actualMap = _ctx.GetMapping(outer.GetType());
                            var entry = _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                            outer = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                        }

                        currentOuter = outer;
                        currentKey = key;
                    }

                    if (!reader.IsDBNull(innerKeyIndex))
                    {
                        // Use configurable MaxGroupJoinSize instead of a hard-coded limit.
                        // Prevents unbounded memory growth while allowing users to opt-in to larger datasets.
                        var maxSize = _ctx.Options.MaxGroupJoinSize;
                        if (currentChildren.Count >= maxSize)
                        {
                            throw new NormQueryException(
                                $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                                "This may indicate a malformed query or incorrect join keys. " +
                                "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                                "Review your GroupJoin operation and ensure join keys are correct.");
                        }

                        var inner = MaterializeEntity(reader, innerMap, outerColumnCount, ct);
                        if (trackInner)
                        {
                            var actualMap = _ctx.GetMapping(inner.GetType());
                            var entry = _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                            inner = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                        }
                        currentChildren.Add(inner);
                    }
                }

                if (currentOuter != null)
                {
                    var list = CreateListFromItems(info.InnerType, currentChildren);
                    // ResultSelector expects IEnumerable<object>; .Cast<object>() provides the typed wrapper.
                    var result = info.ResultSelector(currentOuter, list.Cast<object>());
                    resultList.Add(result);
                }

                return resultList;
            }, "MaterializeGroupJoinAsync", new Dictionary<string, object> { ["Sql"] = RedactSqlForLogging(cmd.CommandText) }).ConfigureAwait(false);

            static object MaterializeEntity(DbDataReader reader, TableMapping map, int offset, CancellationToken ct)
            {
                if (map.DiscriminatorColumn != null && map.TphMappings.Count > 0)
                {
                    var discIndex = offset + Array.IndexOf(map.Columns, map.DiscriminatorColumn);
                    if (!reader.IsDBNull(discIndex))
                    {
                        var disc = reader.GetValue(discIndex);
                        if (disc != null && map.TphMappings.TryGetValue(disc, out var derived))
                            return MaterializeEntity(reader, derived, offset, ct);
                    }
                }

                // Q1/X1 fix: use model-aware (type, tableName) lookup to avoid wrong
                // materializer selection when the same CLR type is registered under
                // multiple table mappings.  Type-only lookup used the [Table]-attribute
                // name, silently bypassing the actual mapping discriminator.
                if (CompiledMaterializerStore.TryGet(map.Type, map.TableName, out var compiled) && offset == 0)
                {
                    return compiled(reader, ct).GetAwaiter().GetResult();
                }

                // Use MaterializerFactory instead of reflection.
                // MaterializerFactory creates compiled IL.Emit/Expression-based materializers.
                var factory = _sharedMaterializerFactory;
                var materializer = factory.CreateSyncMaterializer(map, map.Type, startOffset: offset);
                return materializer(reader);
            }

            static IList CreateListFromItems(Type innerType, List<object> items)
            {
                // PERFORMANCE: Reuse cached list factory to avoid Activator reflection per group
                var list = CreateList(innerType, items.Count);
                foreach (var item in items) list.Add(item);
                return list;
            }
        }

        private IList MaterializeGroupJoin(QueryPlan plan, DbCommand cmd)
        {
            var info = plan.GroupJoinInfo!;

            // SYNC-OVER-ASYNC FIX (Option A): Use truly synchronous code path.
            // Previously this called plan.Materializer(...).GetAwaiter().GetResult() which
            // blocks a thread pool thread.  We now use plan.SyncMaterializer which is a
            // genuine synchronous delegate — no async state machine involved.
            return _exceptionHandler.ExecuteWithExceptionHandlingSync(() =>
            {
                var resultList = CreateList(info.ResultType, DefaultListCapacity);

                var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
                var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

                var outerMap = _ctx.GetMapping(info.OuterType);
                var innerMap = _ctx.GetMapping(info.InnerType);

                var outerColumnCount = outerMap.Columns.Length;
                var innerKeyOffset = Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);
                if (innerKeyOffset < 0)
                    throw new InvalidOperationException(
                        $"GroupJoin inner key column '{info.InnerKeyColumn?.Name ?? "(null)"}' not found in mapping for '{info.InnerType.Name}'.");
                var innerKeyIndex = outerColumnCount + innerKeyOffset;

                using var reader = cmd.ExecuteReaderWithInterception(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

                object? currentOuter = null;
                object? currentKey = null;
                List<object> currentChildren = [];

                while (reader.Read())
                {
                    // Use sync materializer — no GetAwaiter().GetResult().
                    var outer = plan.SyncMaterializer(reader);
                    var key = info.OuterKeySelector(outer) ?? DBNull.Value;

                    if (currentOuter == null || !Equals(currentKey, key))
                    {
                        if (currentOuter != null)
                        {
                            var list = CreateListFromItems(info.InnerType, currentChildren);
                            // ResultSelector expects IEnumerable<object>; .Cast<object>() provides the typed wrapper.
                            var result = info.ResultSelector(currentOuter, list.Cast<object>());
                            resultList.Add(result);
                            currentChildren = [];
                        }

                        if (trackOuter)
                        {
                            var actualMap = _ctx.GetMapping(outer.GetType());
                            var entry = _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                            outer = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                        }

                        currentOuter = outer;
                        currentKey = key;
                    }

                    if (!reader.IsDBNull(innerKeyIndex))
                    {
                        // Use configurable MaxGroupJoinSize instead of a hard-coded limit.
                        var maxSize = _ctx.Options.MaxGroupJoinSize;
                        if (currentChildren.Count >= maxSize)
                        {
                            throw new NormQueryException(
                                $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                                "This may indicate a malformed query or incorrect join keys. " +
                                "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                                "Review your GroupJoin operation and ensure join keys are correct.");
                        }

                        var inner = MaterializeEntity(reader, innerMap, outerColumnCount);
                        if (trackInner)
                        {
                            var actualMap = _ctx.GetMapping(inner.GetType());
                            var entry = _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                            inner = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                        }
                        currentChildren.Add(inner);
                    }
                }

                if (currentOuter != null)
                {
                    var list = CreateListFromItems(info.InnerType, currentChildren);
                    var result = info.ResultSelector(currentOuter, list.Cast<object>());
                    resultList.Add(result);
                }

                return resultList;
            }, "MaterializeGroupJoin", new Dictionary<string, object> { ["Sql"] = RedactSqlForLogging(cmd.CommandText) });

            // SYNC-OVER-ASYNC FIX: MaterializeEntity now uses only synchronous ADO.NET.
            // CompiledMaterializerStore materializers are registered as Func<DbDataReader, object>
            // (truly synchronous); the async wrapper calls Task.FromResult so GetAwaiter().GetResult()
            // would have been safe, but we access the typed sync delegate directly to be explicit.
            static object MaterializeEntity(DbDataReader reader, TableMapping map, int offset)
            {
                if (map.DiscriminatorColumn != null && map.TphMappings.Count > 0)
                {
                    var discIndex = offset + Array.IndexOf(map.Columns, map.DiscriminatorColumn);
                    if (!reader.IsDBNull(discIndex))
                    {
                        var disc = reader.GetValue(discIndex);
                        if (disc != null && map.TphMappings.TryGetValue(disc, out var derived))
                            return MaterializeEntity(reader, derived, offset);
                    }
                }

                // Use the synchronous materializer factory directly — no async wrapper.
                var factory = _sharedMaterializerFactory;
                var materializer = factory.CreateSyncMaterializer(map, map.Type, startOffset: offset);
                return materializer(reader);
            }

            static IList CreateListFromItems(Type innerType, List<object> items)
            {
                var list = CreateList(innerType, items.Count);
                foreach (var item in items) list.Add(item);
                return list;
            }
        }

        /// <summary>
        /// Executes dependent queries for nested collections to mitigate Cartesian explosion.
        /// Fetches child records separately and stitches them to parent entities.
        /// </summary>
        private async Task ExecuteDependentQueriesAsync(
            List<DependentQueryDefinition> dependentQueries,
            IList parents,
            bool noTracking,
            CancellationToken ct)
        {
            if (parents.Count == 0)
                return;

            foreach (var depQuery in dependentQueries)
            {
                ct.ThrowIfCancellationRequested();

                // Phase 1: Extract parent IDs
                var parentIds = new HashSet<object>();
                foreach (var parent in parents.Cast<object>())
                {
                    var keyValue = depQuery.ParentKeyProperty.GetValue(parent);
                    if (keyValue != null)
                    {
                        parentIds.Add(keyValue);
                    }
                }

                if (parentIds.Count == 0)
                {
                    // No parents have keys, assign empty collections
                    foreach (var parent in parents.Cast<object>())
                    {
                        AssignEmptyCollection(parent, depQuery);
                    }
                    continue;
                }

                // Phase 2: Fetch children in batches (to handle SQL parameter limits).
                // Uses provider's MaxParameters minus DependentQueryParameterReserve for overhead.
                var maxBatchSize = Math.Max(DependentQueryParameterReserve, _ctx.Provider.MaxParameters - DependentQueryParameterReserve);
                var allChildren = new List<object>();

                var parentIdList = parentIds.ToList();
                for (int i = 0; i < parentIdList.Count; i += maxBatchSize)
                {
                    ct.ThrowIfCancellationRequested();

                    var batchCount = Math.Min(maxBatchSize, parentIdList.Count - i);
                    var batchIds = parentIdList.GetRange(i, batchCount);
                    var batchChildren = await FetchChildrenBatchAsync(depQuery, batchIds, noTracking, ct).ConfigureAwait(false);
                    allChildren.AddRange(batchChildren);
                }

                // Phase 3: Stitch children to parents
                StitchChildrenToParents(parents, allChildren, depQuery);
            }
        }

        /// <summary>
        /// Truly synchronous variant of <see cref="ExecuteDependentQueriesAsync"/>.
        /// Uses synchronous ADO.NET methods so no thread is blocked on async work.
        /// </summary>
        private void ExecuteDependentQueries(
            List<DependentQueryDefinition> dependentQueries,
            IList parents,
            bool noTracking)
        {
            if (parents.Count == 0)
                return;

            foreach (var depQuery in dependentQueries)
            {
                var parentIds = new HashSet<object>();
                foreach (var parent in parents.Cast<object>())
                {
                    var keyValue = depQuery.ParentKeyProperty.GetValue(parent);
                    if (keyValue != null)
                        parentIds.Add(keyValue);
                }

                if (parentIds.Count == 0)
                {
                    foreach (var parent in parents.Cast<object>())
                        AssignEmptyCollection(parent, depQuery);
                    continue;
                }

                var maxBatchSize = Math.Max(DependentQueryParameterReserve, _ctx.Provider.MaxParameters - DependentQueryParameterReserve);
                var allChildren = new List<object>();

                var parentIdList = parentIds.ToList();
                for (int i = 0; i < parentIdList.Count; i += maxBatchSize)
                {
                    var batchCount = Math.Min(maxBatchSize, parentIdList.Count - i);
                    var batchIds = parentIdList.GetRange(i, batchCount);
                    var batchChildren = FetchChildrenBatch(depQuery, batchIds, noTracking);
                    allChildren.AddRange(batchChildren);
                }

                StitchChildrenToParents(parents, allChildren, depQuery);
            }
        }

        /// <summary>
        /// Truly synchronous variant of <see cref="FetchChildrenBatchAsync"/>.
        /// </summary>
        private List<object> FetchChildrenBatch(
            DependentQueryDefinition depQuery,
            List<object> parentIds,
            bool noTracking)
        {
            var children = new List<object>();

            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();

            var sql = new System.Text.StringBuilder();
            sql.Append("SELECT * FROM ").Append(depQuery.TargetMapping.EscTable);
            sql.Append(" WHERE ").Append(depQuery.ForeignKeyColumn.EscCol);
            sql.Append(" IN (");

            for (int i = 0; i < parentIds.Count; i++)
            {
                if (i > 0) sql.Append(", ");
                var paramName = $"{_ctx.Provider.ParamPrefix}p{i}";
                sql.Append(paramName);
                cmd.AddParam(paramName, parentIds[i]);
            }

            sql.Append(')');

            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = (int)_ctx.GetAdaptiveTimeout(
                AdaptiveTimeoutManager.OperationType.ComplexSelect,
                cmd.CommandText).TotalSeconds;

            using var reader = cmd.ExecuteReaderWithInterception(
                _ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(
                depQuery.TargetMapping,
                depQuery.CollectionElementType);

            while (reader.Read())
            {
                var child = syncMaterializer(reader);

                if (!noTracking)
                {
                    var entry = _ctx.ChangeTracker.Track(child, EntityState.Unchanged, depQuery.TargetMapping);
                    child = entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                }

                children.Add(child);
            }

            return children;
        }

        /// <summary>
        /// Fetches a batch of children for a dependent query using an IN clause.
        /// </summary>
        private async Task<List<object>> FetchChildrenBatchAsync(
            DependentQueryDefinition depQuery,
            List<object> parentIds,
            bool noTracking,
            CancellationToken ct)
        {
            var children = new List<object>();

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();

            // Build SQL: SELECT * FROM ChildTable WHERE ForeignKey IN (@p0, @p1, ...)
            var sql = new System.Text.StringBuilder();
            sql.Append("SELECT * FROM ").Append(depQuery.TargetMapping.EscTable);
            sql.Append(" WHERE ").Append(depQuery.ForeignKeyColumn.EscCol);
            sql.Append(" IN (");

            for (int i = 0; i < parentIds.Count; i++)
            {
                if (i > 0) sql.Append(", ");
                var paramName = $"{_ctx.Provider.ParamPrefix}p{i}";
                sql.Append(paramName);
                cmd.AddParam(paramName, parentIds[i]);
            }

            sql.Append(')');

            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = (int)_ctx.GetAdaptiveTimeout(
                AdaptiveTimeoutManager.OperationType.ComplexSelect,
                cmd.CommandText).TotalSeconds;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(
                _ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct).ConfigureAwait(false);

            // Use sync materializer to avoid per-row Task allocation, consistent with MaterializeAsync.
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(
                depQuery.TargetMapping,
                depQuery.CollectionElementType);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                var child = syncMaterializer(reader);

                if (!noTracking)
                {
                    var entry = _ctx.ChangeTracker.Track(child, EntityState.Unchanged, depQuery.TargetMapping);
                    child = entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                }

                children.Add(child);
            }

            return children;
        }

        /// <summary>
        /// Stitches fetched children back to their parent entities using a lookup.
        /// </summary>
        private static void StitchChildrenToParents(
            IList parents,
            List<object> children,
            DependentQueryDefinition depQuery)
        {
            // Create lookup: ParentId -> List<Child>
            var childrenByParentKey = new Dictionary<object, List<object>>();

            foreach (var child in children)
            {
                var foreignKeyValue = depQuery.ForeignKeyColumn.Getter(child);
                if (foreignKeyValue != null)
                {
                    if (!childrenByParentKey.TryGetValue(foreignKeyValue, out var list))
                    {
                        list = [];
                        childrenByParentKey[foreignKeyValue] = list;
                    }
                    list.Add(child);
                }
            }

            // Assign children to parents
            foreach (var parent in parents.Cast<object>())
            {
                var parentKeyValue = depQuery.ParentKeyProperty.GetValue(parent);

                IList childCollection;
                if (parentKeyValue != null && childrenByParentKey.TryGetValue(parentKeyValue, out var childList))
                {
                    // Use cached compiled factory instead of Activator.CreateInstance.
                    childCollection = CreateList(depQuery.CollectionElementType, childList.Count);
                    foreach (var child in childList)
                    {
                        childCollection.Add(child);
                    }
                }
                else
                {
                    // No children found, create empty list.
                    childCollection = CreateList(depQuery.CollectionElementType, 0);
                }

                depQuery.TargetCollectionProperty.SetValue(parent, childCollection);
            }
        }

        /// <summary>
        /// Assigns an empty collection to a parent entity's navigation property.
        /// </summary>
        private static void AssignEmptyCollection(object parent, DependentQueryDefinition depQuery)
        {
            // Use cached compiled factory instead of Activator.CreateInstance.
            var emptyList = CreateList(depQuery.CollectionElementType, 0);
            depQuery.TargetCollectionProperty.SetValue(parent, emptyList);
        }

        /// <summary>
        /// Loads owned collection items for all given owner entities. Delegates to DbContext.
        /// </summary>
        internal Task LoadOwnedCollectionsAsync(IList owners, TableMapping ownerMap, CancellationToken ct)
            => _ctx.LoadOwnedCollectionsAsync(owners, ownerMap, ct);

    }
}
