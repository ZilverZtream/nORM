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

        // Cached list factory delegates to avoid Activator.CreateInstance on every materialization.
        // Take values are now passed directly from the query plan rather than parsed via regex.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<int, IList>> _listFactoryCache = new();

        /// <summary>
        /// Singleton MaterializerFactory — wraps only static caches, no instance state.
        /// Eliminates heap allocation per dependent/group-join query.
        /// </summary>
        private static readonly MaterializerFactory _sharedMaterializerFactory = new();

        public QueryExecutor(DbContext ctx, IncludeProcessor includeProcessor, ILogger<QueryExecutor>? logger = null)
        {
            _ctx = ctx;
            _includeProcessor = includeProcessor;
            _logger = logger ?? NullLogger<QueryExecutor>.Instance;
            _exceptionHandler = new NormExceptionHandler(_logger);
        }

        /// <summary>
        /// S1: Redacts single-quoted string literals from SQL before it is written to logs,
        /// preventing sensitive literal values from appearing in log sinks.
        /// Identifiers, parameter placeholders (@p0), and SQL keywords are preserved.
        /// </summary>
        private static string RedactSqlForLogging(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;
            // Replace every 'literal' (including escaped '' sequences) with '[redacted]'.
            return Regex.Replace(sql, @"'(?:[^']|'')*'", "'[redacted]'");
        }

        /// <summary>
        /// Creates a list using a cached compiled delegate instead of Activator.CreateInstance,
        /// which is significantly faster on hot paths.
        /// </summary>
        /// <param name="elementType">The element type for the list.</param>
        /// <param name="capacity">The initial capacity.</param>
        /// <returns>A new list instance.</returns>
        /// <summary>Public-facing wrapper for use by NormQueryProvider's pooled command path.</summary>
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
        /// Executes the supplied command and materializes the result set into a list of entities.
        /// </summary>
        /// <param name="plan">Query plan describing how to materialize results.</param>
        /// <param name="cmd">Prepared database command.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>A list containing the materialized entities.</returns>
        /// <summary>
        /// Overload that materializes directly into List&lt;object&gt; to avoid covariant copy
        /// when the caller needs List&lt;object&gt; but the plan's ElementType is a concrete type.
        /// </summary>
        public async Task<List<object>> MaterializeAsObjectListAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            await using var command = cmd;
            try
            {
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
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

                // PERFORMANCE OPTIMIZATION 15: Improved list capacity pre-sizing
                // - SingleResult: capacity = 1
                // - Take specified: use Take value
                // - No Take: use heuristic (16 is typical small result set, avoids resize for most queries)
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
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

                // Execute dependent queries for nested collections (split query for projections)
                if (plan.DependentQueries != null && plan.DependentQueries.Count > 0)
                {
                    await ExecuteDependentQueriesAsync(plan.DependentQueries, list, plan.NoTracking, ct).ConfigureAwait(false);
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

                // PERFORMANCE OPTIMIZATION 15: Improved list capacity pre-sizing
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
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
                var resultList = CreateList(info.ResultType, 16);

                var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
                var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

                var outerMap = _ctx.GetMapping(info.OuterType);
                var innerMap = _ctx.GetMapping(info.InnerType);

                var outerColumnCount = outerMap.Columns.Length;
                var innerKeyIndex = outerColumnCount + Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                object? currentOuter = null;
                object? currentKey = null;
                List<object> currentChildren = [];

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var outer = await plan.Materializer(reader, ct).ConfigureAwait(false);
                    var key = info.OuterKeySelector(outer) ?? DBNull.Value;

                    if (currentOuter == null || !Equals(currentKey, key))
                    {
                        if (currentOuter != null)
                        {
                            var list = CreateListFromItems(info.InnerType, currentChildren);
                            // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
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
                    // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
                    var result = info.ResultSelector(currentOuter, list.Cast<object>());
                    resultList.Add(result);
                }

                return resultList;
            }, "MaterializeGroupJoinAsync", new Dictionary<string, object> { ["Sql"] = cmd.CommandText }).ConfigureAwait(false);

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

                if (CompiledMaterializerStore.TryGet(map.Type, out var compiled) && offset == 0)
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
                var resultList = CreateList(info.ResultType, 16);

                var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
                var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

                var outerMap = _ctx.GetMapping(info.OuterType);
                var innerMap = _ctx.GetMapping(info.InnerType);

                var outerColumnCount = outerMap.Columns.Length;
                var innerKeyIndex = outerColumnCount + Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);

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
                            // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
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
            }, "MaterializeGroupJoin", new Dictionary<string, object> { ["Sql"] = cmd.CommandText });

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

                // Phase 2: Fetch children in batches (to handle SQL parameter limits)
                // HARDCODED LIMIT FIX: Use provider's MaxParameters instead of hardcoding 2000
                // Different databases have different limits: SQL Server=2100, Oracle=1000, PostgreSQL=65535
                // Reserve 100 params for other query parts (WHERE, joins, etc.)
                var maxBatchSize = Math.Max(100, _ctx.Provider.MaxParameters - 100);
                var allChildren = new List<object>();

                var parentIdList = parentIds.ToList();
                for (int i = 0; i < parentIdList.Count; i += maxBatchSize)
                {
                    ct.ThrowIfCancellationRequested();

                    var batchIds = parentIdList.Skip(i).Take(maxBatchSize).ToList();
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

                var maxBatchSize = Math.Max(100, _ctx.Provider.MaxParameters - 100);
                var allChildren = new List<object>();

                var parentIdList = parentIds.ToList();
                for (int i = 0; i < parentIdList.Count; i += maxBatchSize)
                {
                    var batchIds = parentIdList.Skip(i).Take(maxBatchSize).ToList();
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

            // Materialize children
            var materializer = _sharedMaterializerFactory.CreateMaterializer(
                depQuery.TargetMapping,
                depQuery.CollectionElementType);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                var child = await materializer(reader, ct).ConfigureAwait(false);

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

    }
}
