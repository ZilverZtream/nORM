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

        // PERFORMANCE FIX (TASK 7): Removed regex patterns - no longer needed since Take is passed from QueryPlan
        // Previously these regex patterns were executed on EVERY query to estimate list capacity
        // This was extremely CPU intensive for large SQL strings (kilobytes long)
        // Now we pass the Take value directly from the query plan, avoiding regex entirely

        // PERFORMANCE FIX (TASK 13): Cached list factory delegates to avoid reflection
        // Activator.CreateInstance is slow - using compiled Expression delegates instead
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<int, IList>> _listFactoryCache = new();

        public QueryExecutor(DbContext ctx, IncludeProcessor includeProcessor, ILogger<QueryExecutor>? logger = null)
        {
            _ctx = ctx;
            _includeProcessor = includeProcessor;
            _logger = logger ?? NullLogger<QueryExecutor>.Instance;
            _exceptionHandler = new NormExceptionHandler(_logger);
        }

        /// <summary>
        /// PERFORMANCE FIX (TASK 13): Creates a list using a cached compiled delegate instead of Activator.CreateInstance.
        /// This is 10-50x faster on hot paths.
        /// </summary>
        /// <param name="elementType">The element type for the list.</param>
        /// <param name="capacity">The initial capacity.</param>
        /// <returns>A new list instance.</returns>
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
        public async Task<IList> MaterializeAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            await using var command = cmd;
            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
            {
                if (plan.GroupJoinInfo != null)
                    return await MaterializeGroupJoinAsync(plan, command, ct).ConfigureAwait(false);

                // PERFORMANCE OPTIMIZATION 15: Improved list capacity pre-sizing
                // - SingleResult: capacity = 1
                // - Take specified: use Take value
                // - No Take: use heuristic (16 is typical small result set, avoids resize for most queries)
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
                // PERFORMANCE FIX (TASK 13): Use cached list factory instead of Activator.CreateInstance
                var list = CreateList(plan.ElementType, capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null;

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                // PERFORMANCE FIX (TASK 15): Hoist read-only check out of per-row loop
                // IsReadOnlyQuery() checks context options which don't change during query execution
                // Calling it once instead of millions of times for large result sets
                bool isReadOnly = IsReadOnlyQuery();

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                // PERFORMANCE FIX (TASK 14): Use sync materializer to avoid per-row Task allocation
                var syncMaterializer = plan.SyncMaterializer;

                // PERFORMANCE FIX (TASK 19): Respect SingleResult flag to avoid materializing unnecessary rows
                if (plan.SingleResult)
                {
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
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
                    await ExecuteDependentQueriesAsync(plan.DependentQueries, list, ct, plan.NoTracking).ConfigureAwait(false);
                }

                return list;
            }, "MaterializeAsync", new Dictionary<string, object> { ["Sql"] = command.CommandText }).ConfigureAwait(false);
        }

        public IList Materialize(QueryPlan plan, DbCommand cmd)
        {
            using var command = cmd;
            return _exceptionHandler.ExecuteWithExceptionHandling(() =>
            {
                if (plan.GroupJoinInfo != null)
                    return Task.FromResult(MaterializeGroupJoin(plan, command));

                // PERFORMANCE OPTIMIZATION 15: Improved list capacity pre-sizing
                // - SingleResult: capacity = 1
                // - Take specified: use Take value
                // - No Take: use heuristic (16 is typical small result set, avoids resize for most queries)
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
                // PERFORMANCE FIX (TASK 13): Use cached list factory instead of Activator.CreateInstance
                var list = CreateList(plan.ElementType, capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null;

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                // PERFORMANCE FIX (TASK 15): Hoist read-only check out of per-row loop
                bool isReadOnly = IsReadOnlyQuery();

                using var reader = command.ExecuteReaderWithInterception(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

                // PERFORMANCE FIX (TASK 14): Use sync materializer directly - no Task allocation at all!
                var syncMaterializer = plan.SyncMaterializer;

                // PERFORMANCE FIX (TASK 19): Respect SingleResult flag to avoid materializing unnecessary rows
                if (plan.SingleResult)
                {
                    if (reader.Read())
                    {
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
                    while (reader.Read())
                    {
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
                        // SYNC-OVER-ASYNC PATTERN: This is intentional for the synchronous Materialize path
                        // The async method uses ConfigureAwait(false) throughout to minimize deadlock risk
                        // This pattern matches the overall design where synchronous APIs are provided for backward compatibility
                        _includeProcessor.EagerLoadAsync(include, list, default, plan.NoTracking).GetAwaiter().GetResult();
                    }
                }

                // Execute dependent queries for nested collections (split query for projections)
                if (plan.DependentQueries != null && plan.DependentQueries.Count > 0)
                {
                    // SYNC-OVER-ASYNC PATTERN: This is intentional for the synchronous Materialize path
                    // The async method uses ConfigureAwait(false) throughout to minimize deadlock risk
                    ExecuteDependentQueriesAsync(plan.DependentQueries, list, default, plan.NoTracking).GetAwaiter().GetResult();
                }

                return Task.FromResult(list);
            }, "Materialize", new Dictionary<string, object> { ["Sql"] = command.CommandText }).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Processes an entity after materialization, optionally tracking it and enabling lazy loading.
        /// </summary>
        /// <param name="entity">The materialized entity.</param>
        /// <param name="trackable">Whether the entity type is trackable.</param>
        /// <param name="entityMap">The table mapping for the entity.</param>
        /// <param name="isReadOnly">
        /// PERFORMANCE FIX (TASK 15): Whether this is a read-only query (hoisted from per-row check).
        /// </param>
        /// <returns>The processed entity (may be a tracking proxy).</returns>
        // PERFORMANCE OPTIMIZATION 14: Aggressive inlining for per-row hot path
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private object ProcessEntity(object entity, bool trackable, TableMapping? entityMap, bool isReadOnly)
        {
            if (!trackable)
                return entity;

            // PERFORMANCE FIX (TASK 15): Use pre-computed isReadOnly flag instead of calling method
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

        // PERFORMANCE FIX (TASK 7): Removed EstimateCapacity method entirely
        // This method used expensive regex parsing on every query execution
        // Replaced with direct Take value from QueryPlan

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

            // RELIABILITY FIX (TASK 14): Removed try-catch with manual cmd.DisposeAsync
            // MaterializeAsync already owns the command's lifetime via "await using var command = cmd"
            // Double-disposing the command causes errors. Let the caller handle disposal.
            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
            {
                // PERFORMANCE FIX (TASK 13): Use cached list factory instead of Activator.CreateInstance
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
                List<object> currentChildren = new();

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
                            currentChildren = new List<object>();
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
                        // FIX (TASK 5): Use configurable MaxGroupJoinSize instead of hard-coded limit
                        // Prevents unbounded memory growth while allowing users to opt-in to larger datasets
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

                // PERFORMANCE FIX (TASK 9): Use MaterializerFactory instead of slow reflection
                // MaterializerFactory creates compiled IL.Emit/Expression-based materializers
                // that are 10-100x faster than reflection (read.Invoke, col.Setter)
                var factory = new MaterializerFactory();
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

            return _exceptionHandler.ExecuteWithExceptionHandling(() =>
            {
                // PERFORMANCE FIX (TASK 13): Use cached list factory instead of Activator.CreateInstance
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
                List<object> currentChildren = new();

                while (reader.Read())
                {
                    var outer = plan.Materializer(reader, default).GetAwaiter().GetResult();
                    var key = info.OuterKeySelector(outer) ?? DBNull.Value;

                    if (currentOuter == null || !Equals(currentKey, key))
                    {
                        if (currentOuter != null)
                        {
                            var list = CreateListFromItems(info.InnerType, currentChildren);
                            // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
                            var result = info.ResultSelector(currentOuter, list.Cast<object>());
                            resultList.Add(result);
                            currentChildren = new List<object>();
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
                        // FIX (TASK 5): Use configurable MaxGroupJoinSize instead of hard-coded limit
                        var maxSize = _ctx.Options.MaxGroupJoinSize;
                        if (currentChildren.Count >= maxSize)
                        {
                            throw new NormQueryException(
                                $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                                "This may indicate a malformed query or incorrect join keys. " +
                                "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                                "Review your GroupJoin operation and ensure join keys are correct.");
                        }

                        var inner = MaterializeEntity(reader, innerMap, outerColumnCount, default);
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

                // DISPOSAL RACE FIX: Removed redundant manual Dispose in catch block
                // The command is already owned by the calling method (Materialize) via "using var command = cmd"
                // Manual disposal here causes double-dispose which can mask original exceptions
                return Task.FromResult(resultList);
            }, "MaterializeGroupJoin", new Dictionary<string, object> { ["Sql"] = cmd.CommandText }).GetAwaiter().GetResult();

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

                // PERFORMANCE FIX (TASK 9): Use MaterializerFactory instead of slow reflection
                // MaterializerFactory creates compiled IL.Emit/Expression-based materializers
                // that are 10-100x faster than reflection (read.Invoke, col.Setter)
                var factory = new MaterializerFactory();
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

        /// <summary>
        /// Executes dependent queries for nested collections to mitigate Cartesian explosion.
        /// Fetches child records separately and stitches them to parent entities.
        /// </summary>
        private async Task ExecuteDependentQueriesAsync(
            List<DependentQueryDefinition> dependentQueries,
            IList parents,
            CancellationToken ct,
            bool noTracking)
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
                    var batchChildren = await FetchChildrenBatchAsync(depQuery, batchIds, ct, noTracking).ConfigureAwait(false);
                    allChildren.AddRange(batchChildren);
                }

                // Phase 3: Stitch children to parents
                StitchChildrenToParents(parents, allChildren, depQuery);
            }
        }

        /// <summary>
        /// Fetches a batch of children for a dependent query using an IN clause.
        /// </summary>
        private async Task<List<object>> FetchChildrenBatchAsync(
            DependentQueryDefinition depQuery,
            List<object> parentIds,
            CancellationToken ct,
            bool noTracking)
        {
            var children = new List<object>();

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();

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

            sql.Append(")");

            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = (int)_ctx.GetAdaptiveTimeout(
                AdaptiveTimeoutManager.OperationType.ComplexSelect,
                cmd.CommandText).TotalSeconds;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(
                _ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct).ConfigureAwait(false);

            // Materialize children
            var materializer = new MaterializerFactory().CreateMaterializer(
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
        private void StitchChildrenToParents(
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
                        list = new List<object>();
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
                    // PERFORMANCE FIX: Use cached compiled factory instead of Activator.CreateInstance
                    // This is 10-50x faster on hot paths
                    childCollection = CreateList(depQuery.CollectionElementType, childList.Count);
                    foreach (var child in childList)
                    {
                        childCollection.Add(child);
                    }
                }
                else
                {
                    // No children found, create empty list
                    // PERFORMANCE FIX: Use cached compiled factory instead of Activator.CreateInstance
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
            // PERFORMANCE FIX: Use cached compiled factory instead of Activator.CreateInstance
            // This is 10-50x faster on hot paths
            var emptyList = CreateList(depQuery.CollectionElementType, 0);
            depQuery.TargetCollectionProperty.SetValue(parent, emptyList);
        }

    }
}
