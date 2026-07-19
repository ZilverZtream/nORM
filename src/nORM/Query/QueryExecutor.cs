using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Configuration;
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
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("QueryExecutor materializes rows via reflection-built delegates; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("QueryExecutor reflects over entity types; trimming may remove the required members.")]
    internal sealed partial class QueryExecutor
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
        /// Upper bound on the pre-sizing hint derived from <c>plan.Take</c>.
        /// <c>Take(int.MaxValue)</c> is a common "no upper limit" sentinel, but using
        /// it verbatim as the initial List capacity triggers an
        /// <see cref="OutOfMemoryException"/> ("Array dimensions exceeded supported range")
        /// because the runtime tries to reserve a 2 GB backing array up front. The
        /// allocation is only a hint; the List grows as needed if more rows actually
        /// arrive. Cap at a value generous enough to avoid resizes for any realistic
        /// page size yet small enough to allocate cheaply.
        /// </summary>
        internal const int MaxPreSizedCapacity = 4096;

        /// <summary>
        /// Clamps a <c>plan.Take</c>-derived capacity hint to a safe pre-allocation
        /// range. Returns <see cref="DefaultListCapacity"/> when the hint is missing,
        /// the hint itself when it fits comfortably, or <see cref="MaxPreSizedCapacity"/>
        /// otherwise (e.g. for <c>Take(int.MaxValue)</c>).
        /// </summary>
        internal static int ClampTakeCapacity(int? take)
        {
            if (!take.HasValue) return DefaultListCapacity;
            if (take.Value <= 0) return 1;
            return Math.Min(take.Value, MaxPreSizedCapacity);
        }

        /// <summary>
        /// Number of parameter slots reserved for framework use (tenant filters, OCC tokens, etc.)
        /// when computing maximum batch size for dependent query IN clauses.
        /// Aligned with <see cref="Providers.DatabaseProvider"/> ParameterReserve.
        /// </summary>
        private const int DependentQueryParameterReserve = 100;

        /// <summary>
        /// Prefix used by the C# compiler for anonymous type names.
        /// </summary>
        private const string AnonymousTypePrefix = "<>";

        // Cached list factory delegates to avoid Activator.CreateInstance on every materialization.
        // Take values are now passed directly from the query plan rather than parsed via regex.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<int, IList>> _listFactoryCache = new();

        /// <summary>
        /// Singleton MaterializerFactory -- wraps only static caches, no instance state.
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
        /// Redacts SQL string literals for safe logging. Delegates to
        /// <see cref="nORM.Internal.SqlRedaction.RedactForLogging"/> which is not annotated with
        /// RequiresDynamicCode so it may be called from non-annotated interceptor paths.
        /// </summary>
        internal static string RedactSqlForLogging(string sql) =>
            nORM.Internal.SqlRedaction.RedactForLogging(sql);

        /// <summary>
        /// Creates a list using a cached compiled delegate instead of Activator.CreateInstance,
        /// which is significantly faster on hot paths.
        /// </summary>
        /// <param name="elementType">The element type for the list.</param>
        /// <param name="capacity">The initial capacity.</param>
        /// <returns>A new list instance.</returns>
        /// <remarks>Internal wrapper exposed for use by NormQueryProvider's pooled command path.</remarks>
        internal IList CreateListForType(Type elementType, int capacity) => CreateList(elementType, capacity);

        /// <summary>
        /// In-place reverse of an <see cref="IList"/>. Used by TakeLast/SkipLast: the
        /// translator flips ORDER BY direction and applies LIMIT/OFFSET so the SQL
        /// returns the targeted slice; this restores the original row order without
        /// allocating a new list. Works on <c>List&lt;T&gt;</c>, arrays, and any
        /// indexable IList implementation.
        /// </summary>
        internal static void ReverseListInPlace(IList list)
        {
            int n = list.Count;
            for (int i = 0, j = n - 1; i < j; i++, j--)
            {
                var tmp = list[i];
                list[i] = list[j];
                list[j] = tmp;
            }
        }

        internal static IList CreateList(Type elementType, int capacity)
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

        private CommandBehavior GetEntityReadBehavior(Type elementType)
        {
            if (!elementType.IsClass || elementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal))
                return CommandBehavior.SequentialAccess | CommandBehavior.SingleResult;

            for (var current = elementType; current != null && current != typeof(object); current = current.BaseType)
            {
                if (!_ctx.IsMapped(current)) continue;
                var map = _ctx.GetMapping(current);
                if (map.DiscriminatorColumn != null && map.TphMappings.Count > 0)
                    return CommandBehavior.Default | CommandBehavior.SingleResult;
            }

            return CommandBehavior.SequentialAccess | CommandBehavior.SingleResult;
        }

        private static CommandBehavior GetEntityReadBehavior(TableMapping mapping)
            => mapping.DiscriminatorColumn != null && mapping.TphMappings.Count > 0
                ? CommandBehavior.Default | CommandBehavior.SingleResult
                : CommandBehavior.SequentialAccess | CommandBehavior.SingleResult;

        /// <summary>
        /// Resolves the mapping used for owned-collection loading. Owned collections are part
        /// of the entity's data, not of change tracking, so they must load for UNTRACKED reads
        /// too (AsNoTracking, NoTracking-default contexts, and every AsOf query, which forces
        /// no-tracking) — gating the load on the tracking-oriented <paramref name="entityMap"/>
        /// silently returned owners with empty owned collections on all of those paths.
        /// </summary>
        private TableMapping? OwnedRootMapFor(QueryPlan plan, TableMapping? entityMap)
            => entityMap ?? (plan.ElementType.IsClass
                && !plan.ElementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal)
                && _ctx.IsMapped(plan.ElementType)
                    ? _ctx.GetMapping(plan.ElementType)
                    : null);

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
                var capacity = plan.SingleResult ? 1 : ClampTakeCapacity(plan.Take);
                var list = new List<object>(capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal) &&
                                 _ctx.IsMapped(plan.ElementType) &&
                                 !_ctx.GetMapping(plan.ElementType).IsKeyless;   // keyless = query-only, never tracked

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;
                var (idMap, idMapping) = CreateIdentityResolutionMap(_ctx, plan);
                bool isReadOnly = IsReadOnlyQuery() && !plan.ForceTracking;

                // Snapshot any filtered-Include closure params while the command is alive (its lifetime
                // may transfer to the reader), so the eager-load phase can rebind them per execution.
                var includeFilterParams = CaptureIncludeFilterParams(command, plan.Includes);

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, GetEntityReadBehavior(plan.ElementType), ct)
                    .ConfigureAwait(false);

                var syncMaterializer = plan.SyncMaterializer;

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    ct.ThrowIfCancellationRequested();
                    var entity = syncMaterializer(reader);
                    if (plan.ClientProjection != null)
                        entity = plan.ClientProjection(entity);
                    entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                    entity = ResolveRootIdentity(entity, idMap, idMapping);
                    list.Add(entity);
                }

                await reader.DisposeAsync().ConfigureAwait(false);

                // Include() itself requests the related data; split-query loading is the
                // engine's (only) strategy, not an opt-in — gating on AsSplitQuery left the
                // navigations silently null. AsSplitQuery remains as an explicit no-op.
                if (plan.Includes.Count > 0)
                {
                    // Convert to IList for EagerLoadAsync compatibility
                    IList iList = list;
                    foreach (var include in plan.Includes)
                        await _includeProcessor.EagerLoadAsync(include, iList, ct, plan.NoTracking, plan.AsOfTimestamp, includeFilterParams).ConfigureAwait(false);
                }

                // M2M eager loading runs unconditionally (no SplitQuery requirement)
                if (plan.M2MIncludes != null && plan.M2MIncludes.Count > 0)
                {
                    IList iList = list;
                    foreach (var m2mPlan in plan.M2MIncludes)
                        await _includeProcessor.LoadManyToManyAsync(m2mPlan, iList, ct, plan.NoTracking).ConfigureAwait(false);
                }

                // Load owned collections (OwnsMany) — this path serves entity roots too, and an
                // owner materialized without its owned rows silently reads as empty.
                var ownedRootMap = OwnedRootMapFor(plan, entityMap);
                if (ownedRootMap != null && ownedRootMap.OwnedCollections.Count > 0 && list.Count > 0)
                {
                    await LoadOwnedCollectionsAsync(list, ownedRootMap, ct, plan.AsOfTimestamp).ConfigureAwait(false);
                }

                if (plan.PostReverse) ReverseListInPlace(list);
                if (plan.PostMaterializeTransform != null)
                {
                    // This path is strongly typed as List<object> for the projection
                    // materializer; rebuild rather than reassign so callers still get
                    // a List<object> rather than the transform's element-typed list.
                    var transformed = plan.PostMaterializeTransform(_ctx, list);
                    var rebuilt = new List<object>(transformed.Count);
                    foreach (var item in transformed) rebuilt.Add(item!);
                    list = rebuilt;
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
            // and calls LogInformation on EVERY successful query - all pure overhead on the hot path.
            try
            {
                if (plan.GroupJoinInfo != null)
                    return await MaterializeGroupJoinAsync(plan, command, ct).ConfigureAwait(false);

                // List capacity pre-sizing: SingleResult=1, Take=Take value, else DefaultListCapacity heuristic.
                var capacity = plan.SingleResult ? 1 : ClampTakeCapacity(plan.Take);
                var list = plan.PostMaterializeTransform != null
                    ? (IList)new List<object>(capacity)
                    : CreateList(plan.ElementType, capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal) &&
                                 _ctx.IsMapped(plan.ElementType) &&
                                 !_ctx.GetMapping(plan.ElementType).IsKeyless;   // keyless = query-only, never tracked   // only mapped entity roots

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;
                var (idMap, idMapping) = CreateIdentityResolutionMap(_ctx, plan);

                // Hoist read-only check out of per-row loop: context options don't change during execution.
                bool isReadOnly = IsReadOnlyQuery() && !plan.ForceTracking;

                // Snapshot any shaped-collection filter params while the command is alive (its lifetime
                // may transfer to the reader), so the split-query phase can rebind them per execution.
                var dependentFilterParams = CaptureDependentFilterParams(command, plan.DependentQueries);
                var includeFilterParams = CaptureIncludeFilterParams(command, plan.Includes);

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, GetEntityReadBehavior(plan.ElementType), ct)
                    .ConfigureAwait(false);

                // Use sync materializer to avoid per-row Task allocation.
                var syncMaterializer = plan.SyncMaterializer;

                // Respect SingleResult flag to avoid materializing unnecessary rows.
                // Single/SingleOrDefault must read up to 2 rows to detect duplicate-row violations;
                // First/FirstOrDefault only need 1.
                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                if (plan.SingleResult)
                {
                    for (int rowIndex = 0; rowIndex < maxRows; rowIndex++)
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
                        entity = ResolveRootIdentity(entity, idMap, idMapping);
                        list.Add(entity);
                    }
                }

                await reader.DisposeAsync().ConfigureAwait(false);

                // Include() alone triggers eager loading (see the ExecuteToList overload).
                if (plan.Includes.Count > 0)
                {
                    foreach (var include in plan.Includes)
                    {
                        await _includeProcessor.EagerLoadAsync(include, list, ct, plan.NoTracking, plan.AsOfTimestamp, includeFilterParams).ConfigureAwait(false);
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
                    await ExecuteDependentQueriesAsync(plan.DependentQueries, list, plan.NoTracking, dependentFilterParams, plan.AsOfTimestamp, ct).ConfigureAwait(false);
                }

                // Load owned collections (OwnsMany) for all materialized entities — including
                // untracked reads, where owned data is still part of the entity.
                var ownedRootMap = OwnedRootMapFor(plan, entityMap);
                if (ownedRootMap != null && ownedRootMap.OwnedCollections.Count > 0 && list.Count > 0)
                {
                    await LoadOwnedCollectionsAsync(list, ownedRootMap, ct, plan.AsOfTimestamp).ConfigureAwait(false);
                }

                if (plan.PostReverse) ReverseListInPlace(list);
                if (plan.PostMaterializeTransform != null) list = plan.PostMaterializeTransform(_ctx, list);
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
            var command = cmd;
            var commandText = command.CommandText;
            var commandLifetimeTransferred = false;
            try
            {
                if (plan.GroupJoinInfo != null)
                {
                    using var groupJoinCommand = command;
                    return MaterializeGroupJoin(plan, command);
                }

                // List capacity pre-sizing: SingleResult=1, Take=Take value, else DefaultListCapacity heuristic.
                var capacity = plan.SingleResult ? 1 : ClampTakeCapacity(plan.Take);
                var list = plan.PostMaterializeTransform != null
                    ? (IList)new List<object>(capacity)
                    : CreateList(plan.ElementType, capacity);

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal) &&
                                 _ctx.IsMapped(plan.ElementType) &&
                                 !_ctx.GetMapping(plan.ElementType).IsKeyless;   // keyless = query-only, never tracked   // only mapped entity roots

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;
                var (idMap, idMapping) = CreateIdentityResolutionMap(_ctx, plan);

                // Hoist read-only check out of per-row loop: context options don't change during execution.
                bool isReadOnly = IsReadOnlyQuery() && !plan.ForceTracking;

                // Snapshot any shaped-collection filter params BEFORE the reader takes over the command's
                // lifetime (it disposes the command on reader dispose), so the split-query phase can rebind them.
                var dependentFilterParams = CaptureDependentFilterParams(command, plan.DependentQueries);
                var includeFilterParams = CaptureIncludeFilterParams(command, plan.Includes);

                commandLifetimeTransferred = true;
                using var reader = command.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, GetEntityReadBehavior(plan.ElementType));

                // Use sync materializer - no Task allocation, no async state machine.
                var syncMaterializer = plan.SyncMaterializer;

                var maxRows = plan.MethodName is "Single" or "SingleOrDefault" ? 2 : 1;
                if (plan.SingleResult)
                {
                    for (int rowIndex = 0; rowIndex < maxRows; rowIndex++)
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
                        entity = ResolveRootIdentity(entity, idMap, idMapping);
                        list.Add(entity);
                    }
                }

                reader.Dispose();

                // Include() alone triggers eager loading (see the ExecuteToList overload).
                if (plan.Includes.Count > 0)
                {
                    foreach (var include in plan.Includes)
                    {
                        // Truly synchronous eager load - no GetAwaiter().GetResult().
                        _includeProcessor.EagerLoad(include, list, plan.NoTracking, plan.AsOfTimestamp, includeFilterParams);
                    }
                }

                // M2M eager loading - truly synchronous, no GetAwaiter().GetResult().
                if (plan.M2MIncludes != null && plan.M2MIncludes.Count > 0)
                {
                    foreach (var m2mPlan in plan.M2MIncludes)
                        _includeProcessor.LoadManyToMany(m2mPlan, list, plan.NoTracking);
                }

                // Execute dependent queries for nested collections (split query for projections).
                if (plan.DependentQueries != null && plan.DependentQueries.Count > 0)
                {
                    // Truly synchronous dependent query execution - no GetAwaiter().GetResult().
                    ExecuteDependentQueries(plan.DependentQueries, list, plan.NoTracking, dependentFilterParams, plan.AsOfTimestamp);
                }

                // Load owned collections (OwnsMany) for all materialized entities — symmetric with the async
                // path, via a truly synchronous loader (no GetAwaiter().GetResult()). Without this a
                // sync-loaded owner has an EMPTY owned navigation, so a later scalar edit + SaveChanges
                // DELETE-then-reinserts owned children from that empty nav and permanently loses them.
                // Untracked reads load owned data too — it is part of the entity, not of tracking.
                var ownedRootMapSync = OwnedRootMapFor(plan, entityMap);
                if (ownedRootMapSync != null && ownedRootMapSync.OwnedCollections.Count > 0 && list.Count > 0)
                {
                    LoadOwnedCollections(list, ownedRootMapSync, plan.AsOfTimestamp);
                }

                if (plan.PostReverse) ReverseListInPlace(list);
                if (plan.PostMaterializeTransform != null) list = plan.PostMaterializeTransform(_ctx, list);
                return list;
            }
            catch (Exception ex)
            {
                if (!commandLifetimeTransferred)
                    command.Dispose();
                _logger.LogError(ex, "Materialize failed for SQL: {Sql}", RedactSqlForLogging(commandText));
                throw;
            }
        }

        public IList MaterializePooled(QueryPlan plan, DbCommand command)
        {
            var capacity = plan.SingleResult ? 1 : ClampTakeCapacity(plan.Take);
            var list = plan.PostMaterializeTransform != null
                ? (IList)new List<object>(capacity)
                : CreateList(plan.ElementType, capacity);

            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal) &&
                             _ctx.IsMapped(plan.ElementType) &&
                             !_ctx.GetMapping(plan.ElementType).IsKeyless;   // keyless = query-only, never tracked

            TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;
            var (idMap, idMapping) = CreateIdentityResolutionMap(_ctx, plan);
            bool isReadOnly = IsReadOnlyQuery() && !plan.ForceTracking;

            using var reader = command.ExecuteReaderWithInterception(_ctx, GetEntityReadBehavior(plan.ElementType));
            var syncMaterializer = plan.SyncMaterializer;

            while (reader.Read())
            {
                var entity = syncMaterializer(reader);
                entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                entity = ResolveRootIdentity(entity, idMap, idMapping);
                list.Add(entity);
            }

            if (plan.PostReverse) ReverseListInPlace(list);
            return list;
        }

        /// <summary>
        /// Sets up a query-scoped identity map for <c>AsNoTrackingWithIdentityResolution</c>: returns a fresh
        /// dictionary and the root mapping when the plan is untracked-with-identity-resolution over a mapped,
        /// keyed entity root (never a projection/anonymous/keyless type). Returns (null, null) otherwise, so
        /// the per-row resolve is a no-op for every ordinary query.
        /// </summary>
        internal static (Dictionary<object, object>? Map, TableMapping? Mapping) CreateIdentityResolutionMap(DbContext ctx, QueryPlan plan)
        {
            if (!(plan.NoTracking && plan.IdentityResolution)
                || !plan.ElementType.IsClass
                || plan.ElementType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal)
                || !ctx.IsMapped(plan.ElementType))
                return (null, null);
            var mapping = ctx.GetMapping(plan.ElementType);
            return mapping.IsKeyless ? (null, null) : (new Dictionary<object, object>(), mapping);
        }

        /// <summary>
        /// Collapses a repeated root key to the first-materialized instance, so an untracked query that
        /// projects the same entity more than once (Concat/UNION ALL, self-join/SelectMany flatten) returns a
        /// single shared instance — the identity resolution the change tracker would provide for a tracked
        /// query. A default/absent key (a null key value) is left as-is: it has no resolvable identity.
        /// </summary>
        internal static object ResolveRootIdentity(object entity, Dictionary<object, object>? idMap, TableMapping? mapping)
        {
            if (idMap is null || mapping is null || entity is null)
                return entity!;
            var keyValues = new object?[mapping.KeyColumns.Length];
            for (int i = 0; i < keyValues.Length; i++)
                keyValues[i] = mapping.KeyColumns[i].Getter(entity);
            var key = ChangeTracker.BuildLookupKey(mapping, keyValues);
            if (key is null)
                return entity;
            if (idMap.TryGetValue(key, out var existing))
                return existing;
            idMap[key] = entity;
            return entity;
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
            // Null elements are legitimate results of a LEFT JOIN flatten
            // (SelectMany ... DefaultIfEmpty) — nothing to track or proxy.
            if (entity is null)
                return entity!;
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
            // Pass the actual mapped type: `entity` is statically object here, so the generic overload would
            // bind the navigation context to System.Object and the lazy-proxy collection load could not
            // resolve the relationship (a no-op load that still marks the nav loaded → infinite recursion on
            // the next access). actualMap.Type is the entity's real runtime type.
            NavigationPropertyExtensions.EnableLazyLoading(entity, _ctx, actualMap.Type);
            return entity;
        }

        private bool IsReadOnlyQuery()
        {
            // Determine if this is a read-only context
            return _ctx.Options.DefaultTrackingBehavior == QueryTrackingBehavior.NoTracking;
        }

    }
}
