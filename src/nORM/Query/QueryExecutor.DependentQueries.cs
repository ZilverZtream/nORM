using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;

#pragma warning disable IDE0130

namespace nORM.Query
{
    internal sealed partial class QueryExecutor
    {
        /// <summary>
        /// Executes dependent queries for nested collections to mitigate Cartesian explosion.
        /// Fetches child records separately and stitches them to parent entities.
        /// </summary>
        private async Task ExecuteDependentQueriesAsync(
            List<DependentQueryDefinition> dependentQueries,
            IList parents,
            bool noTracking,
            Dictionary<string, object?>? filterParams,
            CancellationToken ct)
        {
            if (parents.Count == 0)
                return;

            foreach (var depQuery in dependentQueries)
            {
                ct.ThrowIfCancellationRequested();

                // Phase 1: Extract parent IDs. Composite relationships must keep the
                // full ordered key tuple or split-query stitching can cross tenants.
                var parentIds = new HashSet<object>();
                foreach (var parent in parents.Cast<object>())
                {
                    var keyValue = GetParentKeyValue(depQuery, parent);
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
                var availableParameters = Math.Max(1, _ctx.RawProvider.MaxParameters - DependentQueryParameterReserve);
                var maxBatchSize = Math.Max(1, availableParameters / Math.Max(1, depQuery.ForeignKeyColumns.Count));
                var allChildren = new List<object>();

                var parentIdList = parentIds.ToList();
                for (int i = 0; i < parentIdList.Count; i += maxBatchSize)
                {
                    ct.ThrowIfCancellationRequested();

                    var batchCount = Math.Min(maxBatchSize, parentIdList.Count - i);
                    var batchIds = parentIdList.GetRange(i, batchCount);
                    var batchChildren = await FetchChildrenBatchAsync(depQuery, batchIds, noTracking, filterParams, ct).ConfigureAwait(false);
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
            bool noTracking,
            Dictionary<string, object?>? filterParams)
        {
            if (parents.Count == 0)
                return;

            foreach (var depQuery in dependentQueries)
            {
                var parentIds = new HashSet<object>();
                foreach (var parent in parents.Cast<object>())
                {
                    var keyValue = GetParentKeyValue(depQuery, parent);
                    if (keyValue != null)
                        parentIds.Add(keyValue);
                }

                if (parentIds.Count == 0)
                {
                    foreach (var parent in parents.Cast<object>())
                        AssignEmptyCollection(parent, depQuery);
                    continue;
                }

                var availableParameters = Math.Max(1, _ctx.RawProvider.MaxParameters - DependentQueryParameterReserve);
                var maxBatchSize = Math.Max(1, availableParameters / Math.Max(1, depQuery.ForeignKeyColumns.Count));
                var allChildren = new List<object>();

                var parentIdList = parentIds.ToList();
                for (int i = 0; i < parentIdList.Count; i += maxBatchSize)
                {
                    var batchCount = Math.Min(maxBatchSize, parentIdList.Count - i);
                    var batchIds = parentIdList.GetRange(i, batchCount);
                    var batchChildren = FetchChildrenBatch(depQuery, batchIds, noTracking, filterParams);
                    allChildren.AddRange(batchChildren);
                }

                StitchChildrenToParents(parents, allChildren, depQuery);
            }
        }

        /// <summary>
        /// Snapshots the values of every compiled parameter any shaped-collection filter references,
        /// read from the main command while it is still alive (the sync path transfers the command's
        /// lifetime to its reader, so the values must be captured before the dependent-query phase runs).
        /// Returns null when no dependent query carries a filter parameter, so the common bare-include
        /// path allocates nothing. The captured values are the provider representations the main query
        /// bound (converters already applied), so the child fetch rebinds them verbatim.
        /// </summary>
        private static Dictionary<string, object?>? CaptureDependentFilterParams(DbCommand command, IReadOnlyList<DependentQueryDefinition>? deps)
        {
            if (deps == null)
                return null;

            Dictionary<string, object?>? snapshot = null;
            foreach (var dep in deps)
            {
                if (dep.FilterParameters == null)
                    continue;
                foreach (var name in dep.FilterParameters)
                {
                    if (snapshot != null && snapshot.ContainsKey(name))
                        continue;
                    if (command.Parameters.Contains(name))
                        (snapshot ??= new Dictionary<string, object?>(StringComparer.Ordinal))[name] = command.Parameters[name].Value;
                }
            }
            return snapshot;
        }

        /// <summary>
        /// Snapshots the compiled-parameter values any filtered Include (<c>Include(o =&gt; o.Lines.Where(pred))</c>)
        /// references from the main command while it is still alive — the JOIN-based eager load runs on
        /// separate commands, so the closure captures must be carried across the same way the split-query
        /// dependent-filter path carries them. Returns null when no include carries a filter parameter, so
        /// the common bare-include path allocates nothing.
        /// </summary>
        private static Dictionary<string, object?>? CaptureIncludeFilterParams(DbCommand command, IReadOnlyList<IncludePlan>? includes)
        {
            if (includes == null)
                return null;

            Dictionary<string, object?>? snapshot = null;
            foreach (var include in includes)
            {
                foreach (var filter in include.Filters)
                {
                    if (filter == null)
                        continue;
                    foreach (var name in filter.Parameters)
                    {
                        if (snapshot != null && snapshot.ContainsKey(name))
                            continue;
                        if (command.Parameters.Contains(name))
                            (snapshot ??= new Dictionary<string, object?>(StringComparer.Ordinal))[name] = command.Parameters[name].Value;
                    }
                }
            }
            return snapshot;
        }

        /// <summary>
        /// Appends a shaped-collection filter (<c>o.Lines.Where(pred).ToList()</c>) — rendered to SQL at
        /// plan-build time — onto the child fetch, binding the compiled parameters it references from the
        /// captured main-command values so closure captures re-bind per execution. No-op when the dependent
        /// query carries no filter.
        /// </summary>
        private static void AppendDependentFilter(
            DbCommand cmd,
            System.Text.StringBuilder sql,
            DependentQueryDefinition depQuery,
            Dictionary<string, object?>? filterParams)
        {
            if (depQuery.FilterSql == null)
                return;

            if (depQuery.FilterParameters != null && filterParams != null)
            {
                foreach (var name in depQuery.FilterParameters)
                {
                    if (cmd.Parameters.Contains(name))
                        continue;
                    filterParams.TryGetValue(name, out var value);
                    cmd.AddParam(name, value ?? DBNull.Value);
                }
            }
            sql.Append(" AND (").Append(depQuery.FilterSql).Append(')');
        }

        private List<object> FetchChildrenBatch(
            DependentQueryDefinition depQuery,
            List<object> parentIds,
            bool noTracking,
            Dictionary<string, object?>? filterParams)
        {
            var children = new List<object>();

            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();

            var sql = new System.Text.StringBuilder();
            sql.Append("SELECT * FROM ").Append(depQuery.TargetMapping.EscTable);
            sql.Append(" WHERE ");
            AppendDependentQueryWhere(cmd, sql, depQuery, parentIds);

            // X2: Apply tenant predicate to split-query child loading, matching the filter applied
            // to the parent query by ApplyGlobalFilters. Without this, cross-tenant FK overlaps
            // could cause a parent from tenant A to load children belonging to tenant B.
            if (_ctx.Options.TenantProvider != null)
            {
                var tenantCol = _ctx.RequireTenantColumn(depQuery.TargetMapping, "split-query child load");
                var tenantParam = $"{_ctx.RawProvider.ParamPrefix}__tenant_child";
                sql.Append($" AND {tenantCol.EscCol}={tenantParam}");
                cmd.AddParam(tenantParam, _ctx.GetRequiredTenantId(depQuery.TargetMapping, "split-query child load"));
            }

            // Apply the general global filters (e.g. soft-delete) to the eager-loaded child too — the
            // root query is filtered by ApplyGlobalFilters, but this hand-built dependent SQL is not, so
            // without this a soft-deleted (or cross-tenant) child leaks into the navigation collection.
            var globalFilterSql = GlobalFilterFragment.Build(_ctx, depQuery.TargetMapping, depQuery.TargetMapping.EscTable, cmd);
            if (globalFilterSql != null)
                sql.Append(" AND ").Append(globalFilterSql);

            // Shaped-projection per-element filter (Lines = o.Lines.Where(pred).ToList()): append the
            // plan-rendered predicate and bind its compiled params from the captured main-command values.
            AppendDependentFilter(cmd, sql, depQuery, filterParams);

            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = (int)_ctx.GetAdaptiveTimeout(
                AdaptiveTimeoutManager.OperationType.ComplexSelect,
                cmd.CommandText).TotalSeconds;

            using var reader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(
                _ctx, GetEntityReadBehavior(depQuery.TargetMapping));

            // A projected collection (o.Lines.Select(l => new Dto{...}).ToList()) materializes the child
            // ENTITY (kept transient — never tracked, since the result is a projected DTO) so the FK stays
            // available for stitching; the projection is applied per child when the parent's collection is
            // built in StitchChildrenToParents.
            var projecting = depQuery.ElementProjection != null;
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(
                depQuery.TargetMapping,
                projecting ? depQuery.TargetMapping.Type : depQuery.CollectionElementType);

            while (reader.Read())
            {
                var child = syncMaterializer(reader);

                if (!noTracking && !projecting)
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
            Dictionary<string, object?>? filterParams,
            CancellationToken ct)
        {
            var children = new List<object>();

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();

            // Build SQL: SELECT * FROM ChildTable WHERE ForeignKey IN (@p0, @p1, ...)
            var sql = new System.Text.StringBuilder();
            sql.Append("SELECT * FROM ").Append(depQuery.TargetMapping.EscTable);
            sql.Append(" WHERE ");
            AppendDependentQueryWhere(cmd, sql, depQuery, parentIds);

            // X2: Apply tenant predicate to split-query child loading, matching the filter applied
            // to the parent query by ApplyGlobalFilters. Without this, cross-tenant FK overlaps
            // could cause a parent from tenant A to load children belonging to tenant B.
            if (_ctx.Options.TenantProvider != null)
            {
                var tenantCol = _ctx.RequireTenantColumn(depQuery.TargetMapping, "split-query child load");
                var tenantParam = $"{_ctx.RawProvider.ParamPrefix}__tenant_child";
                sql.Append($" AND {tenantCol.EscCol}={tenantParam}");
                cmd.AddParam(tenantParam, _ctx.GetRequiredTenantId(depQuery.TargetMapping, "split-query child load"));
            }

            // Apply the general global filters (e.g. soft-delete) to the eager-loaded child too — the
            // root query is filtered by ApplyGlobalFilters, but this hand-built dependent SQL is not, so
            // without this a soft-deleted (or cross-tenant) child leaks into the navigation collection.
            var globalFilterSql = GlobalFilterFragment.Build(_ctx, depQuery.TargetMapping, depQuery.TargetMapping.EscTable, cmd);
            if (globalFilterSql != null)
                sql.Append(" AND ").Append(globalFilterSql);

            // Shaped-projection per-element filter (Lines = o.Lines.Where(pred).ToList()): append the
            // plan-rendered predicate and bind its compiled params from the captured main-command values.
            AppendDependentFilter(cmd, sql, depQuery, filterParams);

            cmd.CommandText = sql.ToString();
            cmd.CommandTimeout = (int)_ctx.GetAdaptiveTimeout(
                AdaptiveTimeoutManager.OperationType.ComplexSelect,
                cmd.CommandText).TotalSeconds;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(
                _ctx, GetEntityReadBehavior(depQuery.TargetMapping), ct).ConfigureAwait(false);

            // Use sync materializer to avoid per-row Task allocation, consistent with MaterializeAsync.
            // A projected collection materializes the child ENTITY (transient, untracked — the result is a
            // projected DTO) so the FK survives for stitching; the projection is applied at stitch time.
            var projecting = depQuery.ElementProjection != null;
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(
                depQuery.TargetMapping,
                projecting ? depQuery.TargetMapping.Type : depQuery.CollectionElementType);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                var child = syncMaterializer(reader);

                if (!noTracking && !projecting)
                {
                    var entry = _ctx.ChangeTracker.Track(child, EntityState.Unchanged, depQuery.TargetMapping);
                    child = entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                }

                children.Add(child);
            }

            return children;
        }

        private void AppendDependentQueryWhere(
            DbCommand cmd,
            System.Text.StringBuilder sql,
            DependentQueryDefinition depQuery,
            List<object> parentIds)
        {
            if (!depQuery.IsComposite)
            {
                sql.Append(depQuery.ForeignKeyColumn.EscCol).Append(" IN (");
                for (int i = 0; i < parentIds.Count; i++)
                {
                    if (i > 0) sql.Append(", ");
                    var paramName = $"{_ctx.RawProvider.ParamPrefix}p{i}";
                    sql.Append(paramName);
                    cmd.AddParam(paramName, parentIds[i]);
                }
                sql.Append(')');
                return;
            }

            sql.Append('(');
            for (int keyIndex = 0; keyIndex < parentIds.Count; keyIndex++)
            {
                if (keyIndex > 0) sql.Append(" OR ");
                if (parentIds[keyIndex] is not RelationKey key || key.Values.Length != depQuery.ForeignKeyColumns.Count)
                    throw new NormConfigurationException(
                        $"Composite split-query load expected {depQuery.ForeignKeyColumns.Count} key values for '{depQuery.TargetCollectionProperty.Name}'.");

                sql.Append('(');
                for (int columnIndex = 0; columnIndex < depQuery.ForeignKeyColumns.Count; columnIndex++)
                {
                    if (columnIndex > 0) sql.Append(" AND ");
                    var paramName = $"{_ctx.RawProvider.ParamPrefix}p{keyIndex}_{columnIndex}";
                    sql.Append(depQuery.ForeignKeyColumns[columnIndex].EscCol).Append(" = ").Append(paramName);
                    cmd.AddParam(paramName, key.Values[columnIndex]!);
                }
                sql.Append(')');
            }
            sql.Append(')');
        }

        /// <summary>
        /// Stitches fetched children back to their parent entities using a lookup.
        /// </summary>
        private static void StitchChildrenToParents(
            IList parents,
            List<object> children,
            DependentQueryDefinition depQuery)
        {
            // For a projected collection the fetched children are transient ENTITIES (kept so the FK can
            // group them); this compiled delegate shapes each into the projected element type as the
            // parent's collection is built. Null for bare/filtered (non-projected) collections.
            var projector = depQuery.ElementProjection is { } proj ? CompileElementProjection(proj) : null;

            // Create lookup: ParentId -> List<Child>
            var childrenByParentKey = new Dictionary<object, List<object>>();

            foreach (var child in children)
            {
                var foreignKeyValue = GetForeignKeyValue(depQuery, child);
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
                var parentKeyValue = GetParentKeyValue(depQuery, parent);

                IList childCollection;
                if (parentKeyValue != null && childrenByParentKey.TryGetValue(parentKeyValue, out var childList))
                {
                    // Use cached compiled factory instead of Activator.CreateInstance.
                    childCollection = CreateList(depQuery.CollectionElementType, childList.Count);
                    foreach (var child in childList)
                    {
                        childCollection.Add(projector != null ? projector(child) : child);
                    }
                }
                else
                {
                    // No children found, create empty list.
                    childCollection = CreateList(depQuery.CollectionElementType, 0);
                }

                ResolveOnParent(depQuery.TargetCollectionProperty, parent, depQuery).SetValue(parent, childCollection);
            }
        }

        /// <summary>
        /// Compiles a closure-free element projection (verified at translation) into a boxed
        /// <c>object → object</c> delegate applied per child entity when a shaped-projected collection is
        /// stitched. Closure-free, so the compiled delegate is stable across executions.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Compiling an element projection emits IL at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Compiling an element projection reflects over the projected type; trimming may remove the required members. See docs/aot-trimming.md.")]
        private static Func<object, object> CompileElementProjection(LambdaExpression projection)
        {
            var param = Expression.Parameter(typeof(object), "e");
            var typed = Expression.Convert(param, projection.Parameters[0].Type);
            var body = Expression.Convert(Expression.Invoke(projection, typed), typeof(object));
            return Expression.Lambda<Func<object, object>>(body, param).Compile();
        }

        private static object? GetParentKeyValue(DependentQueryDefinition depQuery, object parent)
            => GetKeyValue(depQuery.ParentKeyProperties, p => ResolveOnParent(p, parent, depQuery).GetValue(parent));

        private static object? GetForeignKeyValue(DependentQueryDefinition depQuery, object child)
            => GetKeyValue(depQuery.ForeignKeyColumns, c => c.Getter(child));

        private static readonly System.Collections.Concurrent.ConcurrentDictionary<(Type, string), PropertyInfo?> _projectedParentProps = new();

        /// <summary>
        /// Resolves an entity-typed navigation/key <see cref="PropertyInfo"/> against the runtime type
        /// of <paramref name="parent"/>. For a split query feeding an Include the parent IS the entity,
        /// so the property applies directly. For a shaped collection projection the parent is a DTO whose
        /// members mirror the entity by name, so the same-named property is resolved (and cached). Throws
        /// a clear precondition error rather than silently stitching empty collections when the projected
        /// type omits the correlating property — a shaped collection projection must also project the
        /// principal key so its children can be matched.
        /// </summary>
        private static PropertyInfo ResolveOnParent(PropertyInfo prop, object parent, DependentQueryDefinition depQuery)
        {
            if (prop.DeclaringType != null && prop.DeclaringType.IsInstanceOfType(parent))
                return prop;

            var resolved = _projectedParentProps.GetOrAdd((parent.GetType(), prop.Name),
                static k => k.Item1.GetProperty(k.Item2));
            if (resolved == null)
                throw new NormQueryException(
                    $"A shaped collection projection into '{parent.GetType().Name}' must also project the " +
                    $"principal key '{prop.Name}' of '{depQuery.TargetCollectionProperty.DeclaringType?.Name}' so " +
                    $"the '{depQuery.TargetCollectionProperty.Name}' collection can be correlated to its parent.");
            return resolved;
        }

        private static object? GetKeyValue<T>(IReadOnlyList<T> keyParts, Func<T, object?> getter)
        {
            if (keyParts.Count == 1)
                return getter(keyParts[0]);

            var values = new object?[keyParts.Count];
            for (var i = 0; i < keyParts.Count; i++)
            {
                values[i] = getter(keyParts[i]);
                if (values[i] == null)
                    return null;
            }

            return new RelationKey(values);
        }

        private sealed class RelationKey : IEquatable<RelationKey>
        {
            public RelationKey(object?[] values) => Values = values;

            public object?[] Values { get; }

            public bool Equals(RelationKey? other)
            {
                if (other == null || other.Values.Length != Values.Length)
                    return false;
                for (var i = 0; i < Values.Length; i++)
                {
                    if (!object.Equals(Values[i], other.Values[i]))
                        return false;
                }
                return true;
            }

            public override bool Equals(object? obj) => Equals(obj as RelationKey);

            public override int GetHashCode()
            {
                var hash = 17;
                foreach (var value in Values)
                    hash = (hash * 23) + (value?.GetHashCode() ?? 0);
                return hash;
            }
        }

        /// <summary>
        /// Assigns an empty collection to a parent entity's navigation property.
        /// </summary>
        private static void AssignEmptyCollection(object parent, DependentQueryDefinition depQuery)
        {
            // Use cached compiled factory instead of Activator.CreateInstance.
            var emptyList = CreateList(depQuery.CollectionElementType, 0);
            ResolveOnParent(depQuery.TargetCollectionProperty, parent, depQuery).SetValue(parent, emptyList);
        }

        /// <summary>
        /// Loads owned collection items for all given owner entities. Delegates to DbContext.
        /// Under AsOf the owned rows reconstruct through the same history window as the root.
        /// </summary>
        internal Task LoadOwnedCollectionsAsync(IList owners, TableMapping ownerMap, CancellationToken ct, DateTime? asOf = null)
            => _ctx.LoadOwnedCollectionsAsync(owners, ownerMap, ct, asOf);

        /// <summary>
        /// Synchronous owned-collection load used by the sync materialize path (which is deliberately
        /// free of GetAwaiter().GetResult()). Delegates to DbContext.
        /// </summary>
        internal void LoadOwnedCollections(IList owners, TableMapping ownerMap, DateTime? asOf = null)
            => _ctx.LoadOwnedCollections(owners, ownerMap, asOf);
    }
}
