using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Navigation;

namespace nORM.Query
{
    internal sealed partial class IncludeProcessor
    {
        /// <summary>
        /// Eagerly loads a many-to-many relationship for the given parent entities by
        /// querying the join table and then loading the related entities.
        /// </summary>
        public async Task LoadManyToManyAsync(M2MIncludePlan plan, IList parents, CancellationToken ct, bool noTracking)
        {
            if (parents.Count == 0) return;
            var jtm = plan.JoinTable;

            var parentKeyValues = new Dictionary<object, object?[]>();
            foreach (var p in parents.Cast<object>())
            {
                var keyValues = jtm.GetLeftKeyValues(p);
                if (keyValues == null) continue;
                var pk = jtm.CreateLeftKeyFromValues(keyValues);
                if (pk == null) continue;
                parentKeyValues.TryAdd(pk, keyValues);
            }
            if (parentKeyValues.Count == 0) return;

            // Build a query: SELECT right_fk, right_pk FROM join_table INNER JOIN right_table ON ... WHERE left_fk IN (...)
            // Simpler: SELECT right_fk FROM join_table WHERE left_fk IN (...) -> then load right entities.
            var leftKeyGroups = parentKeyValues.Values.ToList();
            var rightMapping = _ctx.GetMapping(jtm.RightType);

            // Reset/initialize empty collections on all parents.
            // Always create a fresh list so that M2M loading replaces whatever
            // was in the collection before (avoids duplicates when the same
            // context is used for both saving and querying).
            foreach (var p in parents.Cast<object>())
            {
                var emptyList = QueryExecutor.CreateList(jtm.RightType, 0);
                jtm.LeftCollectionSetter(p, emptyList);
            }

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();

            // Determine tenant filtering for the join table query and right entity query.
            // When multi-tenancy is active on the left entity, scope the join table query to
            // only return rows whose left FK belongs to the current tenant.
            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var tenantActive = _ctx.Options.TenantProvider != null;
            var tenantId = tenantActive ? _ctx.GetRequiredTenantId(leftMapping, "many-to-many include") : null;
            var leftTenantCol = tenantActive ? _ctx.RequireTenantColumn(leftMapping, "many-to-many include left side") : null;
            var hasTenantFilter = tenantActive;
            var leftPredicate = BuildColumnValuePredicate(cmd, _ctx.RawProvider.ParamPrefix, "jlfk", "jt", jtm.EscLeftFkColumns, leftKeyGroups);
            var selectedJoinColumns = string.Join(", ", jtm.EscLeftFkColumns.Concat(jtm.EscRightFkColumns).Select(c => "jt." + c));

            // Query: SELECT jt.left_fk, jt.right_fk FROM join_table jt [INNER JOIN left_table lt ON ...] WHERE jt.left_fk IN (...)
            if (hasTenantFilter)
            {
                var tp = $"{_ctx.RawProvider.ParamPrefix}jttenant";
                var leftJoinPredicate = BuildColumnJoinPredicate("jt", jtm.EscLeftFkColumns, "lt", jtm.LeftKeyColumns);
                cmd.CommandText = $"SELECT {selectedJoinColumns} FROM {jtm.EscTableName} jt INNER JOIN {leftMapping.EscTable} lt ON {leftJoinPredicate} WHERE lt.{leftTenantCol!.EscCol} = {tp} AND {leftPredicate}";
                cmd.AddParam(tp, tenantId!);
            }
            else
            {
                cmd.CommandText = $"SELECT {string.Join(", ", jtm.EscLeftFkColumns.Concat(jtm.EscRightFkColumns))} FROM {jtm.EscTableName} jt WHERE {leftPredicate}";
            }
            cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

            // Read all join rows: leftPk -> list of rightPks.
            var joinRows = new Dictionary<object, List<object>>();
            var allRightPks = new HashSet<object>();
            var rightKeyValues = new Dictionary<object, object?[]>();
            var leftKeyWidth = jtm.LeftFkColumns.Count;
            var rightKeyWidth = jtm.RightFkColumns.Count;
            await using (var jReader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, System.Data.CommandBehavior.Default, ct).ConfigureAwait(false))
            {
                while (await jReader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var leftValues = ReadKeyValues(jReader, 0, leftKeyWidth);
                    var rightValues = ReadKeyValues(jReader, leftKeyWidth, rightKeyWidth);
                    if (leftValues == null || rightValues == null) continue;
                    var lPk = jtm.CreateLeftKeyFromValues(leftValues);
                    var rPk = jtm.CreateRightKeyFromValues(rightValues);
                    if (lPk == null || rPk == null) continue;
                    if (!joinRows.TryGetValue(lPk, out var rList))
                        joinRows[lPk] = rList = new List<object>();
                    rList.Add(rPk);
                    allRightPks.Add(rPk);
                    rightKeyValues.TryAdd(rPk, rightValues);
                }
            }

            if (allRightPks.Count == 0) return;

            // Load related (right) entities in a single query.
            await using var cmd2 = _ctx.CreateCommand();
            var rightPredicate = BuildColumnValuePredicate(cmd2, _ctx.RawProvider.ParamPrefix, "jrpk", null, jtm.RightKeyColumns, rightKeyValues.Values.ToList());

            // If the right entity table also has a tenant column, filter right entities by tenant.
            var rightTenantCol = rightMapping.TenantColumn;
            if (tenantActive)
            {
                rightTenantCol = _ctx.RequireTenantColumn(rightMapping, "many-to-many include right side");
                var rtp = $"{_ctx.RawProvider.ParamPrefix}jrtenant";
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPredicate} AND {rightTenantCol.EscCol} = {rtp}";
                cmd2.AddParam(rtp, _ctx.GetRequiredTenantId(rightMapping, "many-to-many include right side"));
            }
            else
            {
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPredicate}";
            }
            // The right entity's global filters (soft-delete) restrict which related rows
            // are visible — the same rule the regular include levels apply. Without this,
            // filtered-out rows leak into the loaded collections.
            var rightGlobalFilter = GlobalFilterFragment.Build(_ctx, rightMapping, rightMapping.EscTable, cmd2);
            if (rightGlobalFilter != null)
                cmd2.CommandText += $" AND {rightGlobalFilter}";
            cmd2.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd2.CommandText);

            var rightEntitiesByPk = new Dictionary<object, object>();
            var rightMat = _materializerFactory.CreateSyncMaterializer(rightMapping, jtm.RightType);
            await using (var rReader = await cmd2.ExecuteReaderWithInterceptionAsync(_ctx, System.Data.CommandBehavior.Default, ct).ConfigureAwait(false))
            {
                while (await rReader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var rightEntity = rightMat(rReader);
                    if (!noTracking)
                    {
                        var entry = _ctx.ChangeTracker.Track(rightEntity, EntityState.Unchanged, rightMapping);
                        rightEntity = entry.Entity!;
                        NavigationPropertyExtensions.EnableLazyLoading(rightEntity, _ctx);
                    }
                    var rpk = jtm.GetRightKey(rightEntity);
                    if (rpk != null)
                        rightEntitiesByPk[rpk] = rightEntity;
                }
            }

            // Assign collections to parents.
            foreach (var p in parents.Cast<object>())
            {
                var leftPk = jtm.GetLeftKey(p);
                if (leftPk == null) continue;

                var collection = jtm.LeftCollectionGetter(p);
                if (collection == null)
                {
                    collection = QueryExecutor.CreateList(jtm.RightType, 0);
                    jtm.LeftCollectionSetter(p, collection);
                }

                if (!joinRows.TryGetValue(leftPk, out var rightPks))
                    rightPks = CoercedLookup(joinRows, leftPk);

                if (rightPks != null)
                {
                    foreach (var rPk in rightPks)
                    {
                        if (!rightEntitiesByPk.TryGetValue(rPk, out var rightEntity))
                            rightEntity = CoercedLookup(rightEntitiesByPk, rPk);

                        if (rightEntity != null)
                            collection.Add(rightEntity);
                    }
                }

                // Re-capture the parent's M2M snapshot now that its collection reflects
                // the loaded association set (including the empty case). The parent was
                // tracked BEFORE this include populated the collection, so its snapshot
                // (if any) is stale/empty — without this, a later collection edit computes
                // its delta against an empty baseline and removals are silently dropped.
                if (!noTracking)
                    _ctx.ChangeTracker.GetEntryOrDefault(p)?.CaptureManyToManySnapshots();
            }
        }

        /// <summary>
        /// Synchronous equivalent of <see cref="LoadManyToManyAsync"/> for use in the sync
        /// <c>Materialize</c> code path. Uses synchronous reader APIs so the sync path stays
        /// truly synchronous without any <c>GetAwaiter().GetResult()</c> calls.
        /// </summary>
        public void LoadManyToMany(M2MIncludePlan plan, IList parents, bool noTracking)
        {
            if (parents.Count == 0) return;
            var jtm = plan.JoinTable;

            var parentKeyValues = new Dictionary<object, object?[]>();
            foreach (var p in parents.Cast<object>())
            {
                var keyValues = jtm.GetLeftKeyValues(p);
                if (keyValues == null) continue;
                var pk = jtm.CreateLeftKeyFromValues(keyValues);
                if (pk == null) continue;
                parentKeyValues.TryAdd(pk, keyValues);
            }
            if (parentKeyValues.Count == 0) return;

            var leftKeyGroups = parentKeyValues.Values.ToList();
            var rightMapping = _ctx.GetMapping(jtm.RightType);

            // Reset collections to fresh empty lists before populating.
            foreach (var p in parents.Cast<object>())
            {
                var emptyList = QueryExecutor.CreateList(jtm.RightType, 0);
                jtm.LeftCollectionSetter(p, emptyList);
            }

            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();

            // Determine tenant filtering for the join table query and right entity query.
            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var tenantActive = _ctx.Options.TenantProvider != null;
            var tenantId = tenantActive ? _ctx.GetRequiredTenantId(leftMapping, "many-to-many include") : null;
            var leftTenantCol = tenantActive ? _ctx.RequireTenantColumn(leftMapping, "many-to-many include left side") : null;
            var hasTenantFilter = tenantActive;
            var leftPredicate = BuildColumnValuePredicate(cmd, _ctx.RawProvider.ParamPrefix, "jlfk", "jt", jtm.EscLeftFkColumns, leftKeyGroups);
            var selectedJoinColumns = string.Join(", ", jtm.EscLeftFkColumns.Concat(jtm.EscRightFkColumns).Select(c => "jt." + c));

            if (hasTenantFilter)
            {
                var tp = $"{_ctx.RawProvider.ParamPrefix}jttenant";
                var leftJoinPredicate = BuildColumnJoinPredicate("jt", jtm.EscLeftFkColumns, "lt", jtm.LeftKeyColumns);
                cmd.CommandText = $"SELECT {selectedJoinColumns} FROM {jtm.EscTableName} jt INNER JOIN {leftMapping.EscTable} lt ON {leftJoinPredicate} WHERE lt.{leftTenantCol!.EscCol} = {tp} AND {leftPredicate}";
                cmd.AddParam(tp, tenantId!);
            }
            else
            {
                cmd.CommandText = $"SELECT {string.Join(", ", jtm.EscLeftFkColumns.Concat(jtm.EscRightFkColumns))} FROM {jtm.EscTableName} jt WHERE {leftPredicate}";
            }
            cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

            var joinRows = new Dictionary<object, List<object>>();
            var allRightPks = new HashSet<object>();
            var rightKeyValues = new Dictionary<object, object?[]>();
            var leftKeyWidth = jtm.LeftFkColumns.Count;
            var rightKeyWidth = jtm.RightFkColumns.Count;
            using (var jReader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, System.Data.CommandBehavior.Default))
            {
                while (jReader.Read())
                {
                    var leftValues = ReadKeyValues(jReader, 0, leftKeyWidth);
                    var rightValues = ReadKeyValues(jReader, leftKeyWidth, rightKeyWidth);
                    if (leftValues == null || rightValues == null) continue;
                    var lPk = jtm.CreateLeftKeyFromValues(leftValues);
                    var rPk = jtm.CreateRightKeyFromValues(rightValues);
                    if (lPk == null || rPk == null) continue;
                    if (!joinRows.TryGetValue(lPk, out var rList))
                        joinRows[lPk] = rList = new List<object>();
                    rList.Add(rPk);
                    allRightPks.Add(rPk);
                    rightKeyValues.TryAdd(rPk, rightValues);
                }
            }

            if (allRightPks.Count == 0) return;

            using var cmd2 = _ctx.CreateCommand();
            var rightPredicate = BuildColumnValuePredicate(cmd2, _ctx.RawProvider.ParamPrefix, "jrpk", null, jtm.RightKeyColumns, rightKeyValues.Values.ToList());

            // If the right entity table also has a tenant column, filter right entities by tenant.
            var rightTenantCol = rightMapping.TenantColumn;
            if (tenantActive)
            {
                rightTenantCol = _ctx.RequireTenantColumn(rightMapping, "many-to-many include right side");
                var rtp = $"{_ctx.RawProvider.ParamPrefix}jrtenant";
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPredicate} AND {rightTenantCol.EscCol} = {rtp}";
                cmd2.AddParam(rtp, _ctx.GetRequiredTenantId(rightMapping, "many-to-many include right side"));
            }
            else
            {
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPredicate}";
            }
            // The right entity's global filters (soft-delete) restrict which related rows
            // are visible — the same rule the regular include levels apply. Without this,
            // filtered-out rows leak into the loaded collections.
            var rightGlobalFilter = GlobalFilterFragment.Build(_ctx, rightMapping, rightMapping.EscTable, cmd2);
            if (rightGlobalFilter != null)
                cmd2.CommandText += $" AND {rightGlobalFilter}";
            cmd2.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd2.CommandText);

            var rightEntitiesByPk = new Dictionary<object, object>();
            var rightMat = _materializerFactory.CreateSyncMaterializer(rightMapping, jtm.RightType);
            using (var rReader = cmd2.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, System.Data.CommandBehavior.Default))
            {
                while (rReader.Read())
                {
                    var rightEntity = rightMat(rReader);
                    if (!noTracking)
                    {
                        var entry = _ctx.ChangeTracker.Track(rightEntity, EntityState.Unchanged, rightMapping);
                        rightEntity = entry.Entity!;
                        NavigationPropertyExtensions.EnableLazyLoading(rightEntity, _ctx);
                    }
                    var rpk = jtm.GetRightKey(rightEntity);
                    if (rpk != null)
                        rightEntitiesByPk[rpk] = rightEntity;
                }
            }

            foreach (var p in parents.Cast<object>())
            {
                var leftPk = jtm.GetLeftKey(p);
                if (leftPk == null) continue;

                var collection = jtm.LeftCollectionGetter(p);
                if (collection == null)
                {
                    collection = QueryExecutor.CreateList(jtm.RightType, 0);
                    jtm.LeftCollectionSetter(p, collection);
                }

                if (!joinRows.TryGetValue(leftPk, out var rightPks))
                    rightPks = CoercedLookup(joinRows, leftPk);

                if (rightPks != null)
                {
                    foreach (var rPk in rightPks)
                    {
                        if (!rightEntitiesByPk.TryGetValue(rPk, out var rightEntity))
                            rightEntity = CoercedLookup(rightEntitiesByPk, rPk);

                        if (rightEntity != null)
                            collection.Add(rightEntity);
                    }
                }

                // See the async path: capture the loaded association set as the M2M
                // snapshot baseline so later collection edits diff against it.
                if (!noTracking)
                    _ctx.ChangeTracker.GetEntryOrDefault(p)?.CaptureManyToManySnapshots();
            }
        }

        /// <summary>
        /// Loads a many-to-many collection for a SHAPED PROJECTION — the parents are projected DTOs (or
        /// anonymous types), not the left entity, so the left key and the target collection member are resolved
        /// by NAME (via the split-query stitch helpers) rather than the mapping's entity-typed accessors. Runs
        /// the same two-phase bridge+related fetch as <see cref="LoadManyToMany"/>; related entities are kept
        /// transient (never tracked, since the result is a projection). Bare collection only — filtered/projected
        /// element bindings are rejected upstream at plan build for now.
        /// </summary>
        public void LoadManyToManyProjection(DependentQueryDefinition depQuery, IList parents)
        {
            if (parents.Count == 0) return;
            var jtm = depQuery.M2M!;
            var rightMapping = depQuery.TargetMapping;

            object?[]? LeftKeyValues(object p)
            {
                var kv = new object?[depQuery.ParentKeyProperties.Count];
                for (int i = 0; i < kv.Length; i++)
                {
                    kv[i] = QueryExecutor.ResolveOnParent(depQuery.ParentKeyProperties[i], p, depQuery).GetValue(p);
                    if (kv[i] == null) return null;
                }
                return kv;
            }

            void AssignEmpty(object p) => QueryExecutor.AssignCollectionToTarget(
                QueryExecutor.ResolveOnParent(depQuery.TargetCollectionProperty, p, depQuery), p,
                QueryExecutor.CreateList(depQuery.CollectionElementType, 0));

            var parentKeyValues = new Dictionary<object, object?[]>();
            foreach (var p in parents.Cast<object>())
            {
                var kv = LeftKeyValues(p);
                if (kv == null) continue;
                var pk = jtm.CreateLeftKeyFromValues(kv);
                if (pk != null) parentKeyValues.TryAdd(pk, kv);
            }
            if (parentKeyValues.Count == 0)
            {
                foreach (var p in parents.Cast<object>()) AssignEmpty(p);
                return;
            }

            var leftKeyGroups = parentKeyValues.Values.ToList();
            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();

            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var tenantActive = _ctx.Options.TenantProvider != null;
            var leftPredicate = BuildColumnValuePredicate(cmd, _ctx.RawProvider.ParamPrefix, "jlfk", "jt", jtm.EscLeftFkColumns, leftKeyGroups);
            var selectedJoinColumns = string.Join(", ", jtm.EscLeftFkColumns.Concat(jtm.EscRightFkColumns).Select(c => "jt." + c));
            if (tenantActive)
            {
                var leftTenantCol = _ctx.RequireTenantColumn(leftMapping, "many-to-many shaped projection left side");
                var tp = $"{_ctx.RawProvider.ParamPrefix}jttenant";
                var leftJoinPredicate = BuildColumnJoinPredicate("jt", jtm.EscLeftFkColumns, "lt", jtm.LeftKeyColumns);
                cmd.CommandText = $"SELECT {selectedJoinColumns} FROM {jtm.EscTableName} jt INNER JOIN {leftMapping.EscTable} lt ON {leftJoinPredicate} WHERE lt.{leftTenantCol.EscCol} = {tp} AND {leftPredicate}";
                cmd.AddParam(tp, _ctx.GetRequiredTenantId(leftMapping, "many-to-many shaped projection"));
            }
            else
            {
                cmd.CommandText = $"SELECT {selectedJoinColumns} FROM {jtm.EscTableName} jt WHERE {leftPredicate}";
            }
            cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

            var joinRows = new Dictionary<object, List<object>>();
            var allRightPks = new HashSet<object>();
            var rightKeyValues = new Dictionary<object, object?[]>();
            var leftKeyWidth = jtm.LeftFkColumns.Count;
            var rightKeyWidth = jtm.RightFkColumns.Count;
            using (var jReader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, System.Data.CommandBehavior.Default))
            {
                while (jReader.Read())
                {
                    var leftValues = ReadKeyValues(jReader, 0, leftKeyWidth);
                    var rightValues = ReadKeyValues(jReader, leftKeyWidth, rightKeyWidth);
                    if (leftValues == null || rightValues == null) continue;
                    var lPk = jtm.CreateLeftKeyFromValues(leftValues);
                    var rPk = jtm.CreateRightKeyFromValues(rightValues);
                    if (lPk == null || rPk == null) continue;
                    if (!joinRows.TryGetValue(lPk, out var rList)) joinRows[lPk] = rList = new List<object>();
                    rList.Add(rPk);
                    allRightPks.Add(rPk);
                    rightKeyValues.TryAdd(rPk, rightValues);
                }
            }

            var rightEntitiesByPk = new Dictionary<object, object>();
            if (allRightPks.Count > 0)
            {
                using var cmd2 = _ctx.CreateCommand();
                var rightPredicate = BuildColumnValuePredicate(cmd2, _ctx.RawProvider.ParamPrefix, "jrpk", null, jtm.RightKeyColumns, rightKeyValues.Values.ToList());
                if (tenantActive)
                {
                    var rightTenantCol = _ctx.RequireTenantColumn(rightMapping, "many-to-many shaped projection right side");
                    var rtp = $"{_ctx.RawProvider.ParamPrefix}jrtenant";
                    cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPredicate} AND {rightTenantCol.EscCol} = {rtp}";
                    cmd2.AddParam(rtp, _ctx.GetRequiredTenantId(rightMapping, "many-to-many shaped projection right side"));
                }
                else
                {
                    cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPredicate}";
                }
                var rightGlobalFilter = GlobalFilterFragment.Build(_ctx, rightMapping, rightMapping.EscTable, cmd2);
                if (rightGlobalFilter != null) cmd2.CommandText += $" AND {rightGlobalFilter}";
                cmd2.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd2.CommandText);

                var rightMat = _materializerFactory.CreateSyncMaterializer(rightMapping, jtm.RightType);
                using var rReader = cmd2.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, System.Data.CommandBehavior.Default);
                while (rReader.Read())
                {
                    var rightEntity = rightMat(rReader);   // transient — a projected result is never tracked
                    var rpk = jtm.GetRightKey(rightEntity);
                    if (rpk != null) rightEntitiesByPk[rpk] = rightEntity;
                }
            }

            foreach (var p in parents.Cast<object>())
            {
                var collection = QueryExecutor.CreateList(depQuery.CollectionElementType, 0);
                var kv = LeftKeyValues(p);
                var leftPk = kv != null ? jtm.CreateLeftKeyFromValues(kv) : null;
                if (leftPk != null)
                {
                    if (!joinRows.TryGetValue(leftPk, out var rightPks)) rightPks = CoercedLookup(joinRows, leftPk);
                    if (rightPks != null)
                        foreach (var rPk in rightPks)
                        {
                            if (!rightEntitiesByPk.TryGetValue(rPk, out var rightEntity)) rightEntity = CoercedLookup(rightEntitiesByPk, rPk);
                            if (rightEntity != null) collection.Add(rightEntity);
                        }
                }
                QueryExecutor.AssignCollectionToTarget(QueryExecutor.ResolveOnParent(depQuery.TargetCollectionProperty, p, depQuery), p, collection);
            }
        }
    }
}
