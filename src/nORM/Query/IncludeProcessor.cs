using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Execution;

namespace nORM.Query
{
    /// <summary>
    /// Handles eager loading of navigation properties. Uses multi-result-set queries
    /// for one-to-many chains (<see cref="EagerLoadAsync"/>/<see cref="EagerLoad"/>)
    /// and separate paired queries for many-to-many relationships
    /// (<see cref="LoadManyToManyAsync"/>/<see cref="LoadManyToMany"/>).
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("IncludeProcessor builds expressions via reflection; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("IncludeProcessor reflects over navigation property metadata; trimming may remove the required members.")]
    internal sealed class IncludeProcessor
    {
        private readonly DbContext _ctx;
        private readonly MaterializerFactory _materializerFactory = new();

        /// <summary>
        /// Number of parameter slots reserved for internal use (tenant params, etc.)
        /// when computing the maximum keys per eager-load batch.
        /// Mirrors <c>DatabaseProvider.ParameterReserve</c>.
        /// </summary>
        private const int ParameterReserve = 10;

        /// <summary>
        /// Eagerly loads a many-to-many relationship for the given parent entities by
        /// querying the join table and then loading the related entities.
        /// </summary>
        public async Task LoadManyToManyAsync(M2MIncludePlan plan, IList parents, CancellationToken ct, bool noTracking)
        {
            if (parents.Count == 0) return;
            var jtm = plan.JoinTable;

            // Collect left PKs
            var parentByPk = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var pk = jtm.LeftPkGetter(p);
                if (pk == null) continue;
                if (!parentByPk.TryGetValue(pk, out var list))
                    parentByPk[pk] = list = new List<object>();
                list.Add(p);
            }
            if (parentByPk.Count == 0) return;

            // Build a query: SELECT right_fk, right_pk FROM join_table INNER JOIN right_table ON ... WHERE left_fk IN (...)
            // Simpler: SELECT right_fk FROM join_table WHERE left_fk IN (...) → then load right entities
            var pkeys = parentByPk.Keys.ToArray();
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

            // Build IN clause params for the join table query
            var paramNames = new List<string>();
            for (int i = 0; i < pkeys.Length; i++)
            {
                var pn = $"{_ctx.RawProvider.ParamPrefix}jlfk{i}";
                cmd.AddParam(pn, pkeys[i]!);
                paramNames.Add(pn);
            }
            var inClause = $"({string.Join(", ", paramNames)})";

            // Determine tenant filtering for the join table query and right entity query.
            // When multi-tenancy is active on the left entity, scope the join table query to
            // only return rows whose left FK belongs to the current tenant.
            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var tenantActive = _ctx.Options.TenantProvider != null;
            var tenantId = tenantActive ? _ctx.GetRequiredTenantId(leftMapping, "many-to-many include") : null;
            var leftTenantCol = tenantActive ? _ctx.RequireTenantColumn(leftMapping, "many-to-many include left side") : null;
            // M2M requires single-column PK on both sides; guard early.
            if (leftMapping.KeyColumns.Length != 1)
                throw new NormConfigurationException(
                    $"Many-to-many Include on '{leftMapping.Type.Name}' requires a single-column primary key. " +
                    "Map composite-key join tables as explicit join entities, or use a single-column surrogate key for skip-navigation many-to-many.");
            var leftPkCol = leftMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var hasTenantFilter = tenantActive;

            // Query: SELECT jt.left_fk, jt.right_fk FROM join_table jt [INNER JOIN left_table lt ON ...] WHERE jt.left_fk IN (...)
            if (hasTenantFilter)
            {
                var tp = $"{_ctx.RawProvider.ParamPrefix}jttenant";
                cmd.CommandText = $"SELECT jt.{jtm.EscLeftFkColumn}, jt.{jtm.EscRightFkColumn} FROM {jtm.EscTableName} jt INNER JOIN {leftMapping.EscTable} lt ON jt.{jtm.EscLeftFkColumn} = lt.{leftPkCol!.EscCol} WHERE lt.{leftTenantCol!.EscCol} = {tp} AND jt.{jtm.EscLeftFkColumn} IN {inClause}";
                cmd.AddParam(tp, tenantId!);
            }
            else
            {
                cmd.CommandText = $"SELECT {jtm.EscLeftFkColumn}, {jtm.EscRightFkColumn} FROM {jtm.EscTableName} WHERE {jtm.EscLeftFkColumn} IN {inClause}";
            }
            cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

            // Read all join rows: leftPk → list of rightPks
            var joinRows = new Dictionary<object, List<object>>();
            var allRightPks = new HashSet<object>();
            await using (var jReader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, System.Data.CommandBehavior.Default, ct).ConfigureAwait(false))
            {
                while (await jReader.ReadAsync(ct).ConfigureAwait(false))
                {
                    if (jReader.IsDBNull(0) || jReader.IsDBNull(1)) continue;
                    var lPk = jReader.GetValue(0);
                    var rPk = jReader.GetValue(1);
                    if (!joinRows.TryGetValue(lPk, out var rList))
                        joinRows[lPk] = rList = new List<object>();
                    rList.Add(rPk);
                    allRightPks.Add(rPk);
                }
            }

            if (allRightPks.Count == 0) return;

            // Load related (right) entities in a single query
            await using var cmd2 = _ctx.CreateCommand();
            var rightParamNames = new List<string>();
            var allRightPkList = allRightPks.ToList();
            for (int i = 0; i < allRightPkList.Count; i++)
            {
                var pn = $"{_ctx.RawProvider.ParamPrefix}jrpk{i}";
                cmd2.AddParam(pn, allRightPkList[i]!);
                rightParamNames.Add(pn);
            }
            // M2M requires single-column PK on the right side; guard early.
            if (rightMapping.KeyColumns.Length != 1)
                throw new NormConfigurationException(
                    $"Many-to-many Include on '{rightMapping.Type.Name}' requires a single-column primary key. " +
                    "Map composite-key join tables as explicit join entities, or use a single-column surrogate key for skip-navigation many-to-many.");
            var rightPkCol = rightMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var rightInClause = $"({string.Join(", ", rightParamNames)})";

            // If the right entity table also has a tenant column, filter right entities by tenant
            var rightTenantCol = rightMapping.TenantColumn;
            if (tenantActive)
            {
                rightTenantCol = _ctx.RequireTenantColumn(rightMapping, "many-to-many include right side");
                var rtp = $"{_ctx.RawProvider.ParamPrefix}jrtenant";
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause} AND {rightTenantCol.EscCol} = {rtp}";
                cmd2.AddParam(rtp, _ctx.GetRequiredTenantId(rightMapping, "many-to-many include right side"));
            }
            else
            {
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause}";
            }
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
                    var rpk = rightPkCol.Getter(rightEntity);
                    if (rpk != null)
                        rightEntitiesByPk[rpk] = rightEntity;
                }
            }

            // Assign collections to parents
            foreach (var p in parents.Cast<object>())
            {
                var leftPk = jtm.LeftPkGetter(p);
                if (leftPk == null) continue;

                var collection = jtm.LeftCollectionGetter(p);
                if (collection == null)
                {
                    collection = QueryExecutor.CreateList(jtm.RightType, 0);
                    jtm.LeftCollectionSetter(p, collection);
                }

                // Try direct PK lookup, then coerced lookup (SQLite returns Int64 for int PKs)
                if (!joinRows.TryGetValue(leftPk, out var rightPks))
                {
                    rightPks = CoercedLookup(joinRows, leftPk);
                }

                if (rightPks == null) continue;

                foreach (var rPk in rightPks)
                {
                    // Try direct lookup, then coerced
                    if (!rightEntitiesByPk.TryGetValue(rPk, out var rightEntity))
                    {
                        rightEntity = CoercedLookup(rightEntitiesByPk, rPk);
                    }
                    if (rightEntity != null)
                        collection.Add(rightEntity);
                }
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

            var parentByPk = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var pk = jtm.LeftPkGetter(p);
                if (pk == null) continue;
                if (!parentByPk.TryGetValue(pk, out var list))
                    parentByPk[pk] = list = new List<object>();
                list.Add(p);
            }
            if (parentByPk.Count == 0) return;

            var pkeys = parentByPk.Keys.ToArray();
            var rightMapping = _ctx.GetMapping(jtm.RightType);

            // Reset collections to fresh empty lists before populating.
            foreach (var p in parents.Cast<object>())
            {
                var emptyList = QueryExecutor.CreateList(jtm.RightType, 0);
                jtm.LeftCollectionSetter(p, emptyList);
            }

            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();

            var paramNames = new List<string>();
            for (int i = 0; i < pkeys.Length; i++)
            {
                var pn = $"{_ctx.RawProvider.ParamPrefix}jlfk{i}";
                cmd.AddParam(pn, pkeys[i]!);
                paramNames.Add(pn);
            }
            var inClause = $"({string.Join(", ", paramNames)})";

            // Determine tenant filtering for the join table query and right entity query.
            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var tenantActive = _ctx.Options.TenantProvider != null;
            var tenantId = tenantActive ? _ctx.GetRequiredTenantId(leftMapping, "many-to-many include") : null;
            var leftTenantCol = tenantActive ? _ctx.RequireTenantColumn(leftMapping, "many-to-many include left side") : null;
            // M2M requires single-column PK on both sides; guard early.
            if (leftMapping.KeyColumns.Length != 1)
                throw new NormConfigurationException(
                    $"Many-to-many Include on '{leftMapping.Type.Name}' requires a single-column primary key. " +
                    "Map composite-key join tables as explicit join entities, or use a single-column surrogate key for skip-navigation many-to-many.");
            var leftPkCol = leftMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var hasTenantFilter = tenantActive;

            if (hasTenantFilter)
            {
                var tp = $"{_ctx.RawProvider.ParamPrefix}jttenant";
                cmd.CommandText = $"SELECT jt.{jtm.EscLeftFkColumn}, jt.{jtm.EscRightFkColumn} FROM {jtm.EscTableName} jt INNER JOIN {leftMapping.EscTable} lt ON jt.{jtm.EscLeftFkColumn} = lt.{leftPkCol!.EscCol} WHERE lt.{leftTenantCol!.EscCol} = {tp} AND jt.{jtm.EscLeftFkColumn} IN {inClause}";
                cmd.AddParam(tp, tenantId!);
            }
            else
            {
                cmd.CommandText = $"SELECT {jtm.EscLeftFkColumn}, {jtm.EscRightFkColumn} FROM {jtm.EscTableName} WHERE {jtm.EscLeftFkColumn} IN {inClause}";
            }
            cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

            var joinRows = new Dictionary<object, List<object>>();
            var allRightPks = new HashSet<object>();
            using (var jReader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, System.Data.CommandBehavior.Default))
            {
                while (jReader.Read())
                {
                    if (jReader.IsDBNull(0) || jReader.IsDBNull(1)) continue;
                    var lPk = jReader.GetValue(0);
                    var rPk = jReader.GetValue(1);
                    if (!joinRows.TryGetValue(lPk, out var rList))
                        joinRows[lPk] = rList = new List<object>();
                    rList.Add(rPk);
                    allRightPks.Add(rPk);
                }
            }

            if (allRightPks.Count == 0) return;

            using var cmd2 = _ctx.CreateCommand();
            var rightParamNames = new List<string>();
            var allRightPkList = allRightPks.ToList();
            for (int i = 0; i < allRightPkList.Count; i++)
            {
                var pn = $"{_ctx.RawProvider.ParamPrefix}jrpk{i}";
                cmd2.AddParam(pn, allRightPkList[i]!);
                rightParamNames.Add(pn);
            }
            // M2M requires single-column PK on the right side; guard early.
            if (rightMapping.KeyColumns.Length != 1)
                throw new NormConfigurationException(
                    $"Many-to-many Include on '{rightMapping.Type.Name}' requires a single-column primary key. " +
                    "Map composite-key join tables as explicit join entities, or use a single-column surrogate key for skip-navigation many-to-many.");
            var rightPkCol = rightMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var rightInClause = $"({string.Join(", ", rightParamNames)})";

            // If the right entity table also has a tenant column, filter right entities by tenant
            var rightTenantCol = rightMapping.TenantColumn;
            if (tenantActive)
            {
                rightTenantCol = _ctx.RequireTenantColumn(rightMapping, "many-to-many include right side");
                var rtp = $"{_ctx.RawProvider.ParamPrefix}jrtenant";
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause} AND {rightTenantCol.EscCol} = {rtp}";
                cmd2.AddParam(rtp, _ctx.GetRequiredTenantId(rightMapping, "many-to-many include right side"));
            }
            else
            {
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause}";
            }
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
                    var rpk = rightPkCol.Getter(rightEntity);
                    if (rpk != null)
                        rightEntitiesByPk[rpk] = rightEntity;
                }
            }

            foreach (var p in parents.Cast<object>())
            {
                var leftPk = jtm.LeftPkGetter(p);
                if (leftPk == null) continue;

                var collection = jtm.LeftCollectionGetter(p);
                if (collection == null)
                {
                    collection = QueryExecutor.CreateList(jtm.RightType, 0);
                    jtm.LeftCollectionSetter(p, collection);
                }

                if (!joinRows.TryGetValue(leftPk, out var rightPks))
                {
                    rightPks = CoercedLookup(joinRows, leftPk);
                }

                if (rightPks == null) continue;

                foreach (var rPk in rightPks)
                {
                    if (!rightEntitiesByPk.TryGetValue(rPk, out var rightEntity))
                    {
                        rightEntity = CoercedLookup(rightEntitiesByPk, rPk);
                    }
                    if (rightEntity != null)
                        collection.Add(rightEntity);
                }
            }
        }

        public IncludeProcessor(DbContext ctx) => _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));

        /// <summary>
        /// Eagerly loads all relations defined in the <paramref name="include"/> for the given <paramref name="parents"/>
        /// using a single round trip per batch of keys.
        /// </summary>
        public async Task EagerLoadAsync(IncludePlan include, IList parents, CancellationToken ct, bool noTracking)
        {
            if (parents.Count == 0 || include.Path.Count == 0)
                return;

            var pathMappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();

            var firstRelation = include.Path[0];
            if (include.Path.Count > 1 && include.Path.Any(r => r.IsComposite))
                throw new NormUnsupportedFeatureException(
                    "Multi-level Include over composite-key relationships is not supported yet. Split the query into explicit loads or configure single-column relationship keys.");

            // Build lookup of parent entities by key to allow batching
            var parentLookup = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var key = GetPrincipalKeyValue(firstRelation, p);
                if (key == null)
                {
                    AssignEmptyList(p, firstRelation);
                    continue;
                }

                if (!parentLookup.TryGetValue(key, out var list))
                    parentLookup[key] = list = new List<object>();
                list.Add(p);
            }

            if (parentLookup.Count == 0)
                return;

            // Pre-compute mappings and materializers for the relations (reuse pathMappings computed in guard above)
            var mappings = pathMappings;
            var materializers = mappings
                .Select(m => _materializerFactory.CreateMaterializer(m, m.Type))
                .ToArray();

            // Determine optimal batch size based on provider parameter limits and relationship depth
            var keys = parentLookup.Keys.ToArray();
            var maxParams = _ctx.RawProvider.MaxParameters;
            var firstKeyWidth = Math.Max(1, firstRelation.PrincipalKeys.Count);
            var maxPerBatch = maxParams == int.MaxValue
                ? keys.Length
                : Math.Max(1, (maxParams - ParameterReserve) / Math.Max(1, include.Path.Count * firstKeyWidth));

            foreach (var keyBatch in keys.Chunk(maxPerBatch))
            {
                ct.ThrowIfCancellationRequested();

                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.CreateCommand();

                var paramNames = new List<string>();
                var paramGroups = new List<string[]>();
                for (int i = 0; i < keyBatch.Length; i++)
                {
                    var values = GetKeyValues(keyBatch[i]);
                    var group = new string[values.Length];
                    for (var j = 0; j < values.Length; j++)
                    {
                        var pn = $"{_ctx.RawProvider.ParamPrefix}fk{i}_{j}";
                        cmd.AddParam(pn, values[j]!);
                        group[j] = pn;
                        if (values.Length == 1)
                            paramNames.Add(pn);
                    }
                    paramGroups.Add(group);
                }

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames, paramGroups, cmd);
                cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default, ct).ConfigureAwait(false);

                // Collect parent objects for the current batch
                IList currentParents = keyBatch.SelectMany(k => parentLookup[k]).ToList();

                for (int level = 0; level < include.Path.Count; level++)
                {
                    currentParents = await ProcessLevelAsync(include.Path[level], mappings[level], materializers[level], currentParents, reader, ct, noTracking).ConfigureAwait(false);
                    if (level < include.Path.Count - 1)
                        await reader.NextResultAsync(ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Truly synchronous variant of <see cref="EagerLoadAsync"/>. Uses synchronous
        /// ADO.NET methods so no thread is blocked waiting for async work to complete.
        /// Called from the synchronous <c>Materialize</c> code path to eliminate
        /// sync-over-async anti-pattern.
        /// </summary>
        public void EagerLoad(IncludePlan include, IList parents, bool noTracking)
        {
            if (parents.Count == 0 || include.Path.Count == 0)
                return;

            var pathMappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();

            var firstRelation = include.Path[0];
            if (include.Path.Count > 1 && include.Path.Any(r => r.IsComposite))
                throw new NormUnsupportedFeatureException(
                    "Multi-level Include over composite-key relationships is not supported yet. Split the query into explicit loads or configure single-column relationship keys.");

            var parentLookup = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var key = GetPrincipalKeyValue(firstRelation, p);
                if (key == null)
                {
                    AssignEmptyList(p, firstRelation);
                    continue;
                }

                if (!parentLookup.TryGetValue(key, out var list))
                    parentLookup[key] = list = new List<object>();
                list.Add(p);
            }

            if (parentLookup.Count == 0)
                return;

            // Reuse pathMappings computed in guard above
            var mappings = pathMappings;
            var syncMaterializers = mappings
                .Select(m => _materializerFactory.CreateSyncMaterializer(m, m.Type))
                .ToArray();

            var keys = parentLookup.Keys.ToArray();
            var maxParams = _ctx.RawProvider.MaxParameters;
            var firstKeyWidth = Math.Max(1, firstRelation.PrincipalKeys.Count);
            var maxPerBatch = maxParams == int.MaxValue
                ? keys.Length
                : Math.Max(1, (maxParams - ParameterReserve) / Math.Max(1, include.Path.Count * firstKeyWidth));

            foreach (var keyBatch in keys.Chunk(maxPerBatch))
            {
                _ctx.EnsureConnection();
                using var cmd = _ctx.CreateCommand();

                var paramNames = new List<string>();
                var paramGroups = new List<string[]>();
                for (int i = 0; i < keyBatch.Length; i++)
                {
                    var values = GetKeyValues(keyBatch[i]);
                    var group = new string[values.Length];
                    for (var j = 0; j < values.Length; j++)
                    {
                        var pn = $"{_ctx.RawProvider.ParamPrefix}fk{i}_{j}";
                        cmd.AddParam(pn, values[j]!);
                        group[j] = pn;
                        if (values.Length == 1)
                            paramNames.Add(pn);
                    }
                    paramGroups.Add(group);
                }

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames, paramGroups, cmd);
                cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

                using var reader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, System.Data.CommandBehavior.Default);

                IList currentParents = keyBatch.SelectMany(k => parentLookup[k]).ToList();

                for (int level = 0; level < include.Path.Count; level++)
                {
                    currentParents = ProcessLevel(include.Path[level], mappings[level], syncMaterializers[level], currentParents, reader, noTracking);
                    if (level < include.Path.Count - 1)
                        reader.NextResult();
                }
            }
        }

        private IList ProcessLevel(
            TableMapping.Relation relation,
            TableMapping childMap,
            Func<DbDataReader, object> syncMaterializer,
            IList parents,
            DbDataReader reader,
            bool noTracking)
        {
            var resultChildren = new List<object>();
            var childGroups = new Dictionary<object, List<object>>();

            while (reader.Read())
            {
                var child = syncMaterializer(reader);
                if (!noTracking)
                {
                    var entry = _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
                    child = entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                }

                resultChildren.Add(child);

                var fk = GetForeignKeyValue(relation, child);
                if (fk != null)
                {
                    if (!childGroups.TryGetValue(fk, out var list))
                        childGroups[fk] = list = new List<object>();
                    list.Add(child);
                }
            }

            foreach (var p in parents.Cast<object>())
            {
                var pk = GetPrincipalKeyValue(relation, p);
                var childList = QueryExecutor.CreateList(relation.DependentType, 0);

                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    foreach (var item in c)
                        childList.Add(item);
                }

                relation.NavProp.SetValue(p, childList);
            }

            return resultChildren;
        }

        private static void AssignEmptyList(object parent, TableMapping.Relation relation)
        {
            var childList = QueryExecutor.CreateList(relation.DependentType, 0);
            relation.NavProp.SetValue(parent, childList);
        }

        private static object? GetPrincipalKeyValue(TableMapping.Relation relation, object entity)
            => GetRelationKeyValue(relation.PrincipalKeys, entity);

        private static object? GetForeignKeyValue(TableMapping.Relation relation, object entity)
            => GetRelationKeyValue(relation.ForeignKeys, entity);

        private static object? GetRelationKeyValue(IReadOnlyList<Column> columns, object entity)
        {
            if (columns.Count == 1)
                return columns[0].Getter(entity);

            var values = new object?[columns.Count];
            for (var i = 0; i < columns.Count; i++)
            {
                values[i] = columns[i].Getter(entity);
                if (values[i] is null)
                    return null;
            }

            return new RelationKey(values);
        }

        private static object?[] GetKeyValues(object key)
            => key is RelationKey composite ? composite.Values : new object?[] { key };

        private static void AppendCompositeKeyPredicate(System.Text.StringBuilder sb, TableMapping.Relation relation, List<string[]> paramGroups)
        {
            if (paramGroups.Count == 0)
            {
                sb.Append("1 = 0");
                return;
            }

            sb.Append('(');
            for (var groupIndex = 0; groupIndex < paramGroups.Count; groupIndex++)
            {
                if (groupIndex > 0)
                    sb.Append(" OR ");

                var group = paramGroups[groupIndex];
                if (group.Length != relation.ForeignKeys.Count)
                    throw new NormConfigurationException(
                        $"Composite Include relationship '{relation.NavProp.Name}' expected {relation.ForeignKeys.Count} key values but received {group.Length}.");

                sb.Append('(');
                for (var columnIndex = 0; columnIndex < relation.ForeignKeys.Count; columnIndex++)
                {
                    if (columnIndex > 0)
                        sb.Append(" AND ");
                    sb.Append(relation.ForeignKeys[columnIndex].EscCol)
                      .Append(" = ")
                      .Append(group[columnIndex]);
                }
                sb.Append(')');
            }
            sb.Append(')');
        }

        private string BuildSql(IReadOnlyList<TableMapping.Relation> path, TableMapping[] mappings, List<string> paramNames, List<string[]> paramGroups, DbCommand cmd)
        {
            var tenantActive = _ctx.Options.TenantProvider != null;
            var current = $"({PooledStringBuilder.Join(paramNames, ",")})";
            var sb = PooledStringBuilder.Rent();
            try
            {
                for (int i = 0; i < path.Count; i++)
                {
                    var relation = path[i];
                    var map = mappings[i];
                    var tenantCol = tenantActive ? _ctx.RequireTenantColumn(map, "include path load") : null;

                    sb.Append("SELECT * FROM ").Append(map.EscTable)
                      .Append(" WHERE ");
                    if (i == 0 && relation.IsComposite)
                    {
                        AppendCompositeKeyPredicate(sb, relation, paramGroups);
                    }
                    else
                    {
                        sb.Append(relation.ForeignKey.EscCol)
                          .Append(" IN ").Append(current);
                    }

                    if (tenantActive)
                    {
                        var tp = $"{_ctx.RawProvider.ParamPrefix}tkn{i}";
                        sb.Append(" AND ").Append(tenantCol!.EscCol).Append(" = ").Append(tp);
                        cmd.AddParam(tp, _ctx.GetRequiredTenantId(map, "include path load"));
                    }

                    // Separate multiple result-set statements with semicolons;
                    // skip the trailing one so the final SQL is clean.
                    if (i < path.Count - 1)
                        sb.Append(';');

                    // Build the subquery that feeds the NEXT level's IN clause.
                    // Select only the single column that the next relation's FK references (the
                    // next relation's PrincipalKey), not all PK columns of the current mapping.
                    // Selecting composite PK columns would produce a multi-column subquery that
                    // the next level's single-column IN cannot consume.
                    if (i + 1 < path.Count)
                    {
                        var nextPrincipalKey = path[i + 1].PrincipalKey.EscCol;
                        var tenantPart = tenantActive
                            ? $" AND {tenantCol!.EscCol} = {_ctx.RawProvider.ParamPrefix}tkn{i}"
                            : string.Empty;
                        current = $"(SELECT {nextPrincipalKey} FROM {map.EscTable} WHERE {relation.ForeignKey.EscCol} IN {current}{tenantPart})";
                    }
                }

                return sb.ToString();
            }
            finally
            {
                PooledStringBuilder.Return(sb);
            }
        }

        /// <summary>
        /// Reads a single level of related entities from the <paramref name="reader"/> and
        /// associates them with their corresponding parent objects. The method materializes
        /// each record, optionally tracks it, and groups children by foreign key so they can
        /// be assigned back to their principals.
        /// </summary>
        /// <param name="relation">The relationship metadata describing the association.</param>
        /// <param name="childMap">Mapping information for the dependent type.</param>
        /// <param name="materializer">Delegate that converts a data record into an entity.</param>
        /// <param name="parents">The parent entities for which related data is being loaded.</param>
        /// <param name="reader">Data reader positioned at the result set for this level.</param>
        /// <param name="ct">Cancellation token for the asynchronous operation.</param>
        /// <param name="noTracking">If <c>true</c>, entities are not tracked by the context.</param>
        /// <returns>A list of all materialized child entities for the level.</returns>
        private async Task<IList> ProcessLevelAsync(
            TableMapping.Relation relation,
            TableMapping childMap,
            Func<DbDataReader, CancellationToken, Task<object>> materializer,
            IList parents,
            DbDataReader reader,
            CancellationToken ct,
            bool noTracking)
        {
            var resultChildren = new List<object>();
            var childGroups = new Dictionary<object, List<object>>();

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                var child = await materializer(reader, ct).ConfigureAwait(false);
                if (!noTracking)
                {
                    var entry = _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
                    child = entry.Entity!;
                    NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                }

                resultChildren.Add(child);

                var fk = GetForeignKeyValue(relation, child);
                if (fk != null)
                {
                    if (!childGroups.TryGetValue(fk, out var list))
                        childGroups[fk] = list = new List<object>();
                    list.Add(child);
                }
            }

            foreach (var p in parents.Cast<object>())
            {
                ct.ThrowIfCancellationRequested();
                var pk = GetPrincipalKeyValue(relation, p);
                var childList = QueryExecutor.CreateList(relation.DependentType, 0);

                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    foreach (var item in c)
                        childList.Add(item);
                }

                relation.NavProp.SetValue(p, childList);
            }

            return resultChildren;
        }

        /// <summary>
        /// Performs a coerced lookup in a dictionary by comparing the string representation
        /// of keys. This handles SQLite returning Int64 for int PKs and similar type mismatches.
        /// Returns <c>default</c> if no match is found. Skips null string representations
        /// to avoid false-positive matches between unrelated DBNull/null values.
        /// </summary>
        private static TValue? CoercedLookup<TValue>(Dictionary<object, TValue> dict, object key)
        {
            var keyStr = Convert.ToString(key, System.Globalization.CultureInfo.InvariantCulture);
            if (keyStr == null) return default;
            foreach (var candidate in dict.Keys)
            {
                var candidateStr = Convert.ToString(candidate, System.Globalization.CultureInfo.InvariantCulture);
                if (candidateStr != null && candidateStr == keyStr)
                    return dict[candidate];
            }
            return default;
        }

        private sealed class RelationKey : IEquatable<RelationKey>
        {
            public object?[] Values { get; }

            public RelationKey(object?[] values) => Values = values;

            public bool Equals(RelationKey? other)
            {
                if (other is null || other.Values.Length != Values.Length)
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
                var hash = new HashCode();
                foreach (var value in Values)
                    hash.Add(value);
                return hash.ToHashCode();
            }

            public override string ToString()
                => string.Join("|", Values.Select(v => Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)));
        }

        /// <summary>
        /// Computes a safe integer command timeout from the adaptive timeout provider,
        /// clamping the result to [0, <see cref="int.MaxValue"/>] and treating NaN/negative
        /// as zero (use provider default).
        /// </summary>
        private int SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType opType, string sql)
        {
            var totalSeconds = _ctx.GetAdaptiveTimeout(opType, sql).TotalSeconds;
            if (double.IsNaN(totalSeconds) || totalSeconds < 0)
                return 0;
            return (int)Math.Min(totalSeconds, int.MaxValue);
        }
    }
}

