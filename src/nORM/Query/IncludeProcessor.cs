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
                var emptyList = (System.Collections.IList)Activator.CreateInstance(
                    typeof(List<>).MakeGenericType(jtm.RightType))!;
                jtm.LeftCollectionSetter(p, emptyList);
            }

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();

            // Build IN clause params for the join table query
            var paramNames = new List<string>();
            for (int i = 0; i < pkeys.Length; i++)
            {
                var pn = $"{_ctx.Provider.ParamPrefix}jlfk{i}";
                cmd.AddParam(pn, pkeys[i]!);
                paramNames.Add(pn);
            }
            var inClause = $"({string.Join(", ", paramNames)})";

            // Determine tenant filtering for the join table query and right entity query.
            // When multi-tenancy is active on the left entity, scope the join table query to
            // only return rows whose left FK belongs to the current tenant.
            var tenantId = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var leftTenantCol = leftMapping.TenantColumn;
            // M2M requires single-column PK on both sides; guard early.
            if (leftMapping.KeyColumns.Length == 0)
                throw new InvalidOperationException(
                    $"Many-to-many Include on '{leftMapping.Type.Name}' failed: entity has no primary key columns.");
            var leftPkCol = leftMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var hasTenantFilter = tenantId != null && leftTenantCol != null;

            // Query: SELECT jt.left_fk, jt.right_fk FROM join_table jt [INNER JOIN left_table lt ON ...] WHERE jt.left_fk IN (...)
            if (hasTenantFilter)
            {
                var tp = $"{_ctx.Provider.ParamPrefix}jttenant";
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
                var pn = $"{_ctx.Provider.ParamPrefix}jrpk{i}";
                cmd2.AddParam(pn, allRightPkList[i]!);
                rightParamNames.Add(pn);
            }
            // M2M requires single-column PK on the right side; guard early.
            if (rightMapping.KeyColumns.Length == 0)
                throw new InvalidOperationException(
                    $"Many-to-many Include on '{rightMapping.Type.Name}' failed: entity has no primary key columns.");
            var rightPkCol = rightMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var rightInClause = $"({string.Join(", ", rightParamNames)})";

            // If the right entity table also has a tenant column, filter right entities by tenant
            var rightTenantCol = rightMapping.TenantColumn;
            if (tenantId != null && rightTenantCol != null)
            {
                var rtp = $"{_ctx.Provider.ParamPrefix}jrtenant";
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause} AND {rightTenantCol.EscCol} = {rtp}";
                cmd2.AddParam(rtp, tenantId);
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
                    collection = (System.Collections.IList)Activator.CreateInstance(
                        typeof(List<>).MakeGenericType(jtm.RightType))!;
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
                var emptyList = (IList)Activator.CreateInstance(
                    typeof(List<>).MakeGenericType(jtm.RightType))!;
                jtm.LeftCollectionSetter(p, emptyList);
            }

            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();

            var paramNames = new List<string>();
            for (int i = 0; i < pkeys.Length; i++)
            {
                var pn = $"{_ctx.Provider.ParamPrefix}jlfk{i}";
                cmd.AddParam(pn, pkeys[i]!);
                paramNames.Add(pn);
            }
            var inClause = $"({string.Join(", ", paramNames)})";

            // Determine tenant filtering for the join table query and right entity query.
            var tenantId = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            var leftMapping = _ctx.GetMapping(jtm.LeftType);
            var leftTenantCol = leftMapping.TenantColumn;
            // M2M requires single-column PK on both sides; guard early.
            if (leftMapping.KeyColumns.Length == 0)
                throw new InvalidOperationException(
                    $"Many-to-many Include on '{leftMapping.Type.Name}' failed: entity has no primary key columns.");
            var leftPkCol = leftMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var hasTenantFilter = tenantId != null && leftTenantCol != null;

            if (hasTenantFilter)
            {
                var tp = $"{_ctx.Provider.ParamPrefix}jttenant";
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
            using (var jReader = cmd.ExecuteReaderWithInterception(_ctx, System.Data.CommandBehavior.Default))
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
                var pn = $"{_ctx.Provider.ParamPrefix}jrpk{i}";
                cmd2.AddParam(pn, allRightPkList[i]!);
                rightParamNames.Add(pn);
            }
            // M2M requires single-column PK on the right side; guard early.
            if (rightMapping.KeyColumns.Length == 0)
                throw new InvalidOperationException(
                    $"Many-to-many Include on '{rightMapping.Type.Name}' failed: entity has no primary key columns.");
            var rightPkCol = rightMapping.KeyColumns[0]; // single-PK required for M2M join table queries
            var rightInClause = $"({string.Join(", ", rightParamNames)})";

            // If the right entity table also has a tenant column, filter right entities by tenant
            var rightTenantCol = rightMapping.TenantColumn;
            if (tenantId != null && rightTenantCol != null)
            {
                var rtp = $"{_ctx.Provider.ParamPrefix}jrtenant";
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause} AND {rightTenantCol.EscCol} = {rtp}";
                cmd2.AddParam(rtp, tenantId);
            }
            else
            {
                cmd2.CommandText = $"SELECT * FROM {rightMapping.EscTable} WHERE {rightPkCol.EscCol} IN {rightInClause}";
            }
            cmd2.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd2.CommandText);

            var rightEntitiesByPk = new Dictionary<object, object>();
            var rightMat = _materializerFactory.CreateSyncMaterializer(rightMapping, jtm.RightType);
            using (var rReader = cmd2.ExecuteReaderWithInterception(_ctx, System.Data.CommandBehavior.Default))
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
                    collection = (IList)Activator.CreateInstance(
                        typeof(List<>).MakeGenericType(jtm.RightType))!;
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

            // Composite-PK dependents are not supported; throw early rather than silently corrupting data.
            var pathMappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();
            foreach (var (rel, map) in include.Path.Zip(pathMappings))
                if (map.KeyColumns.Length > 1)
                    throw new NotSupportedException(
                        $"Include on '{map.Type.Name}' with a composite primary key is not yet supported. " +
                        "Use a projected query or manual loading instead.");

            var firstRelation = include.Path[0];

            // Build lookup of parent entities by key to allow batching
            var parentLookup = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var key = firstRelation.PrincipalKey.Getter(p);
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
            var maxParams = _ctx.Provider.MaxParameters;
            var maxPerBatch = maxParams == int.MaxValue
                ? keys.Length
                : Math.Max(1, (maxParams - ParameterReserve) / Math.Max(1, include.Path.Count));

            foreach (var keyBatch in keys.Chunk(maxPerBatch))
            {
                ct.ThrowIfCancellationRequested();

                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.CreateCommand();

                var paramNames = new List<string>();
                for (int i = 0; i < keyBatch.Length; i++)
                {
                    var pn = $"{_ctx.Provider.ParamPrefix}fk{i}";
                    cmd.AddParam(pn, keyBatch[i]!);
                    paramNames.Add(pn);
                }

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames, cmd);
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

            // Composite-PK dependents are not supported; throw early rather than silently corrupting data.
            var pathMappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();
            foreach (var (rel, map) in include.Path.Zip(pathMappings))
                if (map.KeyColumns.Length > 1)
                    throw new NotSupportedException(
                        $"Include on '{map.Type.Name}' with a composite primary key is not yet supported. " +
                        "Use a projected query or manual loading instead.");

            var firstRelation = include.Path[0];

            var parentLookup = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var key = firstRelation.PrincipalKey.Getter(p);
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
            var maxParams = _ctx.Provider.MaxParameters;
            var maxPerBatch = maxParams == int.MaxValue
                ? keys.Length
                : Math.Max(1, (maxParams - ParameterReserve) / Math.Max(1, include.Path.Count));

            foreach (var keyBatch in keys.Chunk(maxPerBatch))
            {
                _ctx.EnsureConnection();
                using var cmd = _ctx.CreateCommand();

                var paramNames = new List<string>();
                for (int i = 0; i < keyBatch.Length; i++)
                {
                    var pn = $"{_ctx.Provider.ParamPrefix}fk{i}";
                    cmd.AddParam(pn, keyBatch[i]!);
                    paramNames.Add(pn);
                }

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames, cmd);
                cmd.CommandTimeout = SafeAdaptiveTimeoutSeconds(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);

                using var reader = cmd.ExecuteReaderWithInterception(_ctx, System.Data.CommandBehavior.Default);

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

                var fk = relation.ForeignKey.Getter(child);
                if (fk != null)
                {
                    if (!childGroups.TryGetValue(fk, out var list))
                        childGroups[fk] = list = new List<object>();
                    list.Add(child);
                }
            }

            foreach (var p in parents.Cast<object>())
            {
                var pk = relation.PrincipalKey.Getter(p);
                var listType = typeof(List<>).MakeGenericType(relation.DependentType);
                var childList = (IList)Activator.CreateInstance(listType)!;

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
            var listType = typeof(List<>).MakeGenericType(relation.DependentType);
            var childList = (IList)Activator.CreateInstance(listType)!;
            relation.NavProp.SetValue(parent, childList);
        }

        private string BuildSql(IReadOnlyList<TableMapping.Relation> path, TableMapping[] mappings, List<string> paramNames, DbCommand cmd)
        {
            var tenantId = _ctx.Options.TenantProvider?.GetCurrentTenantId();
            var current = $"({PooledStringBuilder.Join(paramNames, ",")})";
            var sb = PooledStringBuilder.Rent();
            try
            {
                for (int i = 0; i < path.Count; i++)
                {
                    var relation = path[i];
                    var map = mappings[i];
                    var tenantCol = map.TenantColumn;

                    sb.Append("SELECT * FROM ").Append(map.EscTable)
                      .Append(" WHERE ").Append(relation.ForeignKey.EscCol)
                      .Append(" IN ").Append(current);

                    if (tenantId != null && tenantCol != null)
                    {
                        var tp = $"{_ctx.Provider.ParamPrefix}tkn{i}";
                        sb.Append(" AND ").Append(tenantCol.EscCol).Append(" = ").Append(tp);
                        cmd.AddParam(tp, tenantId);
                    }

                    // Separate multiple result-set statements with semicolons;
                    // skip the trailing one so the final SQL is clean.
                    if (i < path.Count - 1)
                        sb.Append(';');

                    // Include all key columns (composite PK support for multi-level traversal).
                    var pkCols = string.Join(", ", map.KeyColumns.Select(k => k.EscCol));
                    var tenantPart = (tenantId != null && tenantCol != null)
                        ? $" AND {tenantCol.EscCol} = {_ctx.Provider.ParamPrefix}tkn{i}"
                        : string.Empty;
                    current = $"(SELECT {pkCols} FROM {map.EscTable} WHERE {relation.ForeignKey.EscCol} IN {current}{tenantPart})";
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

                var fk = relation.ForeignKey.Getter(child);
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
                var pk = relation.PrincipalKey.Getter(p);
                var listType = typeof(List<>).MakeGenericType(relation.DependentType);
                var childList = (IList)Activator.CreateInstance(listType)!;

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
            var keyStr = Convert.ToString(key);
            if (keyStr == null) return default;
            foreach (var candidate in dict.Keys)
            {
                var candidateStr = Convert.ToString(candidate);
                if (candidateStr != null && candidateStr == keyStr)
                    return dict[candidate];
            }
            return default;
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

