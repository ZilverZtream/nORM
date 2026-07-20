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
    internal sealed partial class IncludeProcessor
    {
        private readonly DbContext _ctx;
        private readonly MaterializerFactory _materializerFactory = new();

        /// <summary>
        /// Number of parameter slots reserved for internal use (tenant params, etc.)
        /// when computing the maximum keys per eager-load batch.
        /// Mirrors <c>DatabaseProvider.ParameterReserve</c>.
        /// </summary>
        private const int ParameterReserve = 10;

        public IncludeProcessor(DbContext ctx) => _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));

        /// <summary>
        /// Eagerly loads all relations defined in the <paramref name="include"/> for the given <paramref name="parents"/>
        /// using a single round trip per batch of keys.
        /// </summary>
        public async Task EagerLoadAsync(IncludePlan include, IList parents, CancellationToken ct, bool noTracking, DateTime? asOf = null, Dictionary<string, object?>? filterParams = null)
        {
            if (parents.Count == 0 || include.Path.Count == 0)
                return;

            var pathMappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();

            var firstRelation = include.Path[0];

            // Build lookup of parent entities by key to allow batching
            var parentLookup = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var key = GetPrincipalKeyValue(firstRelation, p);
                if (key == null)
                {
                    AssignEmptyNavigation(p, firstRelation);
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

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames, paramGroups, cmd, asOf, include.Filters, filterParams, include.Orderings);
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
        public void EagerLoad(IncludePlan include, IList parents, bool noTracking, DateTime? asOf = null, Dictionary<string, object?>? filterParams = null)
        {
            if (parents.Count == 0 || include.Path.Count == 0)
                return;

            var pathMappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();

            var firstRelation = include.Path[0];

            var parentLookup = new Dictionary<object, List<object>>();
            foreach (var p in parents.Cast<object>())
            {
                var key = GetPrincipalKeyValue(firstRelation, p);
                if (key == null)
                {
                    AssignEmptyNavigation(p, firstRelation);
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

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames, paramGroups, cmd, asOf, include.Filters, filterParams, include.Orderings);
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
                if (IsCollectionNavigation(relation))
                {
                    var childList = QueryExecutor.CreateList(relation.DependentType, 0);
                    if (pk != null && childGroups.TryGetValue(pk, out var c))
                    {
                        foreach (var item in c)
                            childList.Add(item);
                    }

                    relation.NavProp.SetValue(p, childList);
                    // Record the loaded children so a later removal from the collection is
                    // detected as a real disassociation and the child's FK is severed on save.
                    _ctx.ChangeTracker.GetEntryOrDefault(p)?.CaptureCollectionNavSnapshot(relation.NavProp.Name, childList);
                }
                else
                {
                    var child = pk != null && childGroups.TryGetValue(pk, out var c)
                        ? c.FirstOrDefault()
                        : null;
                    relation.NavProp.SetValue(p, child);
                    // Record the loaded principal so a later clear (nav = null) is
                    // detected as a real disassociation and the FK is nulled on save.
                    if (child != null)
                        _ctx.ChangeTracker.GetEntryOrDefault(p)?.CaptureReferenceNavSnapshots();
                }
            }

            return resultChildren;
        }

        private static void AssignEmptyNavigation(object parent, TableMapping.Relation relation)
        {
            if (IsCollectionNavigation(relation))
            {
                var childList = QueryExecutor.CreateList(relation.DependentType, 0);
                relation.NavProp.SetValue(parent, childList);
            }
            else
            {
                relation.NavProp.SetValue(parent, null);
            }
        }

        private static bool IsCollectionNavigation(TableMapping.Relation relation)
        {
            var propertyType = relation.NavProp.PropertyType;
            return propertyType != typeof(string)
                   && propertyType.IsGenericType
                   && typeof(IEnumerable).IsAssignableFrom(propertyType);
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

        private static object?[]? ReadKeyValues(DbDataReader reader, int offset, int count)
        {
            var values = new object?[count];
            for (var i = 0; i < count; i++)
            {
                if (reader.IsDBNull(offset + i))
                    return null;
                values[i] = reader.GetValue(offset + i);
            }

            return values;
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
                if (IsCollectionNavigation(relation))
                {
                    var childList = QueryExecutor.CreateList(relation.DependentType, 0);
                    if (pk != null && childGroups.TryGetValue(pk, out var c))
                    {
                        foreach (var item in c)
                            childList.Add(item);
                    }

                    relation.NavProp.SetValue(p, childList);
                    // Record the loaded children so a later removal from the collection is
                    // detected as a real disassociation and the child's FK is severed on save.
                    _ctx.ChangeTracker.GetEntryOrDefault(p)?.CaptureCollectionNavSnapshot(relation.NavProp.Name, childList);
                }
                else
                {
                    var child = pk != null && childGroups.TryGetValue(pk, out var c)
                        ? c.FirstOrDefault()
                        : null;
                    relation.NavProp.SetValue(p, child);
                    // Record the loaded principal so a later clear (nav = null) is
                    // detected as a real disassociation and the FK is nulled on save.
                    if (child != null)
                        _ctx.ChangeTracker.GetEntryOrDefault(p)?.CaptureReferenceNavSnapshots();
                }
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

