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
    /// Handles eager loading of navigation properties using multi-result-set queries.
    /// </summary>
    internal sealed class IncludeProcessor
    {
        private readonly DbContext _ctx;
        private readonly MaterializerFactory _materializerFactory = new();

        public IncludeProcessor(DbContext ctx) => _ctx = ctx;

        /// <summary>
        /// Eagerly loads all relations defined in the <paramref name="include"/> for the given <paramref name="parents"/>
        /// using a single round trip per batch of keys.
        /// </summary>
        public async Task EagerLoadAsync(IncludePlan include, IList parents, CancellationToken ct, bool noTracking)
        {
            if (parents.Count == 0 || include.Path.Count == 0)
                return;

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

            // Pre-compute mappings and materializers for the relations
            var mappings = include.Path.Select(r => _ctx.GetMapping(r.DependentType)).ToArray();
            var materializers = mappings
                .Select(m => _materializerFactory.CreateMaterializer(m, m.Type))
                .ToArray();

            // Determine optimal batch size based on provider parameter limits and relationship depth
            var keys = parentLookup.Keys.ToArray();
            var maxParams = _ctx.Provider.MaxParameters;
            var maxPerBatch = maxParams == int.MaxValue
                ? keys.Length
                : Math.Max(1, (maxParams - 10) / Math.Max(1, include.Path.Count));

            foreach (var keyBatch in keys.Chunk(maxPerBatch))
            {
                ct.ThrowIfCancellationRequested();

                await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
                await using var cmd = _ctx.Connection.CreateCommand();

                var paramNames = new List<string>();
                for (int i = 0; i < keyBatch.Length; i++)
                {
                    var pn = $"{_ctx.Provider.ParamPrefix}fk{i}";
                    cmd.AddParam(pn, keyBatch[i]!);
                    paramNames.Add(pn);
                }

                cmd.CommandText = BuildSql(include.Path, mappings, paramNames);
                cmd.CommandTimeout = (int)_ctx.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText).TotalSeconds;

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

        private static void AssignEmptyList(object parent, TableMapping.Relation relation)
        {
            var listType = typeof(List<>).MakeGenericType(relation.DependentType);
            var childList = (IList)Activator.CreateInstance(listType)!;
            relation.NavProp.SetValue(parent, childList);
        }

        private static string BuildSql(IReadOnlyList<TableMapping.Relation> path, TableMapping[] mappings, List<string> paramNames)
        {
            var current = $"({PooledStringBuilder.Join(paramNames, ",")})";
            var sb = PooledStringBuilder.Rent();
            try
            {
                for (int i = 0; i < path.Count; i++)
                {
                    var relation = path[i];
                    var map = mappings[i];
                    sb.Append("SELECT * FROM ").Append(map.EscTable)
                      .Append(" WHERE ").Append(relation.ForeignKey.EscCol)
                      .Append(" IN ").Append(current).Append(';');

                    var pkCol = map.KeyColumns[0].EscCol;
                    current = $"(SELECT {pkCol} FROM {map.EscTable} WHERE {relation.ForeignKey.EscCol} IN {current})";
                }

                return sb.ToString().TrimEnd(';');
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
    }
}

