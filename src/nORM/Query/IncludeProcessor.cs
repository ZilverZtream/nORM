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

namespace nORM.Query
{
    /// <summary>
    /// Handles eager loading of navigation properties.
    /// </summary>
    internal sealed class IncludeProcessor
    {
        private readonly DbContext _ctx;
        private readonly MaterializerFactory _materializerFactory = new();

        public IncludeProcessor(DbContext ctx) => _ctx = ctx;

        public async Task EagerLoadAsync(IncludePlan include, IList parents, CancellationToken ct, bool noTracking)
        {
            IList current = parents;
            foreach (var relation in include.Path)
            {
                ct.ThrowIfCancellationRequested();
                current = await EagerLoadLevelAsync(relation, current, ct, noTracking).ConfigureAwait(false);
                if (current.Count == 0) break;
            }
        }

        private async Task<IList> EagerLoadLevelAsync(TableMapping.Relation relation, IList parents, CancellationToken ct, bool noTracking)
        {
            var resultChildren = new List<object>();
            if (parents.Count == 0) return resultChildren;

            var childMap = _ctx.GetMapping(relation.DependentType);
            var childMaterializer = _materializerFactory.CreateMaterializer(childMap, childMap.Type);

            // Limit the number of parameters per query to avoid memory pressure
            var maxPerBatch = Math.Max(1, Math.Min(1000, _ctx.Provider.MaxParameters - 10));
            var childGroups = new Dictionary<object, List<object>>();

            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);

            var keyEnumerable = parents.Cast<object>()
                                       .Select(relation.PrincipalKey.Getter)
                                       .Where(k => k != null)
                                       .Distinct();

            var hasKeys = false;
            foreach (var keyBatch in keyEnumerable.Chunk(maxPerBatch))
            {
                ct.ThrowIfCancellationRequested();
                hasKeys = true;
                await using var cmd = _ctx.Connection.CreateCommand();
                cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                var paramNames = new List<string>();
                for (int i = 0; i < keyBatch.Length; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    var pn = $"{_ctx.Provider.ParamPrefix}fk{i}";
                    paramNames.Add(pn);
                    cmd.AddParam(pn, keyBatch[i]!);
                }

                cmd.CommandText = $"SELECT * FROM {childMap.EscTable} WHERE {relation.ForeignKey.EscCol} IN ({string.Join(",", paramNames)})";

                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default, ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    ct.ThrowIfCancellationRequested();
                    var child = await childMaterializer(reader, ct).ConfigureAwait(false);
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
            }

            if (!hasKeys) return resultChildren;

            foreach (var p in parents.Cast<object>())
            {
                ct.ThrowIfCancellationRequested();
                var pk = relation.PrincipalKey.Getter(p);
                var listType = typeof(List<>).MakeGenericType(relation.DependentType);
                var childList = (IList)System.Activator.CreateInstance(listType)!;

                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    foreach (var item in c)
                    {
                        ct.ThrowIfCancellationRequested();
                        childList.Add(item);
                    }
                }

                relation.NavProp.SetValue(p, childList);
            }

            return resultChildren;
        }
    }
}
