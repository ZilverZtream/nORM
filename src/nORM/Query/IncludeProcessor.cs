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
                current = await EagerLoadLevelAsync(relation, current, ct, noTracking).ConfigureAwait(false);
                if (current.Count == 0) break;
            }
        }

        private async Task<IList> EagerLoadLevelAsync(TableMapping.Relation relation, IList parents, CancellationToken ct, bool noTracking)
        {
            var resultChildren = new List<object>();
            if (parents.Count == 0) return resultChildren;

            var childMap = _ctx.GetMapping(relation.DependentType);
            var keys = parents.Cast<object>().Select(relation.PrincipalKey.Getter).Where(k => k != null).Distinct().ToList();
            if (keys.Count == 0) return resultChildren;

            var paramNames = new List<string>();
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            for (int i = 0; i < keys.Count; i++)
            {
                var paramName = $"{_ctx.Provider.ParamPrefix}fk{i}";
                paramNames.Add(paramName);
                cmd.AddParam(paramName, keys[i]!);
            }
            cmd.CommandText = $"SELECT * FROM {childMap.EscTable} WHERE {relation.ForeignKey.EscCol} IN ({string.Join(",", paramNames)})";

            var childMaterializer = _materializerFactory.CreateMaterializer(childMap, childMap.Type);
            await using (var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default, ct).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var child = await childMaterializer(reader, ct).ConfigureAwait(false);
                    if (!noTracking)
                    {
                        var entry = _ctx.ChangeTracker.Track(child, EntityState.Unchanged, childMap);
                        child = entry.Entity!;
                        NavigationPropertyExtensions.EnableLazyLoading(child, _ctx);
                    }
                    resultChildren.Add(child);
                }
            }

            var childGroups = resultChildren
                .GroupBy(relation.ForeignKey.Getter)
                .ToDictionary(g => g.Key!, g => g.ToList());

            foreach (var p in parents.Cast<object>())
            {
                var pk = relation.PrincipalKey.Getter(p);
                var listType = typeof(List<>).MakeGenericType(relation.DependentType);
                var childList = (IList)System.Activator.CreateInstance(listType)!;

                if (pk != null && childGroups.TryGetValue(pk, out var c))
                {
                    foreach (var item in c) childList.Add(item);
                }

                relation.NavProp.SetValue(p, childList);
            }

            return resultChildren;
        }
    }
}
