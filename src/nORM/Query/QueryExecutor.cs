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
    /// Executes <see cref="DbCommand"/> instances and materializes results.
    /// </summary>
    internal sealed class QueryExecutor
    {
        private readonly DbContext _ctx;
        private readonly IncludeProcessor _includeProcessor;

        public QueryExecutor(DbContext ctx, IncludeProcessor includeProcessor)
        {
            _ctx = ctx;
            _includeProcessor = includeProcessor;
        }

        public async Task<IList> MaterializeAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            if (plan.GroupJoinInfo != null)
                return await MaterializeGroupJoinAsync(plan, cmd, ct);

            var listType = typeof(List<>).MakeGenericType(plan.ElementType);
            var list = (IList)Activator.CreateInstance(listType)!;

            var trackable = !plan.NoTracking &&
                             plan.ElementType.IsClass &&
                             !plan.ElementType.Name.StartsWith("<>") &&
                             plan.ElementType.GetConstructor(Type.EmptyTypes) != null;
            TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess, ct);
            while (await reader.ReadAsync(ct))
            {
                var entity = await plan.Materializer(reader, ct);

                if (trackable)
                {
                    NavigationPropertyExtensions.EnableLazyLoading(entity, _ctx);
                    var actualMap = _ctx.GetMapping(entity.GetType());
                    _ctx.ChangeTracker.Track(entity, EntityState.Unchanged, actualMap);
                }

                list.Add(entity);
            }

            foreach (var include in plan.Includes)
            {
                await _includeProcessor.EagerLoadAsync(include, list, ct, plan.NoTracking);
            }

            return list;
        }

        private async Task<IList> MaterializeGroupJoinAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var info = plan.GroupJoinInfo!;

            var listType = typeof(List<>).MakeGenericType(info.ResultType);
            var resultList = (IList)Activator.CreateInstance(listType)!;

            var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
            var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

            var outerMap = _ctx.GetMapping(info.OuterType);
            var innerMap = _ctx.GetMapping(info.InnerType);

            var innerKeyIndex = outerMap.Columns.Length + Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);

            var groups = new Dictionary<object, (object outer, List<object> children)>();

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess, ct);
            while (await reader.ReadAsync(ct))
            {
                var tuple = (ValueTuple<object, object>)await plan.Materializer(reader, ct);
                var outer = tuple.Item1;
                var key = info.OuterKeySelector(outer) ?? DBNull.Value;

                if (!groups.TryGetValue(key, out var entry))
                {
                    if (trackOuter)
                    {
                        NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                        var actualMap = _ctx.GetMapping(outer.GetType());
                        _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                    }
                    entry = (outer, new List<object>());
                    groups[key] = entry;
                }

                if (!reader.IsDBNull(innerKeyIndex))
                {
                    var inner = tuple.Item2;
                    if (trackInner)
                    {
                        NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                        var actualMap = _ctx.GetMapping(inner.GetType());
                        _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                    }
                    entry.children.Add(inner);
                }
            }

            foreach (var entry in groups.Values)
            {
                var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(info.InnerType))!;
                foreach (var child in entry.children) list.Add(child);
                var result = info.ResultSelector(entry.outer, list.Cast<object>());
                resultList.Add(result);
            }

            return resultList;
        }
    }
}
