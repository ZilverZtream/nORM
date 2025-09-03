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

            if (plan.SplitQuery)
            {
                foreach (var include in plan.Includes)
                {
                    await _includeProcessor.EagerLoadAsync(include, list, ct, plan.NoTracking);
                }
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

            var outerColumnCount = outerMap.Columns.Length;
            var innerKeyIndex = outerColumnCount + Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);

            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess, ct);

            object? currentOuter = null;
            object? currentKey = null;
            List<object> currentChildren = new();

            while (await reader.ReadAsync(ct))
            {
                var outer = await plan.Materializer(reader, ct);
                var key = info.OuterKeySelector(outer) ?? DBNull.Value;

                if (currentOuter == null || !Equals(currentKey, key))
                {
                    if (currentOuter != null)
                    {
                        var list = CreateList(info.InnerType, currentChildren);
                        var result = info.ResultSelector(currentOuter, list.Cast<object>());
                        resultList.Add(result);
                        currentChildren = new List<object>();
                    }

                    if (trackOuter)
                    {
                        NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                        var actualMap = _ctx.GetMapping(outer.GetType());
                        _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                    }

                    currentOuter = outer;
                    currentKey = key;
                }

                if (!reader.IsDBNull(innerKeyIndex))
                {
                    var inner = MaterializeEntity(reader, innerMap, outerColumnCount);
                    if (trackInner)
                    {
                        NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                        var actualMap = _ctx.GetMapping(inner.GetType());
                        _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                    }
                    currentChildren.Add(inner);
                }
            }

            if (currentOuter != null)
            {
                var list = CreateList(info.InnerType, currentChildren);
                var result = info.ResultSelector(currentOuter, list.Cast<object>());
                resultList.Add(result);
            }

            return resultList;

            static object MaterializeEntity(DbDataReader reader, TableMapping map, int offset)
            {
                if (map.DiscriminatorColumn != null && map.TphMappings.Count > 0)
                {
                    var discIndex = offset + Array.IndexOf(map.Columns, map.DiscriminatorColumn);
                    if (!reader.IsDBNull(discIndex))
                    {
                        var disc = reader.GetValue(discIndex);
                        if (disc != null && map.TphMappings.TryGetValue(disc, out var derived))
                            return MaterializeEntity(reader, derived, offset);
                    }
                }

                var entity = Activator.CreateInstance(map.Type)!;
                for (int i = 0; i < map.Columns.Length; i++)
                {
                    var idx = offset + i;
                    if (reader.IsDBNull(idx)) continue;
                    var col = map.Columns[i];
                    var read = Methods.GetReaderMethod(col.Prop.PropertyType);
                    var value = read.Invoke(reader, new object[] { idx });
                    if (read == Methods.GetValue)
                        value = Convert.ChangeType(value!, col.Prop.PropertyType);
                    col.Setter(entity, value);
                }
                return entity;
            }

            static IList CreateList(Type innerType, List<object> items)
            {
                var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(innerType))!;
                foreach (var item in items) list.Add(item);
                return list;
            }
        }
    }
}
