using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.SourceGeneration;

#pragma warning disable IDE0130

namespace nORM.Query
{
    internal sealed partial class QueryExecutor
    {
        /// <summary>Prepares shared column offsets and tracking flags for GroupJoin materializers.</summary>
        /// <param name="plan">The query plan describing mappings and selectors.</param>
        private GroupJoinMaterializationState PrepareGroupJoinMaterialization(QueryPlan plan)
        {
            var info = plan.GroupJoinInfo!;
            var trackOuter = info.OuterIsEntity
                             && !plan.NoTracking
                             && info.OuterType.IsClass
                             && !info.OuterType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal);
            var trackInner = !plan.NoTracking
                             && info.InnerType.IsClass
                             && !info.InnerType.Name.StartsWith(AnonymousTypePrefix, StringComparison.Ordinal);

            var outerColumnCount = info.OuterColumnCount;
            if (outerColumnCount < 0)
            {
                if (!info.OuterIsEntity)
                    throw new InvalidOperationException("GroupJoin scalar outer source did not provide an outer column count.");

                outerColumnCount = _ctx.GetMapping(info.OuterType).Columns.Length;
            }

            var innerMap = _ctx.GetMapping(info.InnerType);
            var innerKeyOffset = Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);
            if (innerKeyOffset < 0)
            {
                innerKeyOffset = Array.FindIndex(innerMap.Columns, c =>
                    string.Equals(c.PropName, info.InnerKeyColumn.PropName, StringComparison.Ordinal)
                    || string.Equals(c.Name, info.InnerKeyColumn.Name, StringComparison.Ordinal));
            }

            if (innerKeyOffset < 0)
            {
                throw new InvalidOperationException(
                    $"GroupJoin inner key column '{info.InnerKeyColumn?.Name ?? "(null)"}' not found in mapping for '{info.InnerType.Name}'. " +
                    $"Available columns: {string.Join(", ", innerMap.Columns.Select(c => c.Name))}.");
            }

            return new GroupJoinMaterializationState(
                innerMap,
                outerColumnCount,
                outerColumnCount + innerKeyOffset,
                trackOuter,
                trackInner);
        }

        private readonly record struct GroupJoinMaterializationState(
            TableMapping InnerMap,
            int OuterColumnCount,
            int InnerKeyIndex,
            bool TrackOuter,
            bool TrackInner);

        /// <summary>
        /// Materializes the results of a LINQ <c>GroupJoin</c> operation by streaming records from
        /// the provided command and constructing the grouped results in memory.
        /// </summary>
        /// <param name="plan">The query plan describing mappings and selectors.</param>
        /// <param name="cmd">The database command to execute.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>An <see cref="IList"/> containing the grouped join results.</returns>
        private async Task<IList> MaterializeGroupJoinAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var info = plan.GroupJoinInfo!;

            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
            {
                // Use cached list factory instead of Activator.CreateInstance.
                var resultList = CreateList(info.ResultType, DefaultListCapacity);

                var state = PrepareGroupJoinMaterialization(plan);
                var trackOuter = state.TrackOuter;
                var trackInner = state.TrackInner;
                var innerMap = state.InnerMap;
                var outerColumnCount = state.OuterColumnCount;
                var innerKeyIndex = state.InnerKeyIndex;

                // GroupJoin reads innerKeyIndex BEFORE materializing inner columns - sequential
                // access would cause a backward seek (Npgsql ThrowInvalidSequentialSeek).
                // GroupJoin result sets are bounded by MaxGroupJoinSize so buffered reads are safe.
                await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.Default | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                object? currentOuter = null;
                object? currentKey = null;
                List<object> currentChildren = [];

                // Use sync materializer to avoid per-row Task allocation, consistent with MaterializeAsync.
                var syncMaterializer = plan.SyncMaterializer;
                // Segment per OUTER ROW (PK identity) when available; the join key would fuse
                // distinct outers that share a key value.
                var segmentSelector = info.OuterIdentitySelector ?? info.OuterKeySelector;
                var distinctEmitState = new DistinctEmitState();

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var outer = syncMaterializer(reader);
                    var key = segmentSelector(outer) ?? DBNull.Value;

                    if (currentOuter == null || !Equals(currentKey, key))
                    {
                        if (currentOuter != null)
                        {
                            if (ShouldEmitSegment(info, currentOuter, distinctEmitState))
                            {
                                var list = CreateListFromItems(info.InnerType, currentChildren);
                                // ResultSelector expects IEnumerable<object>; .Cast<object>() provides the typed wrapper.
                                var result = info.ResultSelector(currentOuter, list.Cast<object>());
                                resultList.Add(result);
                            }
                            currentChildren = [];
                        }

                        if (trackOuter)
                        {
                            var actualMap = _ctx.GetMapping(outer.GetType());
                            var entry = _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                            outer = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                        }

                        currentOuter = outer;
                        currentKey = key;
                    }

                    if (!reader.IsDBNull(innerKeyIndex))
                    {
                        // Use configurable MaxGroupJoinSize instead of a hard-coded limit.
                        // Prevents unbounded memory growth while allowing users to opt-in to larger datasets.
                        var maxSize = _ctx.Options.MaxGroupJoinSize;
                        if (currentChildren.Count >= maxSize)
                        {
                            throw new NormQueryException(
                                $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                                "This may indicate a malformed query or incorrect join keys. " +
                                "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                                "Review your GroupJoin operation and ensure join keys are correct.");
                        }

                        var inner = MaterializeEntity(reader, innerMap, outerColumnCount, ct);
                        if (trackInner)
                        {
                            var actualMap = _ctx.GetMapping(inner.GetType());
                            var entry = _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                            inner = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                        }
                        currentChildren.Add(inner);
                    }
                }

                if (currentOuter != null
                    && ShouldEmitSegment(info, currentOuter, distinctEmitState))
                {
                    var list = CreateListFromItems(info.InnerType, currentChildren);
                    // ResultSelector expects IEnumerable<object>; .Cast<object>() provides the typed wrapper.
                    var result = info.ResultSelector(currentOuter, list.Cast<object>());
                    resultList.Add(result);
                }

                return resultList;
            }, "MaterializeGroupJoinAsync", new Dictionary<string, object> { ["Sql"] = RedactSqlForLogging(cmd.CommandText) }).ConfigureAwait(false);

            static object MaterializeEntity(DbDataReader reader, TableMapping map, int offset, CancellationToken ct)
            {
                if (map.DiscriminatorColumn != null && map.TphMappings.Count > 0)
                {
                    var discIndex = offset + Array.IndexOf(map.Columns, map.DiscriminatorColumn);
                    if (!reader.IsDBNull(discIndex))
                    {
                        var disc = reader.GetValue(discIndex);
                        if (disc != null && map.TphMappings.TryGetValue(disc, out var derived))
                            return MaterializeEntity(reader, derived, offset, ct);
                    }
                }

                // Q1/X1 fix: use model-aware (type, tableName) lookup to avoid wrong
                // materializer selection when the same CLR type is registered under
                // multiple table mappings.  Type-only lookup used the [Table]-attribute
                // name, silently bypassing the actual mapping discriminator.
                if (CompiledMaterializerStore.TryGet(map.Type, map.TableName, out var compiled) && offset == 0)
                {
                    return compiled(reader, ct).GetAwaiter().GetResult();
                }

                // Use MaterializerFactory instead of reflection.
                // MaterializerFactory creates compiled IL.Emit/Expression-based materializers.
                var factory = _sharedMaterializerFactory;
                var materializer = factory.CreateSyncMaterializer(map, map.Type, startOffset: offset);
                return materializer(reader);
            }

            static IList CreateListFromItems(Type innerType, List<object> items)
            {
                // PERFORMANCE: Reuse cached list factory to avoid Activator reflection per group
                var list = CreateList(innerType, items.Count);
                foreach (var item in items) list.Add(item);
                return list;
            }
        }

        /// <summary>
        /// Select(computed).Distinct().GroupJoin(...) segments per outer ROW but must
        /// yield one result per DISTINCT outer key: rows are ordered by the key first,
        /// so equal-key segments are adjacent and only the first one is emitted. Every
        /// segment carries an identical inner group for its key, so skipping the rest
        /// drops no data.
        /// </summary>
        private sealed class DistinctEmitState
        {
            public object? LastKey;
            public bool EmittedAny;
        }

        private static bool ShouldEmitSegment(GroupJoinInfo info, object segmentOuter, DistinctEmitState state)
        {
            if (!info.DistinctOuterKeys)
                return true;
            var distinctKey = info.OuterKeySelector(segmentOuter) ?? DBNull.Value;
            if (state.EmittedAny && Equals(state.LastKey, distinctKey))
                return false;
            state.LastKey = distinctKey;
            state.EmittedAny = true;
            return true;
        }

        internal async IAsyncEnumerable<T> StreamGroupJoinAsync<T>(
            QueryPlan plan,
            DbCommand cmd,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            var info = plan.GroupJoinInfo!;

            var state = PrepareGroupJoinMaterialization(plan);
            var trackOuter = state.TrackOuter;
            var trackInner = state.TrackInner;
            var innerMap = state.InnerMap;
            var outerColumnCount = state.OuterColumnCount;
            var innerKeyIndex = state.InnerKeyIndex;

            // Non-sequential read: innerKeyIndex may be read before inner columns are consumed.
            await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(
                    _ctx, CommandBehavior.Default | CommandBehavior.SingleResult, ct)
                .ConfigureAwait(false);

            object? currentOuter = null;
            object? currentKey = null;
            List<object> currentChildren = [];

            var syncMaterializer = plan.SyncMaterializer;
            // Segment per OUTER ROW (PK identity) when available — see MaterializeGroupJoinAsync.
            var segmentSelector = info.OuterIdentitySelector ?? info.OuterKeySelector;
            var distinctEmitState = new DistinctEmitState();

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var outer = syncMaterializer(reader);
                var key = segmentSelector(outer) ?? DBNull.Value;

                if (currentOuter == null || !Equals(currentKey, key))
                {
                    if (currentOuter != null)
                    {
                        if (ShouldEmitSegment(info, currentOuter, distinctEmitState))
                        {
                            var childList = BuildInnerList(info.InnerType, currentChildren);
                            yield return (T)info.ResultSelector(currentOuter, childList.Cast<object>());
                        }
                        currentChildren = [];
                    }

                    if (trackOuter)
                    {
                        var actualMap = _ctx.GetMapping(outer.GetType());
                        var entry = _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                        outer = entry.Entity!;
                        NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                    }

                    currentOuter = outer;
                    currentKey = key;
                }

                if (!reader.IsDBNull(innerKeyIndex))
                {
                    var maxSize = _ctx.Options.MaxGroupJoinSize;
                    if (currentChildren.Count >= maxSize)
                        throw new NormQueryException(
                            $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                            "This may indicate a malformed query or incorrect join keys. " +
                            "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                            "Review your GroupJoin operation and ensure join keys are correct.");

                    var inner = BuildInnerEntity(reader, innerMap, outerColumnCount, ct);
                    if (trackInner)
                    {
                        var actualMap = _ctx.GetMapping(inner.GetType());
                        var entry = _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                        inner = entry.Entity!;
                        NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                    }
                    currentChildren.Add(inner);
                }
            }

            if (currentOuter != null
                && ShouldEmitSegment(info, currentOuter, distinctEmitState))
            {
                var childList = BuildInnerList(info.InnerType, currentChildren);
                yield return (T)info.ResultSelector(currentOuter, childList.Cast<object>());
            }

            static object BuildInnerEntity(DbDataReader reader, TableMapping map, int offset, CancellationToken ct)
            {
                if (map.DiscriminatorColumn != null && map.TphMappings.Count > 0)
                {
                    var discIndex = offset + Array.IndexOf(map.Columns, map.DiscriminatorColumn);
                    if (!reader.IsDBNull(discIndex))
                    {
                        var disc = reader.GetValue(discIndex);
                        if (disc != null && map.TphMappings.TryGetValue(disc, out var derived))
                            return BuildInnerEntity(reader, derived, offset, ct);
                    }
                }

                if (CompiledMaterializerStore.TryGet(map.Type, map.TableName, out var compiled) && offset == 0)
                    return compiled(reader, ct).GetAwaiter().GetResult();

                var materializer = _sharedMaterializerFactory.CreateSyncMaterializer(map, map.Type, startOffset: offset);
                return materializer(reader);
            }

            static IList BuildInnerList(Type innerType, List<object> items)
            {
                var list = CreateList(innerType, items.Count);
                foreach (var item in items) list.Add(item);
                return list;
            }
        }

        private IList MaterializeGroupJoin(QueryPlan plan, DbCommand cmd)
        {
            var info = plan.GroupJoinInfo!;

            // SYNC-OVER-ASYNC FIX (Option A): Use truly synchronous code path.
            // Previously this called plan.Materializer(...).GetAwaiter().GetResult() which
            // blocks a thread pool thread.  We now use plan.SyncMaterializer which is a
            // genuine synchronous delegate - no async state machine involved.
            return _exceptionHandler.ExecuteWithExceptionHandlingSync(() =>
            {
                var resultList = CreateList(info.ResultType, DefaultListCapacity);

                var state = PrepareGroupJoinMaterialization(plan);
                var trackOuter = state.TrackOuter;
                var trackInner = state.TrackInner;
                var innerMap = state.InnerMap;
                var outerColumnCount = state.OuterColumnCount;
                var innerKeyIndex = state.InnerKeyIndex;

                // GroupJoin reads innerKeyIndex BEFORE materializing inner columns - sequential
                // access would cause a backward seek (Npgsql ThrowInvalidSequentialSeek).
                // GroupJoin result sets are bounded by MaxGroupJoinSize so buffered reads are safe.
                using var reader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(_ctx, CommandBehavior.Default | CommandBehavior.SingleResult);

                object? currentOuter = null;
                object? currentKey = null;
                List<object> currentChildren = [];

                // Segment per OUTER ROW (PK identity) when available — see MaterializeGroupJoinAsync.
                var segmentSelector = info.OuterIdentitySelector ?? info.OuterKeySelector;
                var distinctEmitState = new DistinctEmitState();

                while (reader.Read())
                {
                    // Use sync materializer - no GetAwaiter().GetResult().
                    var outer = plan.SyncMaterializer(reader);
                    var key = segmentSelector(outer) ?? DBNull.Value;

                    if (currentOuter == null || !Equals(currentKey, key))
                    {
                        if (currentOuter != null)
                        {
                            if (ShouldEmitSegment(info, currentOuter, distinctEmitState))
                            {
                                var list = CreateListFromItems(info.InnerType, currentChildren);
                                // ResultSelector expects IEnumerable<object>; .Cast<object>() provides the typed wrapper.
                                var result = info.ResultSelector(currentOuter, list.Cast<object>());
                                resultList.Add(result);
                            }
                            currentChildren = [];
                        }

                        if (trackOuter)
                        {
                            var actualMap = _ctx.GetMapping(outer.GetType());
                            var entry = _ctx.ChangeTracker.Track(outer, EntityState.Unchanged, actualMap);
                            outer = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(outer, _ctx);
                        }

                        currentOuter = outer;
                        currentKey = key;
                    }

                    if (!reader.IsDBNull(innerKeyIndex))
                    {
                        // Use configurable MaxGroupJoinSize instead of a hard-coded limit.
                        var maxSize = _ctx.Options.MaxGroupJoinSize;
                        if (currentChildren.Count >= maxSize)
                        {
                            throw new NormQueryException(
                                $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                                "This may indicate a malformed query or incorrect join keys. " +
                                "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                                "Review your GroupJoin operation and ensure join keys are correct.");
                        }

                        var inner = MaterializeEntity(reader, innerMap, outerColumnCount);
                        if (trackInner)
                        {
                            var actualMap = _ctx.GetMapping(inner.GetType());
                            var entry = _ctx.ChangeTracker.Track(inner, EntityState.Unchanged, actualMap);
                            inner = entry.Entity!;
                            NavigationPropertyExtensions.EnableLazyLoading(inner, _ctx);
                        }
                        currentChildren.Add(inner);
                    }
                }

                if (currentOuter != null
                    && ShouldEmitSegment(info, currentOuter, distinctEmitState))
                {
                    var list = CreateListFromItems(info.InnerType, currentChildren);
                    var result = info.ResultSelector(currentOuter, list.Cast<object>());
                    resultList.Add(result);
                }

                reader.Dispose();
                if (plan.PostMaterializeTransform != null)
                    return plan.PostMaterializeTransform(_ctx, resultList);

                return resultList;
            }, "MaterializeGroupJoin", new Dictionary<string, object> { ["Sql"] = RedactSqlForLogging(cmd.CommandText) });

            // SYNC-OVER-ASYNC FIX: MaterializeEntity now uses only synchronous ADO.NET.
            // CompiledMaterializerStore materializers are registered as Func<DbDataReader, object>
            // (truly synchronous); the async wrapper calls Task.FromResult so GetAwaiter().GetResult()
            // would have been safe, but we access the typed sync delegate directly to be explicit.
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

                // Use the synchronous materializer factory directly - no async wrapper.
                var factory = _sharedMaterializerFactory;
                var materializer = factory.CreateSyncMaterializer(map, map.Type, startOffset: offset);
                return materializer(reader);
            }

            static IList CreateListFromItems(Type innerType, List<object> items)
            {
                var list = CreateList(innerType, items.Count);
                foreach (var item in items) list.Add(item);
                return list;
            }
        }
    }
}
