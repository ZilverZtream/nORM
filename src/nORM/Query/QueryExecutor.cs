using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.SourceGeneration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace nORM.Query
{
    /// <summary>
    /// Executes <see cref="DbCommand"/> instances and materializes results.
    /// </summary>
    internal sealed class QueryExecutor
    {
        private readonly DbContext _ctx;
        private readonly IncludeProcessor _includeProcessor;
        private readonly NormExceptionHandler _exceptionHandler;
        private readonly ILogger<QueryExecutor> _logger;

        private static readonly Regex LimitRegex = new("\\bLIMIT\\s+(?<value>[\\w@]+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex TopRegex = new("\\bTOP\\s*(?:\\(\\s*)?(?<value>[\\w@]+)(?:\\s*\\))?", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex FetchRegex = new("\\bFETCH\\s+(?:FIRST|NEXT)\\s+(?<value>[\\w@]+)\\s+ROWS\\s+ONLY", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public QueryExecutor(DbContext ctx, IncludeProcessor includeProcessor, ILogger<QueryExecutor>? logger = null)
        {
            _ctx = ctx;
            _includeProcessor = includeProcessor;
            _logger = logger ?? NullLogger<QueryExecutor>.Instance;
            _exceptionHandler = new NormExceptionHandler(_logger);
        }

        /// <summary>
        /// Executes the supplied command and materializes the result set into a list of entities.
        /// </summary>
        /// <param name="plan">Query plan describing how to materialize results.</param>
        /// <param name="cmd">Prepared database command.</param>
        /// <param name="ct">Token used to cancel the operation.</param>
        /// <returns>A list containing the materialized entities.</returns>
        public async Task<IList> MaterializeAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            await using var command = cmd;
            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
            {
                if (plan.GroupJoinInfo != null)
                    return await MaterializeGroupJoinAsync(plan, command, ct).ConfigureAwait(false);

                var listType = typeof(List<>).MakeGenericType(plan.ElementType);
                var capacity = plan.SingleResult ? 1 : EstimateCapacity(plan.Sql, plan.Parameters);
                var list = (IList)Activator.CreateInstance(listType, capacity)!;

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null;

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var entity = await plan.Materializer(reader, ct).ConfigureAwait(false);
                    entity = ProcessEntity(entity, trackable, entityMap);
                    list.Add(entity);
                }

                if (plan.SplitQuery)
                {
                    foreach (var include in plan.Includes)
                    {
                        await _includeProcessor.EagerLoadAsync(include, list, ct, plan.NoTracking).ConfigureAwait(false);
                    }
                }

                return list;
            }, "MaterializeAsync", new Dictionary<string, object> { ["Sql"] = command.CommandText }).ConfigureAwait(false);
        }

        public IList Materialize(QueryPlan plan, DbCommand cmd)
        {
            using var command = cmd;
            return _exceptionHandler.ExecuteWithExceptionHandling(() =>
            {
                if (plan.GroupJoinInfo != null)
                    return Task.FromResult(MaterializeGroupJoin(plan, command));

                var listType = typeof(List<>).MakeGenericType(plan.ElementType);
                var capacity = plan.SingleResult ? 1 : EstimateCapacity(plan.Sql, plan.Parameters);
                var list = (IList)Activator.CreateInstance(listType, capacity)!;

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null;

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                using var reader = command.ExecuteReaderWithInterception(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

                while (reader.Read())
                {
                    var entity = plan.Materializer(reader, default).GetAwaiter().GetResult();
                    entity = ProcessEntity(entity, trackable, entityMap);
                    list.Add(entity);
                }

                if (plan.SplitQuery)
                {
                    foreach (var include in plan.Includes)
                    {
                        _includeProcessor.EagerLoadAsync(include, list, default, plan.NoTracking).GetAwaiter().GetResult();
                    }
                }

                return Task.FromResult(list);
            }, "Materialize", new Dictionary<string, object> { ["Sql"] = command.CommandText }).GetAwaiter().GetResult();
        }

        private object ProcessEntity(object entity, bool trackable, TableMapping? entityMap)
        {
            if (!trackable)
                return entity;

            // ADD FAST PATH FOR READ-ONLY SCENARIOS
            if (IsReadOnlyQuery())
            {
                return entity; // Skip all tracking setup
            }

            var actualMap = entityMap != null && entity.GetType() == entityMap.Type
                ? entityMap
                : _ctx.GetMapping(entity.GetType());
            var entry = _ctx.ChangeTracker.Track(entity, EntityState.Unchanged, actualMap);
            entity = entry.Entity!;
            NavigationPropertyExtensions.EnableLazyLoading(entity, _ctx);
            return entity;
        }

        private bool IsReadOnlyQuery()
        {
            // Determine if this is a read-only context
            return _ctx.Options.DefaultTrackingBehavior == QueryTrackingBehavior.NoTracking;
        }

        private static int EstimateCapacity(string sql, IReadOnlyDictionary<string, object> parameters)
        {
            foreach (var regex in new[] { LimitRegex, TopRegex, FetchRegex })
            {
                var match = regex.Match(sql);
                if (!match.Success) continue;
                var val = match.Groups["value"].Value;
                if (int.TryParse(val, out var number)) return number;
                if (parameters.TryGetValue(val, out var param))
                {
                    try
                    {
                        return Convert.ToInt32(param);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }

            return 0;
        }

        private async Task<IList> MaterializeGroupJoinAsync(QueryPlan plan, DbCommand cmd, CancellationToken ct)
        {
            var info = plan.GroupJoinInfo!;

            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
            {
                try
                {
                    var listType = typeof(List<>).MakeGenericType(info.ResultType);
                    var resultList = (IList)Activator.CreateInstance(listType)!;

                    var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
                    var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

                    var outerMap = _ctx.GetMapping(info.OuterType);
                    var innerMap = _ctx.GetMapping(info.InnerType);

                    var outerColumnCount = outerMap.Columns.Length;
                    var innerKeyIndex = outerColumnCount + Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);

                    await using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                        .ConfigureAwait(false);

                    object? currentOuter = null;
                    object? currentKey = null;
                    List<object> currentChildren = new();

                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var outer = await plan.Materializer(reader, ct).ConfigureAwait(false);
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

                    if (currentOuter != null)
                    {
                        var list = CreateList(info.InnerType, currentChildren);
                        var result = info.ResultSelector(currentOuter, list.Cast<object>());
                        resultList.Add(result);
                    }

                    return resultList;
                }
                catch (Exception)
                {
                    try
                    {
                        await cmd.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception disposeEx)
                    {
                        _logger.LogError(disposeEx, "Error disposing DbCommand.");
                    }

                    throw;
                }
            }, "MaterializeGroupJoinAsync", new Dictionary<string, object> { ["Sql"] = cmd.CommandText }).ConfigureAwait(false);

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

                if (CompiledMaterializerStore.TryGet(map.Type, out var compiled))
                {
                    var wrapped = offset == 0 ? reader : new OffsetDbDataReader(reader, offset);
                    return compiled(wrapped, ct).GetAwaiter().GetResult();
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

        private IList MaterializeGroupJoin(QueryPlan plan, DbCommand cmd)
        {
            var info = plan.GroupJoinInfo!;

            return _exceptionHandler.ExecuteWithExceptionHandling(() =>
            {
                try
                {
                    var listType = typeof(List<>).MakeGenericType(info.ResultType);
                    var resultList = (IList)Activator.CreateInstance(listType)!;

                    var trackOuter = !plan.NoTracking && info.OuterType.IsClass && !info.OuterType.Name.StartsWith("<>") && info.OuterType.GetConstructor(Type.EmptyTypes) != null;
                    var trackInner = !plan.NoTracking && info.InnerType.IsClass && !info.InnerType.Name.StartsWith("<>") && info.InnerType.GetConstructor(Type.EmptyTypes) != null;

                    var outerMap = _ctx.GetMapping(info.OuterType);
                    var innerMap = _ctx.GetMapping(info.InnerType);

                    var outerColumnCount = outerMap.Columns.Length;
                    var innerKeyIndex = outerColumnCount + Array.IndexOf(innerMap.Columns, info.InnerKeyColumn);

                    using var reader = cmd.ExecuteReaderWithInterception(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

                    object? currentOuter = null;
                    object? currentKey = null;
                    List<object> currentChildren = new();

                    while (reader.Read())
                    {
                        var outer = plan.Materializer(reader, default).GetAwaiter().GetResult();
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
                            var inner = MaterializeEntity(reader, innerMap, outerColumnCount, default);
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

                    if (currentOuter != null)
                    {
                        var list = CreateList(info.InnerType, currentChildren);
                        var result = info.ResultSelector(currentOuter, list.Cast<object>());
                        resultList.Add(result);
                    }

                    return Task.FromResult(resultList);
                }
                catch (Exception)
                {
                    try
                    {
                        cmd.Dispose();
                    }
                    catch (Exception disposeEx)
                    {
                        _logger.LogError(disposeEx, "Error disposing DbCommand.");
                    }

                    throw;
                }
            }, "MaterializeGroupJoin", new Dictionary<string, object> { ["Sql"] = cmd.CommandText }).GetAwaiter().GetResult();

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

                if (CompiledMaterializerStore.TryGet(map.Type, out var compiled))
                {
                    var wrapped = offset == 0 ? reader : new OffsetDbDataReader(reader, offset);
                    return compiled(wrapped, ct).GetAwaiter().GetResult();
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

        private sealed class OffsetDbDataReader : DbDataReader
        {
            private readonly DbDataReader _inner;
            private readonly int _offset;

            public OffsetDbDataReader(DbDataReader inner, int offset)
            {
                _inner = inner;
                _offset = offset;
            }

            public override int FieldCount => _inner.FieldCount - _offset;
            public override bool HasRows => _inner.HasRows;
            public override bool IsClosed => _inner.IsClosed;
            public override int RecordsAffected => _inner.RecordsAffected;
            public override int Depth => _inner.Depth;
            public override int VisibleFieldCount => _inner.VisibleFieldCount - _offset;

            public override object this[int ordinal] => _inner[ordinal + _offset];
            public override object this[string name] => _inner[name];

            public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal + _offset);
            public override byte GetByte(int ordinal) => _inner.GetByte(ordinal + _offset);
            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
                => _inner.GetBytes(ordinal + _offset, dataOffset, buffer, bufferOffset, length);
            public override char GetChar(int ordinal) => _inner.GetChar(ordinal + _offset);
            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
                => _inner.GetChars(ordinal + _offset, dataOffset, buffer, bufferOffset, length);
            public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal + _offset);
            public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal + _offset);
            public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal + _offset);
            public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal + _offset);
            public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal + _offset);
            public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal + _offset);
            public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal + _offset);
            public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal + _offset);
            public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal + _offset);
            public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal + _offset);
            public override string GetName(int ordinal) => _inner.GetName(ordinal + _offset);
            public override int GetOrdinal(string name) => _inner.GetOrdinal(name) - _offset;
            public override string GetString(int ordinal) => _inner.GetString(ordinal + _offset);
            public override object GetValue(int ordinal) => _inner.GetValue(ordinal + _offset);
            public override int GetValues(object[] values)
            {
                var temp = new object[values.Length + _offset];
                var count = _inner.GetValues(temp);
                var len = Math.Min(count - _offset, values.Length);
                if (len > 0)
                    Array.Copy(temp, _offset, values, 0, len);
                if (len < values.Length)
                    Array.Fill(values, DBNull.Value, len, values.Length - len);
                return Math.Max(0, len);
            }
            public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal + _offset);
            /// <summary>
            /// Asynchronously determines whether the specified column is <c>DBNull</c>.
            /// </summary>
            /// <param name="ordinal">Column ordinal adjusted by the offset.</param>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A task returning <c>true</c> if the column contains <c>DBNull</c>.</returns>
            public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
                => _inner.IsDBNullAsync(ordinal + _offset, cancellationToken);
            public override T GetFieldValue<T>(int ordinal) => _inner.GetFieldValue<T>(ordinal + _offset);
            public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
                => _inner.GetFieldValueAsync<T>(ordinal + _offset, cancellationToken);

            public override IEnumerator GetEnumerator() => _inner.GetEnumerator();
            public override bool NextResult() => _inner.NextResult();
            public override bool Read() => _inner.Read();
            /// <summary>
            /// Advances to the next result set asynchronously.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A task indicating whether another result set is available.</returns>
            public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => _inner.NextResultAsync(cancellationToken);

            /// <summary>
            /// Asynchronously reads the next row from the current result set.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the read operation.</param>
            /// <returns>A task returning <c>true</c> if a row was read.</returns>
            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => _inner.ReadAsync(cancellationToken);
            public override System.Data.DataTable GetSchemaTable() => _inner.GetSchemaTable()!;
        }
    }
}
