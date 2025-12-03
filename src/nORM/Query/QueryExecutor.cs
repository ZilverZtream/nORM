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

        // PERFORMANCE FIX (TASK 7): Removed regex patterns - no longer needed since Take is passed from QueryPlan
        // Previously these regex patterns were executed on EVERY query to estimate list capacity
        // This was extremely CPU intensive for large SQL strings (kilobytes long)
        // Now we pass the Take value directly from the query plan, avoiding regex entirely

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
                // PERFORMANCE OPTIMIZATION 15: Improved list capacity pre-sizing
                // - SingleResult: capacity = 1
                // - Take specified: use Take value
                // - No Take: use heuristic (16 is typical small result set, avoids resize for most queries)
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
                var list = (IList)Activator.CreateInstance(listType, capacity)!;

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null;

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                // PERFORMANCE FIX (TASK 15): Hoist read-only check out of per-row loop
                // IsReadOnlyQuery() checks context options which don't change during query execution
                // Calling it once instead of millions of times for large result sets
                bool isReadOnly = IsReadOnlyQuery();

                await using var reader = await command.ExecuteReaderWithInterceptionAsync(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, ct)
                    .ConfigureAwait(false);

                // PERFORMANCE FIX (TASK 14): Use sync materializer to avoid per-row Task allocation
                var syncMaterializer = plan.SyncMaterializer;

                // PERFORMANCE FIX (TASK 19): Respect SingleResult flag to avoid materializing unnecessary rows
                if (plan.SingleResult)
                {
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        ct.ThrowIfCancellationRequested();
                        var entity = syncMaterializer(reader);
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
                }
                else
                {
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        ct.ThrowIfCancellationRequested();
                        var entity = syncMaterializer(reader);
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
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
                // PERFORMANCE OPTIMIZATION 15: Improved list capacity pre-sizing
                // - SingleResult: capacity = 1
                // - Take specified: use Take value
                // - No Take: use heuristic (16 is typical small result set, avoids resize for most queries)
                var capacity = plan.SingleResult ? 1 : (plan.Take ?? 16);
                var list = (IList)Activator.CreateInstance(listType, capacity)!;

                var trackable = !plan.NoTracking &&
                                 plan.ElementType.IsClass &&
                                 !plan.ElementType.Name.StartsWith("<>") &&
                                 plan.ElementType.GetConstructor(Type.EmptyTypes) != null;

                TableMapping? entityMap = trackable ? _ctx.GetMapping(plan.ElementType) : null;

                // PERFORMANCE FIX (TASK 15): Hoist read-only check out of per-row loop
                bool isReadOnly = IsReadOnlyQuery();

                using var reader = command.ExecuteReaderWithInterception(_ctx, CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

                // PERFORMANCE FIX (TASK 14): Use sync materializer directly - no Task allocation at all!
                var syncMaterializer = plan.SyncMaterializer;

                // PERFORMANCE FIX (TASK 19): Respect SingleResult flag to avoid materializing unnecessary rows
                if (plan.SingleResult)
                {
                    if (reader.Read())
                    {
                        var entity = syncMaterializer(reader);
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
                }
                else
                {
                    while (reader.Read())
                    {
                        var entity = syncMaterializer(reader);
                        entity = ProcessEntity(entity, trackable, entityMap, isReadOnly);
                        list.Add(entity);
                    }
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

        /// <summary>
        /// Processes an entity after materialization, optionally tracking it and enabling lazy loading.
        /// </summary>
        /// <param name="entity">The materialized entity.</param>
        /// <param name="trackable">Whether the entity type is trackable.</param>
        /// <param name="entityMap">The table mapping for the entity.</param>
        /// <param name="isReadOnly">
        /// PERFORMANCE FIX (TASK 15): Whether this is a read-only query (hoisted from per-row check).
        /// </param>
        /// <returns>The processed entity (may be a tracking proxy).</returns>
        // PERFORMANCE OPTIMIZATION 14: Aggressive inlining for per-row hot path
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private object ProcessEntity(object entity, bool trackable, TableMapping? entityMap, bool isReadOnly)
        {
            if (!trackable)
                return entity;

            // PERFORMANCE FIX (TASK 15): Use pre-computed isReadOnly flag instead of calling method
            if (isReadOnly)
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

        // PERFORMANCE FIX (TASK 7): Removed EstimateCapacity method entirely
        // This method used expensive regex parsing on every query execution
        // Replaced with direct Take value from QueryPlan

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

            // RELIABILITY FIX (TASK 14): Removed try-catch with manual cmd.DisposeAsync
            // MaterializeAsync already owns the command's lifetime via "await using var command = cmd"
            // Double-disposing the command causes errors. Let the caller handle disposal.
            return await _exceptionHandler.ExecuteWithExceptionHandling(async () =>
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
                            // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
                            var result = info.ResultSelector(currentOuter, list);
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
                        // FIX (TASK 5): Use configurable MaxGroupJoinSize instead of hard-coded limit
                        // Prevents unbounded memory growth while allowing users to opt-in to larger datasets
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

                if (currentOuter != null)
                {
                    var list = CreateList(info.InnerType, currentChildren);
                    // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
                    var result = info.ResultSelector(currentOuter, list);
                    resultList.Add(result);
                }

                return resultList;
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

                // PERFORMANCE FIX (TASK 9): Use MaterializerFactory instead of slow reflection
                // MaterializerFactory creates compiled IL.Emit/Expression-based materializers
                // that are 10-100x faster than reflection (read.Invoke, col.Setter)
                var factory = new MaterializerFactory();
                var materializer = factory.CreateSyncMaterializer(map, map.Type);
                var wrappedReader = offset == 0 ? reader : new OffsetDbDataReader(reader, offset);
                return materializer(wrappedReader);
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
                                // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
                                var result = info.ResultSelector(currentOuter, list);
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
                            // FIX (TASK 5): Use configurable MaxGroupJoinSize instead of hard-coded limit
                            var maxSize = _ctx.Options.MaxGroupJoinSize;
                            if (currentChildren.Count >= maxSize)
                            {
                                throw new NormQueryException(
                                    $"GroupJoin safety limit exceeded: A single group has more than {maxSize} children. " +
                                    "This may indicate a malformed query or incorrect join keys. " +
                                    "To increase this limit, set DbContextOptions.MaxGroupJoinSize to a higher value. " +
                                    "Review your GroupJoin operation and ensure join keys are correct.");
                            }

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
                        // PERFORMANCE: Pass list directly instead of .Cast<object>() which creates unnecessary enumerator
                        var result = info.ResultSelector(currentOuter, list);
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

                // PERFORMANCE FIX (TASK 9): Use MaterializerFactory instead of slow reflection
                // MaterializerFactory creates compiled IL.Emit/Expression-based materializers
                // that are 10-100x faster than reflection (read.Invoke, col.Setter)
                var factory = new MaterializerFactory();
                var materializer = factory.CreateSyncMaterializer(map, map.Type);
                var wrappedReader = offset == 0 ? reader : new OffsetDbDataReader(reader, offset);
                return materializer(wrappedReader);
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

            /// <summary>
            /// Retrieves a Boolean value from the underlying reader adjusted by the offset.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The Boolean value for the column.</returns>
            public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal + _offset);

            /// <summary>
            /// Retrieves a byte value from the underlying reader adjusted by the offset.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The byte value for the column.</returns>
            public override byte GetByte(int ordinal) => _inner.GetByte(ordinal + _offset);

            /// <summary>
            /// Reads a sequence of bytes from the specified column starting at the given offset.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <param name="dataOffset">Index within the field from which to begin the read operation.</param>
            /// <param name="buffer">Destination array for the bytes.</param>
            /// <param name="bufferOffset">Index within the buffer at which to start placing the data.</param>
            /// <param name="length">Maximum number of bytes to read.</param>
            /// <returns>The actual number of bytes read.</returns>
            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
                => _inner.GetBytes(ordinal + _offset, dataOffset, buffer, bufferOffset, length);

            /// <summary>
            /// Retrieves a single character from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The character value for the column.</returns>
            public override char GetChar(int ordinal) => _inner.GetChar(ordinal + _offset);

            /// <summary>
            /// Reads a sequence of characters from the specified column starting at the given offset.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <param name="dataOffset">Index within the field from which to begin the read operation.</param>
            /// <param name="buffer">Destination array for the characters.</param>
            /// <param name="bufferOffset">Index within the buffer at which to start placing the data.</param>
            /// <param name="length">Maximum number of characters to read.</param>
            /// <returns>The actual number of characters read.</returns>
            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
                => _inner.GetChars(ordinal + _offset, dataOffset, buffer, bufferOffset, length);

            /// <summary>
            /// Gets the data type name for the column at the specified ordinal.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The database-specific type name.</returns>
            public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal + _offset);

            /// <summary>
            /// Retrieves a <see cref="DateTime"/> value from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="DateTime"/> value for the column.</returns>
            public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal + _offset);

            /// <summary>
            /// Retrieves a <see cref="decimal"/> value from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The decimal value for the column.</returns>
            public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal + _offset);

            /// <summary>
            /// Retrieves a double-precision floating-point value from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="double"/> value for the column.</returns>
            public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal + _offset);

            /// <summary>
            /// Gets the runtime type of the column at the specified ordinal.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="Type"/> of the data stored in the column.</returns>
            public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal + _offset);

            /// <summary>
            /// Retrieves a single-precision floating-point value from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="float"/> value for the column.</returns>
            public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal + _offset);

            /// <summary>
            /// Retrieves a <see cref="Guid"/> value from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="Guid"/> value for the column.</returns>
            public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal + _offset);

            /// <summary>
            /// Retrieves a 16-bit integer from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="short"/> value for the column.</returns>
            public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal + _offset);

            /// <summary>
            /// Retrieves a 32-bit integer from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="int"/> value for the column.</returns>
            public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal + _offset);

            /// <summary>
            /// Retrieves a 64-bit integer from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The <see cref="long"/> value for the column.</returns>
            public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal + _offset);

            /// <summary>
            /// Gets the name of the column at the specified ordinal.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The column name.</returns>
            public override string GetName(int ordinal) => _inner.GetName(ordinal + _offset);

            /// <summary>
            /// Retrieves the column ordinal given its name, compensating for the offset.
            /// </summary>
            /// <param name="name">The name of the column.</param>
            /// <returns>The zero-based column ordinal.</returns>
            public override int GetOrdinal(string name) => _inner.GetOrdinal(name) - _offset;

            /// <summary>
            /// Retrieves a string value from the specified column.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The string value for the column.</returns>
            public override string GetString(int ordinal) => _inner.GetString(ordinal + _offset);

            /// <summary>
            /// Retrieves the value of the specified column as an object.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns>The value of the column.</returns>
            public override object GetValue(int ordinal) => _inner.GetValue(ordinal + _offset);

            /// <summary>
            /// Populates the provided array with values from the current row, accounting for the offset.
            /// </summary>
            /// <param name="values">Destination array for the values.</param>
            /// <returns>The number of values copied into the array.</returns>
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

            /// <summary>
            /// Determines whether the column at the specified ordinal contains <c>DBNull</c>.
            /// </summary>
            /// <param name="ordinal">Column ordinal relative to the offset.</param>
            /// <returns><c>true</c> if the column is <c>DBNull</c>; otherwise, <c>false</c>.</returns>
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

            /// <summary>
            /// Returns an enumerator that iterates through the current result set, applying the column offset.
            /// </summary>
            /// <returns>An enumerator over the underlying data reader.</returns>
            public override IEnumerator GetEnumerator() => _inner.GetEnumerator();

            /// <summary>
            /// Advances the reader to the next result set while preserving the column offset.
            /// </summary>
            /// <returns><c>true</c> if there is another result set; otherwise, <c>false</c>.</returns>
            public override bool NextResult() => _inner.NextResult();

            /// <summary>
            /// Reads the next row from the current result set.
            /// </summary>
            /// <returns><c>true</c> if there are more rows; otherwise, <c>false</c>.</returns>
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
            /// <summary>
            /// Retrieves a <see cref="System.Data.DataTable"/> that describes the column metadata for the current result set.
            /// </summary>
            /// <returns>A schema table describing the current result set.</returns>
            public override System.Data.DataTable GetSchemaTable() => _inner.GetSchemaTable()!;
        }
    }
}
