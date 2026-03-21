using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Execution;
using nORM.Providers;

namespace nORM.Navigation
{
    /// <summary>
    /// Coordinates deferred loading of navigation properties by batching multiple
    /// requests into a single query. This reduces database round trips when many
    /// entities require the same navigation data.
    /// </summary>
    public sealed class BatchedNavigationLoader : IDisposable
    {
        private readonly DbContext _context;
        private readonly Dictionary<(Type EntityType, string PropertyName), List<(object Entity, TaskCompletionSource<object> Tcs, CancellationToken Ct)>> _pendingLoads = new();
        private Timer? _batchTimer;
        /// <summary>Atomic flag (0 = idle, 1 = processing) guarding single-flight batch execution.</summary>
        private int _processing;
        private readonly SemaphoreSlim _batchSemaphore = new(1, 1);
        private volatile bool _disposed;

        /// <summary>
        /// Delay in milliseconds before the batch timer fires after the first pending load is queued.
        /// Balances latency (lower value) against batching efficiency (higher value).
        /// </summary>
        private const int BatchDelayMs = 10;

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchedNavigationLoader"/>
        /// for the specified <see cref="DbContext"/>.
        /// </summary>
        /// <param name="context">The owning context used to execute navigation queries.</param>
        /// <exception cref="ArgumentNullException"><paramref name="context"/> is <c>null</c>.</exception>
        /// <remarks>
        /// PERFORMANCE OPTIMIZATION: Uses reactive timer scheduling instead of polling.
        /// Timer is only active when there are pending loads, reducing CPU overhead.
        /// </remarks>
        public BatchedNavigationLoader(DbContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            NavigationPropertyExtensions.RegisterLoader(this);
            _context.RegisterForDisposal(this);
        }

        /// <summary>
        /// Queues a request to load the specified navigation property for an entity. Multiple
        /// requests for the same navigation are batched together and executed in a single
        /// database query to reduce round trips.
        /// </summary>
        /// <param name="entity">The entity instance whose navigation should be loaded.</param>
        /// <param name="propertyName">Name of the navigation property to load.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A list of related entities once the batch has been processed.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="entity"/> or <paramref name="propertyName"/> is <c>null</c>.</exception>
        /// <exception cref="ObjectDisposedException">The loader has been disposed.</exception>
        public async Task<List<object>> LoadNavigationAsync(object entity, string propertyName, CancellationToken ct = default)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (propertyName == null) throw new ArgumentNullException(nameof(propertyName));
            ThrowIfDisposed();

            var entityType = entity.GetType();
            var key = (entityType, propertyName);
            // RunContinuationsAsynchronously prevents synchronous continuations from running
            // inline on the thread that calls SetResult/SetException/SetCanceled, avoiding
            // potential stack dives when many callers await the same batch.
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            await _batchSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (!_pendingLoads.TryGetValue(key, out var list))
                {
                    list = new List<(object, TaskCompletionSource<object>, CancellationToken)>();
                    _pendingLoads[key] = list;
                }
                list.Add((entity, tcs, ct));

                // Schedule a one-shot timer when the first pending item is queued.
                // Check _batchTimer first to avoid unnecessary count calculation.
                if (_batchTimer == null)
                {
                    // Count total pending items without LINQ allocation.
                    int totalCount = 0;
                    foreach (var kvp in _pendingLoads)
                    {
                        totalCount += kvp.Value.Count;
                        if (totalCount > 1) break; // Early exit — only care whether count == 1
                    }
                    // Create timer INSIDE the semaphore to prevent TOCTOU race
                    // where ProcessBatchAsync disposes/nulls _batchTimer between
                    // the check and the assignment.
                    if (totalCount == 1)
                        _batchTimer = new Timer(TimerTick, null, BatchDelayMs, Timeout.Infinite);
                }
            }
            finally
            {
                _batchSemaphore.Release();
            }

            return (List<object>)await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Callback invoked by the internal one-shot timer to trigger processing of pending
        /// navigation loads. Uses an atomic flag to ensure only one batch is processed at a
        /// time and dispatches the work to the thread pool.
        /// </summary>
        /// <param name="state">Unused timer state object.</param>
        private void TimerTick(object? state)
        {
            if (_disposed) return;
            if (Interlocked.Exchange(ref _processing, 1) == 1) return;
            _ = Task.Run(async () =>
            {
                try
                {
                    await ProcessBatchAsync().ConfigureAwait(false);
                }
                finally
                {
                    Volatile.Write(ref _processing, 0);
                }
            });
        }

        /// <summary>
        /// Processes all currently queued navigation load requests as a single batch. The method
        /// collects the pending loads, clears the queue and dispatches each group of entities and
        /// navigation property to <see cref="LoadNavigationBatchAsync"/>. A semaphore is used to
        /// ensure that only one batch is processed at a time, preventing concurrent access to the
        /// internal dictionaries.
        /// </summary>
        private async Task ProcessBatchAsync()
        {
            await _batchSemaphore.WaitAsync().ConfigureAwait(false);

            Dictionary<(Type, string), List<(object Entity, TaskCompletionSource<object> Tcs, CancellationToken Ct)>> batches;
            try
            {
                batches = new Dictionary<(Type, string), List<(object Entity, TaskCompletionSource<object> Tcs, CancellationToken Ct)>>(_pendingLoads);
                _pendingLoads.Clear();

                // Dispose the one-shot timer after draining the queue (reactive approach).
                _batchTimer?.Dispose();
                _batchTimer = null;
            }
            finally
            {
                _batchSemaphore.Release();
            }

            // Execute batch loads outside the semaphore to avoid holding the lock
            // during potentially long-running DB operations.
            foreach (var kvp in batches)
            {
                var (entityType, propertyName) = kvp.Key;
                var entities = kvp.Value;

                await LoadNavigationBatchAsync(entityType, propertyName, entities).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Loads related entities for a batch of pending navigation requests sharing the same
        /// entity type and property name. The shared DB query always runs to completion with
        /// <see cref="CancellationToken.None"/>; individual caller tokens are checked only when
        /// delivering results, so one caller's cancellation does not abort the query for others.
        /// </summary>
        private async Task LoadNavigationBatchAsync(Type entityType, string propertyName,
            List<(object Entity, TaskCompletionSource<object> Tcs, CancellationToken Ct)> entities)
        {
            // Shared DB query always runs to completion; per-caller tokens checked on delivery.
            var ct = CancellationToken.None;

            try
            {
                var mapping = _context.GetMapping(entityType);
                if (!mapping.Relations.TryGetValue(propertyName, out var relation))
                {
                    foreach (var (_, tcs, callerCt) in entities)
                        DeliverEmpty(tcs, callerCt);
                    return;
                }

                var keys = entities.Select(e => relation.PrincipalKey.Getter(e.Entity))
                                   .Where(k => k != null)
                                   .Distinct()
                                   .ToList();

                if (keys.Count == 0)
                {
                    foreach (var (_, tcs, callerCt) in entities)
                        DeliverEmpty(tcs, callerCt);
                    return;
                }

                var relatedData = await LoadRelatedDataBatch(relation, keys, ct).ConfigureAwait(false);
                var grouped = relatedData.GroupBy(relation.ForeignKey.Getter)
                                         .ToDictionary(g => g.Key!, g => g.ToList());

                foreach (var (entity, tcs, callerCt) in entities)
                {
                    if (callerCt.IsCancellationRequested)
                    {
                        tcs.TrySetCanceled(callerCt);
                        continue;
                    }
                    var key = relation.PrincipalKey.Getter(entity);
                    var related = key != null && grouped.TryGetValue(key, out var list) ? list : new List<object>();
                    tcs.TrySetResult(related);
                }
            }
            catch (OperationCanceledException oce)
            {
                foreach (var (_, tcs, callerCt) in entities)
                {
                    // Deliver cancellation with the caller's own token when possible.
                    tcs.TrySetCanceled(callerCt.IsCancellationRequested ? callerCt : oce.CancellationToken);
                }
            }
            catch (Exception ex)
            {
                foreach (var (_, tcs, callerCt) in entities)
                {
                    if (callerCt.IsCancellationRequested)
                        tcs.TrySetCanceled(callerCt);
                    else
                        tcs.TrySetException(ex);
                }
            }
        }

        /// <summary>
        /// Delivers an empty result to a single caller's <see cref="TaskCompletionSource{T}"/>,
        /// honouring the caller's cancellation token. If cancellation has already been requested,
        /// the task is cancelled instead of resolved with an empty list.
        /// </summary>
        private static void DeliverEmpty(TaskCompletionSource<object> tcs, CancellationToken callerCt)
        {
            if (callerCt.IsCancellationRequested)
                tcs.TrySetCanceled(callerCt);
            else
                tcs.TrySetResult(new List<object>());
        }

        /// <summary>
        /// Loads all related entities for the supplied foreign key values. When the number of keys
        /// exceeds the provider's parameter limit (e.g. 999 for SQLite, 2100 for SQL Server), the
        /// keys are automatically split into chunks and multiple queries are issued, with results
        /// merged before returning.
        /// </summary>
        /// <param name="relation">Metadata describing the relationship being loaded.</param>
        /// <param name="keys">The set of principal key values to retrieve related entities for.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A list containing the materialized dependent entities.</returns>
        private async Task<List<object>> LoadRelatedDataBatch(TableMapping.Relation relation, List<object?> keys, CancellationToken ct)
        {
            var mapping = _context.GetMapping(relation.DependentType);
            await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);

            // X2 fix: Chunk keys by the provider's parameter limit to avoid provider-specific
            // "too many variables" errors. SQLite allows ≤ 999 params, SQL Server ≤ 2100,
            // PostgreSQL ≤ 32767. Reserve 10 params for any ambient predicates (e.g. tenant).
            var maxParams = _context.Provider.MaxParameters;
            var maxKeysPerChunk = maxParams == int.MaxValue ? keys.Count : Math.Max(1, maxParams - 10);

            using var translator = Query.QueryTranslator.Rent(_context);
            var materializer = translator.CreateMaterializer(mapping, relation.DependentType);

            if (keys.Count <= maxKeysPerChunk)
                return await ExecuteNavigationChunkAsync(mapping, materializer, relation, keys, ct).ConfigureAwait(false);

            // Issue one query per chunk and merge all results.
            var results = new List<object>(keys.Count);
            for (int offset = 0; offset < keys.Count; offset += maxKeysPerChunk)
            {
                var chunk = keys.GetRange(offset, Math.Min(maxKeysPerChunk, keys.Count - offset));
                var chunkResults = await ExecuteNavigationChunkAsync(mapping, materializer, relation, chunk, ct).ConfigureAwait(false);
                results.AddRange(chunkResults);
            }
            return results;
        }

        /// <summary>
        /// Executes a single <c>WHERE IN</c> query for a chunk of foreign key values and
        /// materializes the results into tracked entities.
        /// </summary>
        private async Task<List<object>> ExecuteNavigationChunkAsync(
            TableMapping mapping,
            Func<DbDataReader, CancellationToken, Task<object>> materializer,
            TableMapping.Relation relation,
            List<object?> chunk,
            CancellationToken ct)
        {
            using var cmd = _context.CreateCommand();
            var where = _context.Provider.BuildContainsClause(cmd, relation.ForeignKey.EscCol, chunk);
            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {where}";

            var timeout = _context.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);
            cmd.CommandTimeout = ToSecondsClamped(timeout);

            var results = new List<object>();
            using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_context, CommandBehavior.Default, ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = await materializer(reader, ct).ConfigureAwait(false);
                var entry = _context.ChangeTracker.Track(entity, EntityState.Unchanged, mapping);
                entity = entry.Entity!;
                NavigationPropertyExtensions._navigationContexts.GetValue(entity, _ => new NavigationContext(_context, relation.DependentType));
                results.Add(entity);
            }
            return results;
        }

        /// <summary>
        /// Removes any queued navigation load requests associated with the specified entity.
        /// This is typically called when an entity is disposed or otherwise no longer requires
        /// lazy-loading operations.
        /// </summary>
        /// <param name="entity">The entity whose pending navigation loads should be cleared.</param>
        internal void RemovePendingLoadsForEntity(object entity)
        {
            if (entity == null) return;
            if (_disposed) return;

            // Use the same semaphore as LoadNavigationAsync/ProcessBatchAsync
            // to prevent concurrent Dictionary mutation.
            _batchSemaphore.Wait();
            try
            {
                foreach (var key in _pendingLoads.Keys.ToList())
                {
                    var list = _pendingLoads[key];
                    list.RemoveAll(e => ReferenceEquals(e.Entity, entity));
                    if (list.Count == 0)
                        _pendingLoads.Remove(key);
                }
            }
            finally
            {
                _batchSemaphore.Release();
            }
        }

        /// <summary>
        /// Releases resources used by the loader, cancels any pending navigation requests, and
        /// unregisters it from the navigation system. Safe to call multiple times.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            NavigationPropertyExtensions.UnregisterLoader(this);
            _batchTimer?.Dispose();
            _batchTimer = null;

            // Drain and cancel pending TCS entries under the semaphore so that a concurrent
            // RemovePendingLoadsForEntity (which also holds the semaphore) cannot race on
            // _pendingLoads. Acquire with Wait() — Dispose is synchronous and called at most
            // once (guarded by the _disposed flag above), so blocking here is safe.
            // After the semaphore is released we do NOT call _batchSemaphore.Dispose()
            // immediately — RemovePendingLoadsForEntity may still be in its own Wait().
            // The semaphore is finalized by GC; SemaphoreSlim wraps a lightweight kernel
            // object only if AvailableWaitHandle was accessed, which we do not do here.
            _batchSemaphore.Wait();
            try
            {
                // Cancel any pending TCS entries so callers are not left waiting indefinitely.
                foreach (var kvp in _pendingLoads)
                {
                    foreach (var (_, tcs, callerCt) in kvp.Value)
                    {
                        tcs.TrySetCanceled(callerCt.IsCancellationRequested ? callerCt : CancellationToken.None);
                    }
                }
                _pendingLoads.Clear();
            }
            finally
            {
                _batchSemaphore.Release();
            }

            _batchSemaphore.Dispose();
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this loader has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(BatchedNavigationLoader));
        }

        /// <summary>
        /// Converts a <see cref="TimeSpan"/> to whole seconds suitable for
        /// <see cref="System.Data.Common.DbCommand.CommandTimeout"/>, clamping to a minimum of 1
        /// and guarding against overflow. Mirrors the helper used by <see cref="DbContext"/>.
        /// </summary>
        private static int ToSecondsClamped(TimeSpan t)
        {
            if (t.TotalSeconds > int.MaxValue)
                return int.MaxValue;

            return Math.Max(1, (int)Math.Ceiling(t.TotalSeconds));
        }
    }
}
