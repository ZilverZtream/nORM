using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("BatchedNavigationLoader reflects over navigation properties to batch related entity loads; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("BatchedNavigationLoader reflects over navigation property metadata; trimming may remove the required members.")]
    public sealed class BatchedNavigationLoader : IDisposable
    {
        private readonly DbContext _context;
        private readonly Dictionary<(Type EntityType, string PropertyName), List<(object Entity, TaskCompletionSource<List<object>> Tcs, CancellationToken Ct)>> _pendingLoads = new();
        /// <summary>Atomic flag (0 = idle, 1 = processing) guarding single-flight batch execution.</summary>
        private int _processing;
        private readonly SemaphoreSlim _batchSemaphore = new(1, 1);
        private bool _batchScheduled;
        private int _hasCancelablePending;
        private long _lastEnqueueTimestamp;
        private volatile bool _disposed;
        private int _schedulerQueued;
        private int _cancellationDrainQueued;

        /// <summary>
        /// Delay in milliseconds before the batch timer fires after the first pending load is queued.
        /// Balances latency (lower value) against batching efficiency (higher value).
        /// </summary>
        private const int BatchDelayMs = 25;
        private const int CancelableBatchDelayMs = 250;

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchedNavigationLoader"/>
        /// for the specified <see cref="DbContext"/>.
        /// </summary>
        /// <param name="context">The owning context used to execute navigation queries.</param>
        /// <exception cref="ArgumentNullException"><paramref name="context"/> is <c>null</c>.</exception>
        /// <remarks>
        /// PERFORMANCE OPTIMIZATION: Uses reactive scheduling instead of polling.
        /// Async delay continuations own scheduled batch windows, avoiding a shared scheduler
        /// bottleneck or a retained thread per loader.
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
        public Task<List<object>> LoadNavigationAsync(object entity, string propertyName, CancellationToken ct = default)
        {
            try
            {
                return LoadNavigationCore(entity, propertyName, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return Task.FromCanceled<List<object>>(ct);
            }
            catch (Exception ex)
            {
                return Task.FromException<List<object>>(ex);
            }
        }

        private Task<List<object>> LoadNavigationCore(object entity, string propertyName, CancellationToken ct)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (propertyName == null) throw new ArgumentNullException(nameof(propertyName));
            ThrowIfDisposed();
            if (ct.IsCancellationRequested)
                return Task.FromCanceled<List<object>>(ct);

            var entityType = entity.GetType();
            if (TryCompleteWithoutBatch(entityType, propertyName, entity, out var completed))
                return completed;

            var key = (entityType, propertyName);
            var tcs = new TaskCompletionSource<List<object>>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (ct.CanBeCanceled)
            {
                var cancellationRegistration = ct.Register(static state =>
                {
                    var (loader, completion, token) = ((BatchedNavigationLoader, TaskCompletionSource<List<object>>, CancellationToken))state!;
                    completion.TrySetCanceled(token);
                    loader.QueueCancellationDrainWorker();
                }, (this, tcs, ct))
                ;

                _ = tcs.Task.ContinueWith(static (_, state) =>
                {
                    ((CancellationTokenRegistration)state!).Dispose();
                }, cancellationRegistration, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }

            try
            {
                _batchSemaphore.Wait(ct);
            }
            catch (OperationCanceledException)
            {
                tcs.TrySetCanceled(ct);
                return tcs.Task;
            }

            try
            {
                if (!_pendingLoads.TryGetValue(key, out var list))
                {
                    list = new List<(object, TaskCompletionSource<List<object>>, CancellationToken)>();
                    _pendingLoads[key] = list;
                }
                list.Add((entity, tcs, ct));
                Volatile.Write(ref _lastEnqueueTimestamp, Stopwatch.GetTimestamp());
                if (ct.CanBeCanceled)
                    Volatile.Write(ref _hasCancelablePending, 1);

                if (!_batchScheduled)
                    _batchScheduled = true;
            }
            finally
            {
                _batchSemaphore.Release();
            }

            if (ct.CanBeCanceled)
                QueueBatchWorker();
            else
                ProcessBatchInline();

            return tcs.Task;
        }

        /// <summary>
        /// Processes a scheduled navigation batch after the configured delay without relying on
        /// thread-pool timer callbacks.
        /// </summary>
        private void QueueBatchWorker()
        {
            if (Interlocked.Exchange(ref _schedulerQueued, 1) != 0)
                return;

            _ = ProcessScheduledBatchAsync();
        }

        private void QueueCancellationDrainWorker()
        {
            if (Interlocked.Exchange(ref _cancellationDrainQueued, 1) != 0)
                return;

            _ = DrainAfterCancellationAsync();
        }

        private async Task DrainAfterCancellationAsync()
        {
            await Task.Delay(5).ConfigureAwait(false);
            try
            {
                ProcessBatchInline();
            }
            finally
            {
                Volatile.Write(ref _cancellationDrainQueued, 0);
            }
        }

        private void ProcessBatchInline()
        {
            while (Interlocked.Exchange(ref _processing, 1) == 1)
            {
                if (_disposed)
                    return;
                Thread.Sleep(1);
            }

            try
            {
                ProcessBatch();
            }
            catch
            {
                // ProcessBatch delivers per-request errors to queued TCS instances. If an
                // unexpected exception escapes outside that path, leave future batches usable.
            }
            finally
            {
                Volatile.Write(ref _processing, 0);
                Volatile.Write(ref _schedulerQueued, 0);
                if (!_disposed && HasScheduledBatch())
                    QueueBatchWorker();
            }
        }

        private async Task ProcessScheduledBatchAsync()
        {
            while (true)
            {
                var elapsedMs = (Stopwatch.GetTimestamp() - Volatile.Read(ref _lastEnqueueTimestamp)) * 1000.0 / Stopwatch.Frequency;
                var delayMs = Volatile.Read(ref _hasCancelablePending) == 1 ? CancelableBatchDelayMs : BatchDelayMs;
                var remainingMs = delayMs - elapsedMs;
                if (remainingMs <= 0)
                    break;

                await Task.Delay(Math.Max(1, (int)Math.Ceiling(remainingMs))).ConfigureAwait(false);
                if (_disposed) return;
            }

            while (Interlocked.Exchange(ref _processing, 1) == 1)
            {
                if (_disposed) return;
                await Task.Delay(1).ConfigureAwait(false);
            }

            try
            {
                ProcessBatch();
            }
            catch
            {
                // ProcessBatch delivers per-request errors to queued TCS instances. If an
                // unexpected exception escapes outside that path, subsequent requests can still
                // schedule a new batch.
            }
            finally
            {
                Volatile.Write(ref _processing, 0);
                Volatile.Write(ref _schedulerQueued, 0);
                if (!_disposed && HasScheduledBatch())
                    QueueBatchWorker();
            }
        }

        private bool HasScheduledBatch()
        {
            _batchSemaphore.Wait();
            try
            {
                return _batchScheduled;
            }
            finally
            {
                _batchSemaphore.Release();
            }
        }

        private bool TryCompleteWithoutBatch(
            Type entityType,
            string propertyName,
            object entity,
            out Task<List<object>> completed)
        {
            var mapping = _context.GetMapping(entityType);
            if (!mapping.Relations.TryGetValue(propertyName, out var relation) ||
                relation.PrincipalKey.Getter(entity) == null)
            {
                completed = Task.FromResult(new List<object>());
                return true;
            }

            completed = null!;
            return false;
        }

        /// <summary>
        /// Processes all currently queued navigation load requests as a single batch. The method
        /// collects the pending loads, clears the queue and dispatches each group of entities and
        /// navigation property to <see cref="LoadNavigationBatchAsync"/>. A semaphore is used to
        /// ensure that only one batch is processed at a time, preventing concurrent access to the
        /// internal dictionaries.
        /// </summary>
        private void ProcessBatch()
        {
            _batchSemaphore.Wait();

            Dictionary<(Type, string), List<(object Entity, TaskCompletionSource<List<object>> Tcs, CancellationToken Ct)>> batches;
            try
            {
                batches = new Dictionary<(Type, string), List<(object Entity, TaskCompletionSource<List<object>> Tcs, CancellationToken Ct)>>(_pendingLoads);
                _pendingLoads.Clear();
                _batchScheduled = false;
                _hasCancelablePending = 0;
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

                LoadNavigationBatchAsync(entityType, propertyName, entities).GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Loads related entities for a batch of pending navigation requests sharing the same
        /// entity type and property name. The shared DB query always runs to completion with
        /// <see cref="CancellationToken.None"/>; individual caller tokens are checked only when
        /// delivering results, so one caller's cancellation does not abort the query for others.
        /// </summary>
        private async Task LoadNavigationBatchAsync(Type entityType, string propertyName,
            List<(object Entity, TaskCompletionSource<List<object>> Tcs, CancellationToken Ct)> entities)
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
        private static void DeliverEmpty(TaskCompletionSource<List<object>> tcs, CancellationToken callerCt)
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
            if (_context.RawProvider.PrefersSyncExecution)
                return LoadRelatedDataBatchSync(relation, keys, ct);

            var mapping = _context.GetMapping(relation.DependentType);
            await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);

            // X2 fix: Chunk keys by the provider's parameter limit to avoid provider-specific
            // "too many variables" errors. SQLite allows ≤ 999 params, SQL Server ≤ 2100,
            // PostgreSQL ≤ 32767. Reserve 10 params for any ambient predicates (e.g. tenant).
            var maxParams = _context.RawProvider.MaxParameters;
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

        private List<object> LoadRelatedDataBatchSync(TableMapping.Relation relation, List<object?> keys, CancellationToken ct)
        {
            var mapping = _context.GetMapping(relation.DependentType);
            _context.EnsureConnection();

            var maxParams = _context.RawProvider.MaxParameters;
            var maxKeysPerChunk = maxParams == int.MaxValue ? keys.Count : Math.Max(1, maxParams - 10);

            using var translator = Query.QueryTranslator.Rent(_context);
            var materializer = translator.CreateMaterializer(mapping, relation.DependentType);

            if (keys.Count <= maxKeysPerChunk)
                return ExecuteNavigationChunkSync(mapping, materializer, relation, keys, ct);

            var results = new List<object>(keys.Count);
            for (int offset = 0; offset < keys.Count; offset += maxKeysPerChunk)
            {
                var chunk = keys.GetRange(offset, Math.Min(maxKeysPerChunk, keys.Count - offset));
                var chunkResults = ExecuteNavigationChunkSync(mapping, materializer, relation, chunk, ct);
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
            var where = _context.RawProvider.BuildContainsClause(cmd, relation.ForeignKey.EscCol, chunk);
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

        private List<object> ExecuteNavigationChunkSync(
            TableMapping mapping,
            Func<DbDataReader, CancellationToken, Task<object>> materializer,
            TableMapping.Relation relation,
            List<object?> chunk,
            CancellationToken ct)
        {
            using var cmd = _context.CreateCommand();
            var where = _context.RawProvider.BuildContainsClause(cmd, relation.ForeignKey.EscCol, chunk);
            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {where}";

            var timeout = _context.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.ComplexSelect, cmd.CommandText);
            cmd.CommandTimeout = ToSecondsClamped(timeout);

            var results = new List<object>();
            using var reader = cmd.ExecuteReaderWithInterceptionAndCommandDispose(_context, CommandBehavior.Default);
            while (reader.Read())
            {
                ct.ThrowIfCancellationRequested();
                var entity = materializer(reader, ct).GetAwaiter().GetResult();
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
            if (Volatile.Read(ref _processing) == 1) return;

            // Cleanup is best-effort. If a batch is actively mutating/draining the queue,
            // skip this pass instead of blocking teardown or cancellation paths behind it.
            if (!_batchSemaphore.Wait(0))
                return;
            try
            {
                foreach (var key in _pendingLoads.Keys.ToList())
                {
                    var list = _pendingLoads[key];
                    for (int i = list.Count - 1; i >= 0; i--)
                    {
                        var pending = list[i];
                        if (!ReferenceEquals(pending.Entity, entity))
                            continue;

                        list.RemoveAt(i);
                        pending.Tcs.TrySetCanceled(pending.Ct.IsCancellationRequested
                            ? pending.Ct
                            : CancellationToken.None);
                    }
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

            // Do NOT call _batchSemaphore.Dispose() here: RemovePendingLoadsForEntity may
            // have passed its `if (_disposed) return` guard before _disposed was set and is
            // now racing to call _batchSemaphore.Wait(). Disposing the semaphore while that
            // thread is about to Wait() would throw ObjectDisposedException on a benign
            // teardown path. SemaphoreSlim only allocates a kernel object when AvailableWaitHandle
            // is accessed (which we never do), so GC finalization is sufficient.
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
