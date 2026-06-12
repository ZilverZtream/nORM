using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using nORM.Versioning;
#nullable enable
namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Releases resources used by the context. When <paramref name="disposing"/>
        /// is <c>true</c>, both managed and unmanaged resources are released; otherwise
        /// only unmanaged resources are cleaned up.
        /// </summary>
        /// <param name="disposing">Indicates whether the method was invoked from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                // Use Dispose(WaitHandle) to ensure timer callbacks complete before proceeding.
                // Prevents ObjectDisposedException if a callback fires concurrently with Dispose.
                {
                    var waitHandle = new ManualResetEvent(false);
                    _cleanupTimer.Dispose(waitHandle);
                    waitHandle.WaitOne(TimeSpan.FromSeconds(CleanupTimerDrainTimeoutSeconds));
                    waitHandle.Dispose();
                }
                _providerInitLock.Dispose();
                _temporalInitLock.Dispose();
                DisposePreparedInsertCache();
                DisposeFastPathPreparedCommandCache();

                // Copy disposables to a local list inside the lock, then dispose outside the lock.
                // Prevents deadlock if a disposable's Dispose() acquires locks or accesses DbContext.
                List<IDisposable> toDispose = new();
                lock (_disposablesLock)
                {
                    CleanupDisposablesInternal();
                    for (var node = _disposables.First; node != null;)
                    {
                        var next = node.Next;
                        if (node.Value.TryGetTarget(out var d))
                        {
                            toDispose.Add(d);
                        }
                        _disposables.Remove(node);
                        node = next;
                    }
                }

                // Dispose items outside the lock to prevent deadlocks.
                // Exceptions are logged (not swallowed silently) so that resource-leak
                // failures surface in diagnostics, but disposal continues for remaining items.
                foreach (var d in toDispose)
                {
                    try
                    {
                        d.Dispose();
                    }
                    catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
                    {
                        Options.Logger?.LogDebug(ex,
                            "Exception during disposal of tracked resource {ResourceType}.",
                            d.GetType().Name);
                    }
                }

                // Only dispose the connection when this context owns it.
                if (_ownsConnection)
                    _cn?.Dispose();
                _disposed = true;
            }
        }

        private void CleanupDisposablesInternal()
        {
            for (var node = _disposables.First; node != null;)
            {
                var next = node.Next;
                if (!node.Value.TryGetTarget(out _))
                {
                    _disposables.Remove(node);
                }
                node = next;
            }
        }

        private void CleanupDisposables(object? state = null)
        {
            lock (_disposablesLock)
            {
                CleanupDisposablesInternal();
            }
        }

        private async Task CleanupDisposablesAsync()
        {
            List<IDisposable> toDispose = new();
            lock (_disposablesLock)
            {
                for (var n = _disposables.First; n != null;)
                {
                    var next = n.Next;
                    if (n.Value.TryGetTarget(out var d))
                        toDispose.Add(d);
                    _disposables.Remove(n);
                    n = next;
                }
            }
            foreach (var d in toDispose)
            {
                if (d is IAsyncDisposable ad) await ad.DisposeAsync().ConfigureAwait(false);
                else d.Dispose();
            }
        }

        /// <summary>
        /// Registers an <see cref="IDisposable"/> resource to be disposed when the context is disposed.
        /// </summary>
        /// <param name="disposable">The resource to track for disposal.</param>
        public void RegisterForDisposal(IDisposable disposable)
        {
            if (disposable != null)
            {
                lock (_disposablesLock)
                {
                    CleanupDisposablesInternal();
                    _disposables.AddLast(new WeakReference<IDisposable>(disposable));
                }
            }
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this context has been disposed.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name,
                    "This DbContext instance has been disposed. Create a new instance to continue.");
        }

        internal void ThrowIfStrictProviderMobilityEscapeHatch(string feature)
        {
            if (!IsStrictProviderMobility)
                return;

            throw new NormUnsupportedFeatureException(ProviderMobilityTranslator.BuildStrictViolationMessage(feature));
        }

        /// <summary>
        /// Releases all resources used by the context.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        /// <summary>
        /// Asynchronously releases all resources used by the context, including
        /// active connections and registered disposables.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                // Use WaitHandle pattern to safely stop the timer, preventing a race condition
                // where the timer callback fires concurrently with disposal.
                {
                    var waitHandle = new ManualResetEvent(false);
                    _cleanupTimer.Dispose(waitHandle);
                    // Use Task.Run with timeout to avoid blocking the async context indefinitely
                    // if a timer callback deadlocks or takes too long.
                    await Task.Run(() => waitHandle.WaitOne(TimeSpan.FromSeconds(CleanupTimerDrainTimeoutSeconds))).ConfigureAwait(false);
                    waitHandle.Dispose();
                }
                _providerInitLock.Dispose();
                _temporalInitLock.Dispose();
                await DisposePreparedInsertCacheAsync().ConfigureAwait(false);
                await DisposeFastPathPreparedCommandCacheAsync().ConfigureAwait(false);
                await CleanupDisposablesAsync().ConfigureAwait(false);
                // Only dispose the connection when this context owns it.
                // _cn is always non-null (set in constructor with null-guard), so the null
                // check is defensive only against theoretical subclass tampering.
                if (_ownsConnection && _cn != null)
                    await _cn.DisposeAsync().ConfigureAwait(false);
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }
    }
}
