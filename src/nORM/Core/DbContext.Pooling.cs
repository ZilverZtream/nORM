using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        // Set by NormDbContextPool when this context is leased; invoked from Dispose/DisposeAsync so a pooled
        // context returns to its pool instead of tearing down. Null for non-pooled contexts (normal disposal).
        private Func<bool>? _poolReturnHook;

        /// <summary>Assigns (or clears) the pool-return hook used when this context is leased from a pool.</summary>
        internal void SetPoolReturnHook(Func<bool>? hook) => _poolReturnHook = hook;

        // Invoked at the start of Dispose/DisposeAsync. Returns true when the context was returned to its pool
        // (the caller must skip teardown). The hook is cleared before invoking so a re-entrant dispose during
        // reset cannot loop; the pool re-sets it when the context is leased again.
        private bool TryReturnToPoolOnDispose()
        {
            var hook = _poolReturnHook;
            if (hook == null || _disposed)
                return false;
            _poolReturnHook = null;
            return hook();
        }

        /// <summary>
        /// Resets per-request state so this context can be reused from a pool (see
        /// <c>IServiceCollection.AddNormPool&lt;TContext&gt;</c>), while KEEPING the warm per-context caches
        /// (entity mappings, prepared commands, fast-path SQL, the query provider) and the context's own
        /// connection — reusing those warm is the entire point of pooling.
        /// </summary>
        /// <returns>
        /// <c>true</c> when the context was reset and is safe to return to the pool; <c>false</c> when it must
        /// NOT be pooled and should instead be disposed — namely when it has already been disposed or holds a
        /// live transaction (explicit or ambient <see cref="System.Transactions.Transaction"/>), because
        /// reusing such a context would leak an open transaction into the next lease.
        /// </returns>
        internal bool TryResetForPooling()
        {
            if (_disposed)
                return false;
            // A context mid-transaction (explicit begin, its context-transaction wrapper, or an enlisted
            // ambient System.Transactions transaction) must not be pooled: the next lease would inherit the
            // open transaction and its uncommitted state.
            if (CurrentTransaction != null || _currentContextTransaction != null || _registeredAmbientTransaction != null)
                return false;

            // A lease ends the query scope, so dispose the query-scoped resources registered for disposal
            // (navigation loaders, etc.) and clear the list — otherwise they accumulate across leases. Mirror
            // the Dispose path: collect targets under the lock, dispose outside it to avoid re-entrant locks.
            var toDispose = new List<IDisposable>();
            lock (_disposablesLock)
            {
                for (var node = _disposables.First; node != null;)
                {
                    var next = node.Next;
                    if (node.Value.TryGetTarget(out var d))
                        toDispose.Add(d);
                    _disposables.Remove(node);
                    node = next;
                }
            }
            foreach (var d in toDispose)
            {
                try
                {
                    d.Dispose();
                }
                catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
                {
                    Options.Logger?.LogDebug(ex,
                        "Exception disposing tracked resource {ResourceType} during pool reset.", d.GetType().Name);
                }
            }

            // Clear all tracked entities, the identity map, and any pending relationship key fixups.
            ChangeTracker.Clear();
            ChangeTracker.ClearPendingReferenceKeyFixups();

            // CRITICAL for tenant isolation: null the applied native tenant session key so the next lease
            // re-applies ITS OWN tenant session. Leaving it set is a cross-tenant leak hazard.
            _nativeTenantSessionAppliedKey = null;

            // With no live transaction (checked above) these snapshots should already be null; clear any
            // residue defensively so no key snapshot survives into the next lease.
            _savepointKeySnapshots = null;
            _transactionKeySnapshot = null;
            _ambientKeySnapshot = null;

            return true;
        }
    }
}
