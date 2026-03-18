using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    /// <summary>
    /// Lightweight wrapper around <see cref="DbTransaction"/> used by the
    /// <see cref="DbContext"/> to manage ambient transactions. The wrapper ensures
    /// the context releases its reference when the transaction completes or is
    /// disposed.
    /// </summary>
    public sealed class DbContextTransaction : IAsyncDisposable, IDisposable
    {
        private readonly DbTransaction? _transaction;
        private readonly DbContext _context;
        // int field + Interlocked.CompareExchange so concurrent Commit/Dispose calls
        // never both observe _completed==false and proceed to double-dispose the transaction.
        private int _completed; // 0 = not yet completed, 1 = completed

        internal DbContextTransaction(DbTransaction? transaction, DbContext context)
        {
            _transaction = transaction;
            _context = context;
        }

        /// <summary>
        /// Gets the underlying <see cref="DbTransaction"/> represented by this wrapper.
        /// May be <c>null</c> when no transaction was started.
        /// </summary>
        public DbTransaction? Transaction => _transaction;

        /// <summary>
        /// Commits the underlying database transaction and disposes this wrapper
        /// instance. After calling this method the transaction can no longer be used
        /// and the context's current transaction reference is cleared.
        /// </summary>
        /// <summary>
        /// T-1: try/finally guarantees Dispose() (and therefore ClearTransaction) always
        /// runs even when Commit() throws. Without the finally block, an exception during
        /// commit leaves CurrentTransaction non-null and poisons the context for any
        /// subsequent BeginTransactionAsync call.
        /// </summary>
        public void Commit()
        {
            try { _transaction?.Commit(); }
            finally { Dispose(); }
        }

        /// <summary>
        /// Asynchronously commits the underlying transaction and disposes the wrapper.
        /// Uses try/finally so that CurrentTransaction is ALWAYS cleared even when
        /// CommitAsync throws (e.g. network drop during commit acknowledgement). Without the
        /// finally block a failed commit leaves the context's CurrentTransaction non-null, and
        /// the next call to BeginTransactionAsync would throw "transaction already active".
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task CommitAsync(CancellationToken ct = default)
        {
            try
            {
                if (_transaction != null)
                    await _transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                // Always clear — even if commit threw, the transaction is done (unknown state).
                await DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Rolls back the underlying database transaction and disposes this wrapper
        /// instance. Any changes made within the transaction are undone and the
        /// context is returned to a non-transactional state.
        /// Uses try/finally so that CurrentTransaction is always cleared even when Rollback throws.
        /// </summary>
        public void Rollback()
        {
            try { _transaction?.Rollback(); }
            finally { Dispose(); }
        }

        /// <summary>
        /// Asynchronously rolls back the underlying transaction and disposes the wrapper.
        /// Uses try/finally so that CurrentTransaction is ALWAYS cleared even when
        /// RollbackAsync throws. Without the finally block a failed rollback leaves the context's
        /// CurrentTransaction non-null.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task RollbackAsync(CancellationToken ct = default)
        {
            try
            {
                if (_transaction != null)
                    // Always use CancellationToken.None: a rollback must not be aborted even when
                    // the caller's token is already cancelled, otherwise the DB is left mid-transaction.
                    await _transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                // Always clear — even if rollback threw, the transaction is done (unknown state).
                await DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Disposes the transaction and clears it from the owning <see cref="DbContext"/>.
        /// Interlocked.CompareExchange ensures only one concurrent caller proceeds.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _completed, 1, 0) == 0)
            {
                _transaction?.Dispose();
                if (_transaction != null)
                    _context.ClearTransaction(_transaction);
            }
        }

        /// <summary>
        /// Asynchronously disposes the transaction and clears it from the context.
        /// Uses the same atomic gate as Dispose() to prevent double-dispose.
        /// </summary>
        /// <returns>A task representing the dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.CompareExchange(ref _completed, 1, 0) == 0)
            {
                if (_transaction != null)
                {
                    await _transaction.DisposeAsync().ConfigureAwait(false);
                    _context.ClearTransaction(_transaction);
                }
            }
        }
    }
}
