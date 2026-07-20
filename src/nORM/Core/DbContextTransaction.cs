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
        // False when the transaction was supplied via Database.UseTransaction: the caller owns its lifetime,
        // so commit/rollback and clearing the context reference are allowed but the handle is never disposed.
        private readonly bool _ownsTransaction;
        // int field + Interlocked.CompareExchange so concurrent Commit/Dispose calls
        // never both observe _completed==false and proceed to double-dispose the transaction.
        private int _completed; // 0 = not yet completed, 1 = completed

        internal DbContextTransaction(DbTransaction? transaction, DbContext context, bool ownsTransaction = true)
        {
            _transaction = transaction;
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _ownsTransaction = ownsTransaction;
        }

        /// <summary>
        /// Gets the underlying <see cref="DbTransaction"/> represented by this wrapper.
        /// May be <c>null</c> when no transaction was started.
        /// </summary>
        public DbTransaction? Transaction
        {
            get
            {
                _context.ThrowIfStrictProviderMobilityEscapeHatch(nameof(Transaction));
                return _transaction;
            }
        }

        /// <summary>
        /// Commits the underlying database transaction and disposes this wrapper
        /// instance. After calling this method the transaction can no longer be used
        /// and the context's current transaction reference is cleared.
        /// Uses try/finally so that Dispose (and therefore ClearTransaction) always
        /// runs even when Commit throws, preventing a poisoned CurrentTransaction reference.
        /// </summary>
        public void Commit()
        {
            if (Interlocked.CompareExchange(ref _completed, 1, 0) != 0)
                return;

            try { _transaction?.Commit(); }
            finally { DisposeTransactionAndClear(); }
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
            if (Interlocked.CompareExchange(ref _completed, 1, 0) != 0)
                return;

            try
            {
                if (_transaction != null)
                    // Always use CancellationToken.None: a commit must not be aborted even when the
                    // caller's token is already cancelled. The database may have already committed;
                    // surfacing OperationCanceledException at this point would leave the caller unable
                    // to distinguish a cancelled-before-commit from an ambiguous partial-commit.
                    // This mirrors the behaviour of RollbackAsync and internal TransactionManager.CommitAsync.
                    await _transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                // Always clear — even if commit threw, the transaction is done (unknown state).
                await DisposeTransactionAndClearAsync().ConfigureAwait(false);
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
            if (Interlocked.CompareExchange(ref _completed, 1, 0) != 0)
                return;

            try
            {
                _transaction?.Rollback();
                // The rollback undid every insert made in this transaction; reset the keys those
                // inserts stamped so still-Added entities are re-inserted on the next SaveChanges.
                _context.ResetGeneratedKeysAfterFullRollback();
            }
            finally { DisposeTransactionAndClear(); }
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
            if (Interlocked.CompareExchange(ref _completed, 1, 0) != 0)
                return;

            try
            {
                if (_transaction != null)
                    // Always use CancellationToken.None: a rollback must not be aborted even when
                    // the caller's token is already cancelled, otherwise the DB is left mid-transaction.
                    await _transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                // The rollback undid every insert made in this transaction; reset the keys those
                // inserts stamped so still-Added entities are re-inserted on the next SaveChanges.
                _context.ResetGeneratedKeysAfterFullRollback();
            }
            finally
            {
                // Always clear — even if rollback threw, the transaction is done (unknown state).
                await DisposeTransactionAndClearAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates a savepoint inside this nORM-managed transaction without
        /// exposing the underlying provider transaction handle.
        /// </summary>
        /// <param name="name">Name of the savepoint to create.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the savepoint has been created.</returns>
        public Task CreateSavepointAsync(string name, CancellationToken ct = default)
        {
            EnsureUsable();
            return _context.CreateSavepointCoreAsync(_transaction!, name, ct);
        }

        /// <summary>
        /// Rolls this nORM-managed transaction back to a previously created
        /// savepoint without exposing the underlying provider transaction handle.
        /// </summary>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when rollback to the savepoint has finished.</returns>
        public Task RollbackToSavepointAsync(string name, CancellationToken ct = default)
        {
            EnsureUsable();
            return _context.RollbackToSavepointCoreAsync(_transaction!, name, ct);
        }

        /// <summary>
        /// Releases a previously created savepoint inside this nORM-managed transaction without exposing the
        /// underlying provider transaction handle. The work done since the savepoint is KEPT — the savepoint
        /// simply stops being a rollback target.
        /// </summary>
        /// <param name="name">Name of the savepoint to release.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the savepoint has been released.</returns>
        public Task ReleaseSavepointAsync(string name, CancellationToken ct = default)
        {
            EnsureUsable();
            return _context.ReleaseSavepointCoreAsync(_transaction!, name, ct);
        }

        /// <summary>
        /// Disposes the transaction and clears it from the owning <see cref="DbContext"/>.
        /// Interlocked.CompareExchange ensures only one concurrent caller proceeds.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _completed, 1, 0) == 0)
            {
                ResetGeneratedKeysIfOwnedRollback();
                DisposeTransactionAndClear();
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
                ResetGeneratedKeysIfOwnedRollback();
                await DisposeTransactionAndClearAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reaching Dispose with <c>_completed == 0</c> means the transaction was neither committed nor
        /// explicitly rolled back. An owned transaction rolls back on dispose (ADO.NET contract), so — before
        /// <see cref="DbContext.ClearTransaction"/> discards the key snapshot — reset the DB-generated keys
        /// stamped during it, exactly as <see cref="Rollback"/>/<see cref="RollbackAsync"/> do; otherwise the
        /// still-Added entities are silently dropped by the skip-already-inserted guard on the next
        /// SaveChanges. A caller-owned handle (<c>Database.UseTransaction</c>) is not disposed here and its
        /// lifetime belongs to the caller, so its keys are left untouched.
        /// </summary>
        private void ResetGeneratedKeysIfOwnedRollback()
        {
            if (_ownsTransaction && _transaction != null)
                _context.ResetGeneratedKeysAfterFullRollback();
        }

        private void EnsureUsable()
        {
            if (_transaction == null || Volatile.Read(ref _completed) != 0)
                throw new NormUsageException("No active transaction.");
        }

        private void DisposeTransactionAndClear()
        {
            if (_transaction == null)
                return;

            try
            {
                // A caller-owned transaction (Database.UseTransaction) is never disposed here — only the
                // context's reference to it is released; the caller disposes their own handle.
                if (_ownsTransaction)
                    _transaction.Dispose();
            }
            finally
            {
                _context.ClearTransaction(_transaction);
            }
        }

        private async ValueTask DisposeTransactionAndClearAsync()
        {
            if (_transaction == null)
                return;

            try
            {
                if (_ownsTransaction)
                    await _transaction.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _context.ClearTransaction(_transaction);
            }
        }
    }
}
