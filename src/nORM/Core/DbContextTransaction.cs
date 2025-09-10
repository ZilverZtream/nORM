using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    public sealed class DbContextTransaction : IAsyncDisposable, IDisposable
    {
        private readonly DbTransaction? _transaction;
        private readonly DbContext _context;
        private bool _completed;

        internal DbContextTransaction(DbTransaction? transaction, DbContext context)
        {
            _transaction = transaction;
            _context = context;
        }

        public DbTransaction? Transaction => _transaction;

        /// <summary>
        /// Commits the underlying database transaction and disposes the wrapper.
        /// </summary>
        public void Commit()
        {
            _transaction?.Commit();
            Dispose();
        }

        /// <summary>
        /// Asynchronously commits the underlying transaction and disposes the wrapper.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task CommitAsync(CancellationToken ct = default)
        {
            if (_transaction != null)
                await _transaction.CommitAsync(ct).ConfigureAwait(false);
            await DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Rolls back the underlying database transaction and disposes the wrapper.
        /// </summary>
        public void Rollback()
        {
            _transaction?.Rollback();
            Dispose();
        }

        /// <summary>
        /// Asynchronously rolls back the underlying transaction and disposes the wrapper.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task RollbackAsync(CancellationToken ct = default)
        {
            if (_transaction != null)
                await _transaction.RollbackAsync(ct).ConfigureAwait(false);
            await DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the transaction and clears it from the owning <see cref="DbContext"/>.
        /// </summary>
        public void Dispose()
        {
            if (!_completed)
            {
                _completed = true;
                _transaction?.Dispose();
                if (_transaction != null)
                    _context.ClearTransaction(_transaction);
            }
        }

        /// <summary>
        /// Asynchronously disposes the transaction and clears it from the context.
        /// </summary>
        /// <returns>A task representing the dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (!_completed)
            {
                _completed = true;
                if (_transaction != null)
                    await _transaction.DisposeAsync().ConfigureAwait(false);
                if (_transaction != null)
                    _context.ClearTransaction(_transaction);
            }
        }
    }
}
