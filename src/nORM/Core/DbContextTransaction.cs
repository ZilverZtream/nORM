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

        public void Commit()
        {
            _transaction?.Commit();
            Dispose();
        }

        public async Task CommitAsync(CancellationToken ct = default)
        {
            if (_transaction != null)
                await _transaction.CommitAsync(ct).ConfigureAwait(false);
            await DisposeAsync().ConfigureAwait(false);
        }

        public void Rollback()
        {
            _transaction?.Rollback();
            Dispose();
        }

        public async Task RollbackAsync(CancellationToken ct = default)
        {
            if (_transaction != null)
                await _transaction.RollbackAsync(ct).ConfigureAwait(false);
            await DisposeAsync().ConfigureAwait(false);
        }

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
