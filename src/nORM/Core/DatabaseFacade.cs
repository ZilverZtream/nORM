using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    /// <summary>
    /// Provides access to database-specific functionality for a <see cref="DbContext"/>,
    /// such as transaction management.
    /// </summary>
    public class DatabaseFacade
    {
        private readonly DbContext _context;

        internal DatabaseFacade(DbContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Gets the active <see cref="DbTransaction"/> for the context, if one exists.
        /// </summary>
        public DbTransaction? CurrentTransaction => _context.CurrentTransaction;

        /// <summary>
        /// Begins a new database transaction for the current context.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A <see cref="DbContextTransaction"/> representing the started transaction.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a transaction is already active.</exception>
        public async Task<DbContextTransaction> BeginTransactionAsync(CancellationToken ct = default)
        {
            if (_context.CurrentTransaction != null)
                throw new InvalidOperationException("A transaction is already active.");
            await _context.EnsureConnectionAsync(ct).ConfigureAwait(false);
            var transaction = await _context.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            _context.CurrentTransaction = transaction;
            return new DbContextTransaction(transaction, _context);
        }
    }
}
