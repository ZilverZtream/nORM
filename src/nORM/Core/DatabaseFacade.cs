using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    public class DatabaseFacade
    {
        private readonly DbContext _context;

        internal DatabaseFacade(DbContext context)
        {
            _context = context;
        }

        public DbTransaction? CurrentTransaction => _context.CurrentTransaction;

        public async Task<DbContextTransaction> BeginTransactionAsync(CancellationToken ct = default)
        {
            if (_context.CurrentTransaction != null)
                throw new InvalidOperationException("A transaction is already active.");

            var transaction = await _context.Connection.BeginTransactionAsync(ct);
            _context.CurrentTransaction = transaction;
            return new DbContextTransaction(transaction, _context);
        }
    }
}
