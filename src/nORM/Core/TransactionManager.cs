using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    internal sealed class TransactionManager : IAsyncDisposable, IDisposable
    {
        private readonly CancellationTokenSource? _cts;

        public DbTransaction? Transaction { get; }
        public CancellationToken Token { get; }
        public bool OwnsTransaction { get; }

        private TransactionManager(DbTransaction? transaction, bool ownsTransaction,
            CancellationTokenSource? cts, CancellationToken token)
        {
            Transaction = transaction;
            OwnsTransaction = ownsTransaction;
            _cts = cts;
            Token = token;
        }

        /// <summary>
        /// Creates a <see cref="TransactionManager"/> instance for the specified context. If no
        /// ambient transaction exists, a new transaction is started and owned by the manager.
        /// </summary>
        /// <param name="context">The <see cref="DbContext"/> managing the connection.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task producing a configured <see cref="TransactionManager"/>.</returns>
        public static async Task<TransactionManager> CreateAsync(DbContext context, CancellationToken ct)
        {
            var existingTransaction = context.Database.CurrentTransaction;
            var ambientTransaction = System.Transactions.Transaction.Current;
            var ownsTransaction = existingTransaction == null && ambientTransaction == null;

            DbTransaction? transaction = null;
            CancellationTokenSource? cts = null;
            var token = ct;

            if (ownsTransaction)
            {
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                transaction = await context.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);

                // Create a CTS that cancels if the ambient token cancels.
                cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                token = cts.Token;
            }

            return new TransactionManager(transaction, ownsTransaction, cts, token);
        }

        /// <summary>
        /// Commits the underlying transaction if this instance owns it.
        /// </summary>
        /// <returns>A task that completes once the commit operation has finished.</returns>
        public async ValueTask CommitAsync()
        {
            if (OwnsTransaction && Transaction != null)
                await Transaction.CommitAsync(Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Rolls back the underlying transaction if owned by this instance.
        /// </summary>
        /// <returns>A task that completes when the rollback has finished.</returns>
        public async ValueTask RollbackAsync()
        {
            if (OwnsTransaction && Transaction != null)
                await Transaction.RollbackAsync(Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously disposes the transaction and linked cancellation source.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (OwnsTransaction && Transaction != null)
            {
                try { await Transaction.DisposeAsync().ConfigureAwait(false); } catch { }
            }
            _cts?.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the underlying transaction and cancellation source.
        /// </summary>
        public void Dispose()
        {
            if (OwnsTransaction && Transaction != null)
            {
                try { Transaction.Dispose(); } catch { }
            }
            _cts?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
