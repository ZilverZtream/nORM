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

            // TRANSACTION CONTEXT VALIDATION FIX: Warn about potential mismatch with ambient transactions
            // Ambient TransactionScope may be associated with a different connection/database
            if (ambientTransaction != null && existingTransaction == null)
            {
                context.Options.Logger?.LogWarning(
                    "Ambient System.Transactions.Transaction detected but no database transaction is active. " +
                    "Ensure the TransactionScope is associated with the correct database connection. " +
                    "Connection string: {ConnectionString}, Ambient transaction ID: {TransactionId}",
                    context.Connection?.ConnectionString ?? "null",
                    ambientTransaction.TransactionInformation.LocalIdentifier);

                // Additional validation: Check if connection is enlisted in the ambient transaction
                if (context.Connection != null && context.Connection.State == System.Data.ConnectionState.Open)
                {
                    // Note: We cannot reliably detect if the connection is enlisted without attempting
                    // an operation, so we log a warning to alert developers of potential issues
                    context.Options.Logger?.LogDebug(
                        "Connection state: {State}. Verify this connection is enrolled in the ambient transaction.",
                        context.Connection.State);
                }
            }

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
