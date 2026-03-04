using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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

            DbTransaction? transaction = existingTransaction;
            CancellationTokenSource? cts = null;
            var token = ct;

            if (ownsTransaction)
            {
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var connection = context.Connection ?? throw new InvalidOperationException("Database connection is not initialized.");
                transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

                // Create a CTS that cancels if the ambient token cancels.
                cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                token = cts.Token;
            }
            else if (ambientTransaction != null && existingTransaction == null)
            {
                // TX-1: When an ambient System.Transactions.Transaction exists but no explicit
                // DbTransaction has been started, explicitly enlist the connection so that
                // providers that have auto-enlist disabled (e.g. Enlist=False in connection string,
                // or providers that never auto-enlist) are still correctly included in the
                // TransactionScope and will roll back if the scope is disposed without Complete().
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var connection = context.Connection;
                if (connection != null && connection.State == System.Data.ConnectionState.Open)
                {
                    try
                    {
                        connection.EnlistTransaction(System.Transactions.Transaction.Current);
                        context.Options.Logger?.LogDebug(
                            "Explicitly enlisted connection in ambient transaction {TransactionId}.",
                            ambientTransaction.TransactionInformation.LocalIdentifier);
                    }
                    catch (Exception ex)
                    {
                        // The provider does not support EnlistTransaction (e.g. some in-memory or
                        // test-double implementations). Log a warning so developers are aware that
                        // operations may commit outside the ambient scope.
                        context.Options.Logger?.LogWarning(
                            "Could not enlist connection in ambient transaction {TransactionId}: {Message}. " +
                            "Operations may commit independently of the TransactionScope.",
                            ambientTransaction.TransactionInformation.LocalIdentifier,
                            ex.Message);
                    }
                }
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
        /// Always uses <see cref="CancellationToken.None"/> so that a canceled caller token
        /// cannot abort the rollback and leave an incomplete transaction state.
        /// </summary>
        /// <param name="_">Ignored — rollback always uses <see cref="CancellationToken.None"/>.</param>
        /// <returns>A task that completes when the rollback has finished.</returns>
        public async ValueTask RollbackAsync(CancellationToken _ = default)
        {
            if (OwnsTransaction && Transaction != null)
                // TX-1: Use CancellationToken.None — rollback must complete regardless of caller cancellation
                await Transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously disposes the transaction and linked cancellation source.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (OwnsTransaction && Transaction != null)
            {
                // ERROR MASKING FIX: Log exceptions instead of silently swallowing them
                // While Dispose should not throw, logging helps diagnose connection state issues
                try
                {
                    await Transaction.DisposeAsync().ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                    // Expected - transaction already disposed, safe to ignore
                }
                catch (InvalidOperationException)
                {
                    // Expected - transaction in invalid state for disposal, safe to ignore
                }
                catch (Exception ex)
                {
                    // Unexpected exception during dispose - log for diagnostics but don't throw
                    // per .NET guidelines (Dispose should not throw)
                    Debug.WriteLine($"Warning: Exception disposing transaction: {ex.GetType().Name}: {ex.Message}");
                }
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
                // ERROR MASKING FIX: Log exceptions instead of silently swallowing them
                // While Dispose should not throw, logging helps diagnose connection state issues
                try
                {
                    Transaction.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // Expected - transaction already disposed, safe to ignore
                }
                catch (InvalidOperationException)
                {
                    // Expected - transaction in invalid state for disposal, safe to ignore
                }
                catch (Exception ex)
                {
                    // Unexpected exception during dispose - log for diagnostics but don't throw
                    // per .NET guidelines (Dispose should not throw)
                    Debug.WriteLine($"Warning: Exception disposing transaction: {ex.GetType().Name}: {ex.Message}");
                }
            }
            _cts?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
