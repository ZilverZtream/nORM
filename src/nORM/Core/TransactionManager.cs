using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Configuration;

namespace nORM.Core
{
    internal sealed class TransactionManager : IAsyncDisposable, IDisposable
    {
        private readonly CancellationTokenSource? _cts;
        private readonly ILogger? _logger;

        public DbTransaction? Transaction { get; }
        public CancellationToken Token { get; }
        public bool OwnsTransaction { get; }

        /// <summary>
        /// X1 fix: true when the writes performed within this manager have committed
        /// independently and the change tracker should be advanced to Unchanged.
        /// This is true when:
        ///   — we own the transaction (commit was ours),
        ///   — ambient policy is Ignore (enlistment intentionally skipped), or
        ///   — ambient policy is BestEffort but enlistment failed (writes committed outside scope).
        /// It is false when an external or successfully-enlisted ambient transaction controls
        /// durability (the caller decides whether to commit or roll back).
        /// </summary>
        public bool ShouldAcceptChanges { get; }

        private TransactionManager(DbTransaction? transaction, bool ownsTransaction, bool shouldAcceptChanges,
            CancellationTokenSource? cts, CancellationToken token, ILogger? logger)
        {
            Transaction = transaction;
            OwnsTransaction = ownsTransaction;
            ShouldAcceptChanges = shouldAcceptChanges;
            _cts = cts;
            Token = token;
            _logger = logger;
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
            // X1 fix: track whether DB writes committed independently so SaveChanges can
            // decide whether to call AcceptChanges on the change tracker.
            bool shouldAcceptChanges = ownsTransaction; // default: true only when we own the tx

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
                // When an ambient System.Transactions.Transaction exists but no explicit DbTransaction
                // has been started, attempt to enlist the connection so that providers are included
                // in the TransactionScope and roll back correctly if the scope is disposed without
                // Complete(). The enlistment failure policy is configurable via AmbientTransactionPolicy.
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var connection = context.Connection;
                var policy = context.Options.AmbientTransactionPolicy;

                if (policy == AmbientTransactionEnlistmentPolicy.Ignore)
                {
                    // X1 fix: Explicitly de-enlist from any ambient transaction that the
                    // ADO.NET driver may have auto-enlisted into when the connection was
                    // opened (SqlClient, Microsoft.Data.Sqlite with Enlist=true in the
                    // connection string both auto-enlist on Open() if Transaction.Current
                    // is set at that moment). EnlistTransaction(null) detaches the connection
                    // so subsequent writes commit independently rather than being rolled back
                    // when the ambient scope is abandoned without Complete().
                    bool deEnlistFailed = false;
                    if (connection != null && connection.State == System.Data.ConnectionState.Open)
                    {
                        try
                        {
                            connection.EnlistTransaction(null);
                            context.Options.Logger?.LogDebug(
                                "De-enlisted connection from ambient transaction for Ignore policy.");
                        }
                        catch (NotSupportedException deEx)
                        {
                            // Provider does not support EnlistTransaction at all, which also means
                            // it did not auto-enlist on Open(). Writes commit independently.
                            context.Options.Logger?.LogDebug(
                                "Provider does not support EnlistTransaction " +
                                "(Ignore policy): {Message}. No auto-enlistment occurred.", deEx.Message);
                        }
                        catch (InvalidOperationException deEx)
                        {
                            // Provider threw InvalidOperationException — typically means the
                            // operation is not supported in the current state. Same reasoning
                            // as NotSupportedException: no auto-enlistment happened.
                            context.Options.Logger?.LogDebug(
                                "EnlistTransaction(null) not valid in current state " +
                                "(Ignore policy): {Message}. Treating as no auto-enlistment.", deEx.Message);
                        }
                        catch (Exception deEx)
                        {
                            // T1 fix: check if there's still an active ambient scope. If so,
                            // the connection may still be enlisted and writes could be rolled
                            // back — do NOT advance the tracker. If no ambient scope exists
                            // (e.g., SQLite which never auto-enlists), the failure is harmless.
                            if (System.Transactions.Transaction.Current != null)
                            {
                                deEnlistFailed = true;
                                context.Options.Logger?.LogWarning(
                                    "Failed to de-enlist connection from ambient transaction " +
                                    "(Ignore policy): {Message}. Tracker will NOT advance (T1).", deEx.Message);
                            }
                            else
                            {
                                context.Options.Logger?.LogDebug(
                                    "EnlistTransaction(null) failed but no ambient scope active " +
                                    "(Ignore policy): {Message}. Writes commit independently.", deEx.Message);
                            }
                        }
                    }
                    // T1 fix: advance the tracker unless de-enlistment failed while an ambient
                    // scope was active, indicating the connection may still be scope-bound.
                    shouldAcceptChanges = !deEnlistFailed;
                }
                else if (connection != null && connection.State == System.Data.ConnectionState.Open)
                {
                    try
                    {
                        connection.EnlistTransaction(System.Transactions.Transaction.Current);
                        context.Options.Logger?.LogDebug(
                            "Explicitly enlisted connection in ambient transaction {TransactionId}.",
                            ambientTransaction.TransactionInformation.LocalIdentifier);
                        // Enlistment succeeded — caller's scope controls durability; do NOT accept.
                        shouldAcceptChanges = false;
                    }
                    catch (Exception ex)
                    {
                        if (policy == AmbientTransactionEnlistmentPolicy.FailFast)
                        {
                            // Gate E: Surface enlistment failures so the caller knows their
                            // operations are NOT participating in the ambient scope.
                            throw new NormConfigurationException(
                                "Provider does not support ambient transaction enlistment. " +
                                "Set AmbientTransactionPolicy = BestEffort to suppress this error " +
                                "or AmbientTransactionPolicy = Ignore to skip enlistment entirely. " +
                                $"Provider error: {ex.Message}", ex);
                        }

                        // BestEffort: enlistment failed — log warning and continue.
                        // Writes will commit independently of the scope.
                        // X1 fix: accept tracker state because the DB commits without the scope.
                        shouldAcceptChanges = true;
                        context.Options.Logger?.LogWarning(
                            "Could not enlist connection in ambient transaction {TransactionId}: {Message}. " +
                            "Operations may commit independently of the TransactionScope.",
                            ambientTransaction.TransactionInformation.LocalIdentifier,
                            ex.Message);
                    }
                }
            }
            // else: existingTransaction != null — external explicit transaction controls durability.
            // shouldAcceptChanges remains false; caller commits/rolls back.

            return new TransactionManager(transaction, ownsTransaction, shouldAcceptChanges, cts, token, context.Options.Logger);
        }

        /// <summary>
        /// Commits the underlying transaction if this instance owns it.
        /// </summary>
        /// <returns>A task that completes once the commit operation has finished.</returns>
        public async ValueTask CommitAsync()
        {
            if (OwnsTransaction && Transaction != null)
                // Use CancellationToken.None — commit must not be aborted mid-flight.
                // A cancelled commit leaves ambiguous DB state; always run to completion.
                await Transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
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
                // Use CancellationToken.None — rollback must complete regardless of caller cancellation.
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
                    // Unexpected exception during dispose — log for diagnostics but don't
                    // throw per .NET Dispose contract. Connection pool may be in indeterminate state.
                    _logger?.LogWarning(ex,
                        "Unexpected exception disposing database transaction. " +
                        "Connection pool may be in an indeterminate state.");
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
                    // Unexpected exception during dispose — log for diagnostics but don't
                    // throw per .NET Dispose contract. Connection pool may be in indeterminate state.
                    _logger?.LogWarning(ex,
                        "Unexpected exception disposing database transaction. " +
                        "Connection pool may be in an indeterminate state.");
                }
            }
            _cts?.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
