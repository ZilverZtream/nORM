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
        private volatile bool _disposed;

        public DbTransaction? Transaction { get; }
        public CancellationToken Token { get; }
        public bool OwnsTransaction { get; }

        /// <summary>
        /// Indicates whether the change tracker should advance entity states to Unchanged
        /// after a successful write operation.
        /// <para>True when:</para>
        /// <list type="bullet">
        ///   <item>This manager owns the transaction (commit was ours).</item>
        ///   <item>Ambient policy is <see cref="AmbientTransactionEnlistmentPolicy.Ignore"/>
        ///         and de-enlistment succeeded or no ambient scope was active.</item>
        ///   <item>Ambient policy is <see cref="AmbientTransactionEnlistmentPolicy.BestEffort"/>
        ///         but enlistment failed (writes committed outside scope).</item>
        /// </list>
        /// <para>False when an external or successfully-enlisted ambient transaction controls
        /// durability (the caller decides whether to commit or roll back).</para>
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
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="context"/> is null.</exception>
        public static async Task<TransactionManager> CreateAsync(DbContext context, CancellationToken ct)
        {
            ArgumentNullException.ThrowIfNull(context);

            var existingTransaction = context.Database.CurrentTransaction;
            var ambientTransaction = System.Transactions.Transaction.Current;
            var ownsTransaction = existingTransaction == null && ambientTransaction == null;

            DbTransaction? transaction = existingTransaction;
            CancellationTokenSource? cts = null;
            var token = ct;
            // Track whether DB writes committed independently so SaveChanges can
            // decide whether to call AcceptChanges on the change tracker.
            bool shouldAcceptChanges = ownsTransaction; // default: true only when we own the tx

            if (ownsTransaction)
            {
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var connection = context.Connection ?? throw new InvalidOperationException("Database connection is not initialized.");
                transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

                // Create a linked CTS that cancels if the caller's token fires.
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
                    shouldAcceptChanges = HandleIgnorePolicy(context, connection);
                }
                else if (connection != null && connection.State == ConnectionState.Open)
                {
                    shouldAcceptChanges = HandleEnlistmentPolicy(context, connection, ambientTransaction, policy);
                }
            }
            // else: existingTransaction != null -- external explicit transaction controls durability.
            // shouldAcceptChanges remains false; caller commits/rolls back.

            return new TransactionManager(transaction, ownsTransaction, shouldAcceptChanges, cts, token, context.Options.Logger);
        }

        /// <summary>
        /// Handles the <see cref="AmbientTransactionEnlistmentPolicy.Ignore"/> path:
        /// attempts to de-enlist the connection from any auto-enlisted ambient transaction.
        /// </summary>
        /// <returns>True if the change tracker should accept changes; false if the connection
        /// may still be scope-bound.</returns>
        private static bool HandleIgnorePolicy(DbContext context, DbConnection? connection)
        {
            // Explicitly de-enlist from any ambient transaction that the ADO.NET driver
            // may have auto-enlisted into when the connection was opened (SqlClient,
            // Microsoft.Data.Sqlite with Enlist=true both auto-enlist on Open() if
            // Transaction.Current is set). EnlistTransaction(null) detaches the connection
            // so subsequent writes commit independently rather than being rolled back
            // when the ambient scope is abandoned without Complete().
            bool deEnlistFailed = false;
            if (connection != null && connection.State == ConnectionState.Open)
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
                    // Provider threw InvalidOperationException -- typically means the
                    // operation is not supported in the current state. Same reasoning
                    // as NotSupportedException: no auto-enlistment happened.
                    context.Options.Logger?.LogDebug(
                        "EnlistTransaction(null) not valid in current state " +
                        "(Ignore policy): {Message}. Treating as no auto-enlistment.", deEx.Message);
                }
                catch (OperationCanceledException)
                {
                    // Propagate cancellation rather than swallowing it.
                    throw;
                }
                catch (DbException deEx)
                {
                    // Provider-level error during de-enlistment. Check if there's still an
                    // active ambient scope. If so, the connection may still be enlisted and
                    // writes could be rolled back -- do NOT advance the tracker. If no ambient
                    // scope exists (e.g., SQLite which never auto-enlists), the failure is harmless.
                    deEnlistFailed = HandleDeEnlistFailure(context, deEx);
                }
                catch (Exception deEx) when (deEx is not OutOfMemoryException and not StackOverflowException)
                {
                    // Non-provider, non-critical exception during de-enlistment. Apply the
                    // same ambient-scope check as the DbException path above.
                    deEnlistFailed = HandleDeEnlistFailure(context, deEx);
                }
            }
            // Advance the tracker unless de-enlistment failed while an ambient
            // scope was active, indicating the connection may still be scope-bound.
            return !deEnlistFailed;
        }

        /// <summary>
        /// Evaluates whether a de-enlistment failure is significant by checking if an ambient
        /// <see cref="System.Transactions.Transaction"/> is still active.
        /// </summary>
        /// <returns>True if the failure is significant (ambient scope still active); false otherwise.</returns>
        private static bool HandleDeEnlistFailure(DbContext context, Exception deEx)
        {
            if (System.Transactions.Transaction.Current != null)
            {
                context.Options.Logger?.LogWarning(
                    "Failed to de-enlist connection from ambient transaction " +
                    "(Ignore policy): {Message}. Tracker will NOT advance.", deEx.Message);
                return true;
            }

            context.Options.Logger?.LogDebug(
                "EnlistTransaction(null) failed but no ambient scope active " +
                "(Ignore policy): {Message}. Writes commit independently.", deEx.Message);
            return false;
        }

        /// <summary>
        /// Handles the <see cref="AmbientTransactionEnlistmentPolicy.FailFast"/> and
        /// <see cref="AmbientTransactionEnlistmentPolicy.BestEffort"/> enlistment paths.
        /// </summary>
        /// <returns>True if the change tracker should accept changes; false if an ambient
        /// transaction now controls durability.</returns>
        /// <exception cref="NormConfigurationException">Thrown under <see cref="AmbientTransactionEnlistmentPolicy.FailFast"/>
        /// when the provider cannot enlist in the ambient transaction.</exception>
        private static bool HandleEnlistmentPolicy(DbContext context, DbConnection connection,
            System.Transactions.Transaction ambientTransaction, AmbientTransactionEnlistmentPolicy policy)
        {
            try
            {
                connection.EnlistTransaction(System.Transactions.Transaction.Current);
                context.Options.Logger?.LogDebug(
                    "Explicitly enlisted connection in ambient transaction {TransactionId}.",
                    ambientTransaction.TransactionInformation.LocalIdentifier);
                // Enlistment succeeded -- caller's scope controls durability; do NOT accept.
                return false;
            }
            catch (OperationCanceledException)
            {
                // Propagate cancellation rather than wrapping it in NormConfigurationException.
                throw;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                if (policy == AmbientTransactionEnlistmentPolicy.FailFast)
                {
                    // Surface enlistment failures so the caller knows their operations are
                    // NOT participating in the ambient scope.
                    throw new NormConfigurationException(
                        "Provider does not support ambient transaction enlistment. " +
                        "Set AmbientTransactionPolicy = BestEffort to suppress this error " +
                        "or AmbientTransactionPolicy = Ignore to skip enlistment entirely. " +
                        $"Provider error: {ex.Message}", ex);
                }

                // BestEffort: enlistment failed -- log warning and continue.
                // Writes will commit independently of the scope; accept tracker state.
                context.Options.Logger?.LogWarning(
                    "Could not enlist connection in ambient transaction {TransactionId}: {Message}. " +
                    "Operations may commit independently of the TransactionScope.",
                    ambientTransaction.TransactionInformation.LocalIdentifier,
                    ex.Message);
                return true;
            }
        }

        /// <summary>
        /// Commits the underlying transaction if this instance owns it.
        /// Always uses <see cref="CancellationToken.None"/> so that a cancelled caller token
        /// cannot abort the commit mid-flight and leave ambiguous database state.
        /// </summary>
        /// <returns>A task that completes once the commit operation has finished.</returns>
        public async ValueTask CommitAsync()
        {
            if (OwnsTransaction && Transaction != null)
                await Transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }

        /// <summary>
        /// Rolls back the underlying transaction if owned by this instance.
        /// Always uses <see cref="CancellationToken.None"/> so that a cancelled caller token
        /// cannot abort the rollback and leave an incomplete transaction state.
        /// </summary>
        /// <param name="_">Ignored -- rollback always uses <see cref="CancellationToken.None"/>.</param>
        /// <returns>A task that completes when the rollback has finished.</returns>
        public async ValueTask RollbackAsync(CancellationToken _ = default)
        {
            if (OwnsTransaction && Transaction != null)
                await Transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously disposes the transaction and linked cancellation source.
        /// Idempotent: subsequent calls after the first are no-ops.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;

            if (OwnsTransaction && Transaction != null)
            {
                try
                {
                    await Transaction.DisposeAsync().ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                    // Transaction already disposed via explicit commit/rollback path.
                }
                catch (InvalidOperationException)
                {
                    // Transaction in invalid state for disposal (e.g., already rolled back).
                }
                catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
                {
                    // Unexpected exception during dispose -- log for diagnostics but do not
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
        /// Synchronously releases the underlying transaction and cancellation source.
        /// Idempotent: subsequent calls after the first are no-ops.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;

            if (OwnsTransaction && Transaction != null)
            {
                try
                {
                    Transaction.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // Transaction already disposed via explicit commit/rollback path.
                }
                catch (InvalidOperationException)
                {
                    // Transaction in invalid state for disposal (e.g., already rolled back).
                }
                catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
                {
                    // Unexpected exception during dispose -- log for diagnostics but do not
                    // throw per .NET Dispose contract. Connection pool may be in an indeterminate state.
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
