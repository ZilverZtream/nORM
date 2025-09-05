using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace nORM.Core
{
    internal sealed class TransactionManager : IAsyncDisposable, IDisposable
    {
        private readonly CancellationTokenSource? _timeoutCts;
        private readonly CancellationTokenSource? _linkedCts;

        public DbTransaction? Transaction { get; }
        public CancellationToken Token { get; }
        public bool OwnsTransaction { get; }

        private TransactionManager(DbTransaction? transaction, bool ownsTransaction,
            CancellationTokenSource? timeoutCts, CancellationTokenSource? linkedCts, CancellationToken token)
        {
            Transaction = transaction;
            OwnsTransaction = ownsTransaction;
            _timeoutCts = timeoutCts;
            _linkedCts = linkedCts;
            Token = token;
        }

        public static async Task<TransactionManager> CreateAsync(DbContext context, CancellationToken ct)
        {
            var existingTransaction = context.Database.CurrentTransaction;
            var ambientTransaction = System.Transactions.Transaction.Current;
            var ownsTransaction = existingTransaction == null && ambientTransaction == null;

            DbTransaction? transaction = null;
            CancellationTokenSource? timeoutCts = null;
            CancellationTokenSource? linkedCts = null;
            var token = ct;

            if (ambientTransaction != null && existingTransaction == null)
            {
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                context.Connection.EnlistTransaction(ambientTransaction);
            }
            else if (ownsTransaction)
            {
                await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
                var isolationLevel = System.Data.IsolationLevel.ReadCommitted;
                transaction = await context.Connection.BeginTransactionAsync(isolationLevel, ct).ConfigureAwait(false);

                timeoutCts = new CancellationTokenSource(context.Options.TimeoutConfiguration.BaseTimeout);
                linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
                token = linkedCts.Token;
            }
            else
            {
                transaction = existingTransaction!;
            }

            return new TransactionManager(transaction, ownsTransaction, timeoutCts, linkedCts, token);
        }

        public async ValueTask CommitAsync()
        {
            if (OwnsTransaction && Transaction != null)
            {
                await Transaction.CommitAsync(Token).ConfigureAwait(false);
            }
        }

        public async ValueTask RollbackAsync()
        {
            if (OwnsTransaction && Transaction != null)
            {
                await Transaction.RollbackAsync(Token).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (OwnsTransaction && Transaction != null)
            {
                await Transaction.DisposeAsync().ConfigureAwait(false);
            }
            _linkedCts?.Dispose();
            _timeoutCts?.Dispose();
        }

        public void Dispose()
        {
            if (OwnsTransaction && Transaction != null)
            {
                Transaction.Dispose();
            }
            _linkedCts?.Dispose();
            _timeoutCts?.Dispose();
        }
    }
}
