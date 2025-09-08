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

        public static async Task<TransactionManager> CreateAsync(DbContext context, CancellationToken ct)
        {
            var existingTransaction = context.Database.CurrentTransaction;
            var ambientTransaction = System.Transactions.Transaction.Current;
            var ownsTransaction = existingTransaction == null && ambientTransaction == null;

            DbTransaction? transaction = null;
            CancellationTokenSource? cts = null;
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

                cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(context.Options.TimeoutConfiguration.BaseTimeout);
                token = cts.Token;
            }
            else
            {
                transaction = existingTransaction!;
            }

            return new TransactionManager(transaction, ownsTransaction, cts, token);
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
            _cts?.Dispose();
            GC.SuppressFinalize(this);
        }

        public void Dispose()
        {
            if (OwnsTransaction && Transaction != null)
            {
                Transaction.Dispose();
            }
            _cts?.Dispose();
            GC.SuppressFinalize(this);
        }

        ~TransactionManager()
        {
            if (OwnsTransaction && Transaction != null)
            {
                Transaction.Dispose();
            }
            _cts?.Dispose();
        }
    }
}
