using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Mapping;

namespace nORM.Providers
{
    /// <summary>
    /// Provides helpers for provider specific bulk operations by extracting the
    /// common batching and transactional patterns used by multiple providers.
    /// </summary>
    public abstract class BulkOperationProvider : DatabaseProvider
    {
        /// <summary>
        /// Executes a bulk operation in batches inside a single transaction. The
        /// concrete provider supplies the per batch action which performs the
        /// actual database work.
        /// </summary>
        protected async Task<int> ExecuteBulkOperationAsync<T>(
            DbContext ctx,
            TableMapping mapping,
            IList<T> entityList,
            string operationKey,
            Func<List<T>, DbTransaction, CancellationToken, Task<int>> batchAction,
            CancellationToken ct) where T : class
        {
            var sizing = BatchSizer.CalculateOptimalBatchSize(
                entityList.Take(BatchSizingSampleCount), mapping, operationKey, entityList.Count);

            var total = 0;
            // Reuse caller's transaction when one is already open on this context.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction? transaction = ownedTx
                ? await ctx.Connection.BeginTransactionAsync(ct).ConfigureAwait(false)
                : ctx.CurrentTransaction;
            try
            {
                for (int i = 0; i < entityList.Count; i += sizing.OptimalBatchSize)
                {
                    var batchCount = Math.Min(sizing.OptimalBatchSize, entityList.Count - i);
                    List<T> batch;

                    if (entityList is List<T> list)
                    {
                        batch = list.GetRange(i, batchCount);
                    }
                    else
                    {
                        batch = new List<T>(batchCount);
                        for (var j = 0; j < batchCount; j++)
                        {
                            batch.Add(entityList[i + j]);
                        }
                    }
                    var batchSw = Stopwatch.StartNew();
                    total += await batchAction(batch, transaction!, ct).ConfigureAwait(false);
                    batchSw.Stop();
                    BatchSizer.RecordBatchPerformance(
                        operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                }

                // Use CancellationToken.None so a cancelled caller token after a successful commit
                // does not cause a spurious OperationCanceledException for already-committed data.
                if (ownedTx) await transaction!.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ownedTx)
                {
                    try
                    {
                        // Use CancellationToken.None so that a cancelled caller token does not
                        // also cancel the rollback, leaving the transaction in an uncertain state.
                        await transaction!.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(ex, rollbackEx);
                    }
                }
                throw;
            }
            finally
            {
                if (ownedTx && transaction != null)
                    await transaction.DisposeAsync().ConfigureAwait(false);
            }

            return total;
        }
    }
}

