using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Configuration;

#nullable enable

namespace nORM.Providers
{
    public abstract partial class DatabaseProvider
    {
        #region Bulk Operations (Abstract & Fallback)
        /// <summary>
        /// Inserts a large collection of entities into the database in batches.
        /// The method dynamically tunes batch size using <see cref="DynamicBatchSizer"/>
        /// to balance throughput with resource consumption and transaction log pressure.
        /// </summary>
        /// <typeparam name="T">Type of entities being inserted.</typeparam>
        /// <param name="ctx">The active <see cref="DbContext"/> that supplies the connection and options.</param>
        /// <param name="m">Mapping metadata describing how the entity maps to the database table.</param>
        /// <param name="entities">The entity instances to persist.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The total number of rows inserted across all batches.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public virtual async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0)
            {
                ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, 0, sw.Elapsed);
                return 0;
            }

            var operationKey = $"BulkInsert_{m.Type.Name}";
            var sizing = BatchSizer.CalculateOptimalBatchSize(entityList.Take(BatchSizingSampleCount), m, operationKey, entityList.Count);
            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            var maxBatchForProvider = MaxParameters == int.MaxValue
                ? 1000
                : Math.Max(1, Math.Min(1000, (MaxParameters - 10) / Math.Max(1, cols.Count)));
            var effectiveBatchSize = Math.Max(1, Math.Min(sizing.OptimalBatchSize, maxBatchForProvider));
            // Logging infrastructure doesn't support arbitrary info; batch size can be inferred from performance metrics.

            var recordsAffected = 0;
            var index = 0;
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                while (index < entityList.Count)
                {
                    var availableMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
                    if (availableMemory < sizing.EstimatedMemoryUsage * 2)
                        effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);

                    if (await IsTransactionLogNearCapacityAsync(ctx, ct).ConfigureAwait(false))
                        effectiveBatchSize = Math.Max(1, effectiveBatchSize / 2);

                    var batch = entityList.GetRange(index, Math.Min(effectiveBatchSize, entityList.Count - index));
                    var batchSw = Stopwatch.StartNew();
                    recordsAffected += await ExecuteInsertBatch(ctx, m, batch, ct, transaction).ConfigureAwait(false);
                    batchSw.Stop();
                    BatchSizer.RecordBatchPerformance(operationKey, batch.Count, batchSw.Elapsed, batch.Count);
                    index += batch.Count;
                }

                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkInsert failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw;
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, recordsAffected, sw.Elapsed);
            return recordsAffected;
        }

        /// <summary>
        /// Executes a single batch insert for the supplied entities.
        /// </summary>
        /// <typeparam name="T">Type of entity being inserted.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/> providing the database connection.</param>
        /// <param name="m">Table mapping used to generate the insert statement.</param>
        /// <param name="batch">Entities to insert in a single round-trip.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <param name="transaction">Transaction that should contain the batch, or <c>null</c> to use the context's active transaction.</param>
        /// <returns>The number of rows affected by the batch.</returns>
        protected async Task<int> ExecuteInsertBatch<T>(DbContext ctx, TableMapping m, List<T> batch, CancellationToken ct, DbTransaction? transaction = null) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToList();
            // All columns are DB-generated - use DEFAULT VALUES per row (no batching possible).
            if (cols.Count == 0)
            {
                var inserted = 0;
                await using var cmd = ctx.CreateCommand();
                if (transaction != null) cmd.Transaction = transaction;
                cmd.CommandText = $"INSERT INTO {m.EscTable} {DefaultValuesInsertClause}";
                for (int i = 0; i < batch.Count; i++)
                    inserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                return inserted;
            }
            var sb = _stringBuilderPool.Get();
            try
            {
                var colNames = string.Join(", ", cols.Select(c => c.EscCol));
                sb.Append($"INSERT INTO {m.EscTable} ({colNames}) VALUES ");

                await using var cmd = ctx.CreateCommand();
                if (transaction != null) cmd.Transaction = transaction;
                var pIndex = 0;
                for (int i = 0; i < batch.Count; i++)
                {
                    sb.Append(i > 0 ? ",(" : "(");
                    for (int j = 0; j < cols.Count; j++)
                    {
                        var pName = $"{ParamPrefix}p{pIndex++}";
                        // Apply the value converter like every non-bulk write path; binding the raw model
                        // value would skip the converter and silently corrupt the column on read-back.
                        var raw = cols[j].Getter(batch[i]);
                        var conv = cols[j].Converter;
                        cmd.AddParam(pName, conv != null ? conv.ConvertToProvider(raw) : raw);
                        sb.Append(j > 0 ? $",{pName}" : pName);
                    }
                    sb.Append(")");
                }
                cmd.CommandText = sb.ToString();
                return await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Performs a bulk update across the provided entities. Providers without a
        /// native implementation fall back to batched updates when enabled.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public virtual Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedUpdateAsync(ctx, m, e, ct);
            throw new NormUnsupportedFeatureException(
                $"Bulk update is not supported by provider {Capabilities.ProviderName}. " +
                "Use the standard SaveChanges path instead, or set DbContextOptions.UseBatchedBulkOps = true.");
        }

        /// <summary>
        /// Performs a bulk delete across the provided entities. Providers without a
        /// native implementation fall back to batched deletes when enabled.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public virtual Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> e, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            if (ctx.Options.UseBatchedBulkOps)
                return BatchedDeleteAsync(ctx, m, e, ct);
            throw new NormUnsupportedFeatureException(
                $"Bulk delete is not supported by provider {Capabilities.ProviderName}. " +
                "Use the standard SaveChanges path instead, or set DbContextOptions.UseBatchedBulkOps = true.");
        }

        /// <summary>
        /// Executes update statements in batches, selecting the batch size dynamically
        /// to balance performance and resource usage.
        /// </summary>
        protected async Task<int> BatchedUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var totalUpdated = 0;
            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                foreach (var entity in entities)
                {
                    await using var cmd = ctx.CreateCommand();
                    cmd.Transaction = transaction;
                    var batchHasTenant = ctx.Options.TenantProvider != null;
                    if (batchHasTenant)
                        ctx.RequireTenantColumn(m, "bulk update fallback");
                    cmd.CommandText = BuildUpdate(m, batchHasTenant);
                    foreach (var col in m.Columns.Where(c => !c.IsTimestamp && !(batchHasTenant && ReferenceEquals(c, m.TenantColumn))))
                        cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                    if (m.TimestampColumn != null) cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                    // X1: bind tenant param to match the WHERE predicate added when batchHasTenant is true.
                    if (batchHasTenant) cmd.AddParam(ParamPrefix + m.TenantColumn!.PropName, ctx.GetRequiredTenantId(m, "bulk update fallback"));
                    totalUpdated += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                // Use CancellationToken.None so a cancelled caller token after a successful commit
                // does not cause a spurious OperationCanceledException for already-committed data.
                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); // Use None so cancelled caller token does not abort rollback
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkUpdate failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable - satisfies compiler
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }
            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        /// <summary>
        /// Executes delete statements in batches, selecting the batch size dynamically
        /// to balance performance and resource usage.
        /// </summary>
        protected async Task<int> BatchedDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            if (m.KeyColumns.Length == 0)
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var totalDeleted = 0;
            var keyColumns = m.KeyColumns.ToList();

            // Determine maximum entities per batch based on provider parameter limit
            var batchSize = Math.Min(ctx.Options.BulkBatchSize, 1000);
            if (MaxParameters != int.MaxValue)
            {
                // Reserve one extra parameter per row for the concurrency token when present.
                var paramsPerEntity = Math.Max(1, keyColumns.Count + (m.TimestampColumn != null ? 1 : 0));
                var maxBatchByParams = Math.Max(1, (MaxParameters - 10) / paramsPerEntity);
                batchSize = Math.Min(batchSize, maxBatchByParams);
            }

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                for (int i = 0; i < entityList.Count; i += batchSize)
                {
                    var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                    await using var cmd = ctx.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                    var paramNames = new List<string>();
                    var paramIndex = 0;

                    string whereClause;
                    var timestampCol = m.TimestampColumn;

                    if (timestampCol == null && keyColumns.Count == 1)
                    {
                        // Fast IN path: no concurrency token to match.
                        var keyCol = keyColumns[0];
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var pName = $"{ParamPrefix}p{paramIndex++}";
                            paramNames.Add(pName);
                            cmd.AddParam(pName, keyCol.Getter(batch[j]));
                        }
                        whereClause = $"{keyCol.EscCol} IN ({string.Join(",", paramNames)})";
                    }
                    else
                    {
                        // Per-row (keys [AND token]) OR-conditions. Including the
                        // concurrency token means a row whose token another writer has
                        // bumped is skipped, not destroyed — matching the bulk-update
                        // contract (stale rows skipped, reduced count returned).
                        var orConditions = new List<string>();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var conds = new List<string>();
                            foreach (var c in keyColumns)
                            {
                                var pName = $"{ParamPrefix}p{paramIndex++}";
                                cmd.AddParam(pName, c.Getter(batch[j]));
                                conds.Add($"{c.EscCol} = {pName}");
                            }
                            if (timestampCol != null)
                            {
                                var tpName = $"{ParamPrefix}p{paramIndex++}";
                                cmd.AddParam(tpName, timestampCol.Getter(batch[j]));
                                conds.Add($"({timestampCol.EscCol} = {tpName} OR ({timestampCol.EscCol} IS NULL AND {tpName} IS NULL))");
                            }
                            orConditions.Add($"({string.Join(" AND ", conds)})");
                        }
                        whereClause = string.Join(" OR ", orConditions);
                    }

                    if (ctx.Options.TenantProvider != null)
                    {
                        var tenantCol = ctx.RequireTenantColumn(m, "bulk delete fallback");
                        var tenantParam = $"{ParamPrefix}__tenant_bulk";
                        cmd.AddParam(tenantParam, ctx.GetRequiredTenantId(m, "bulk delete fallback"));
                        whereClause = $"({whereClause}) AND {tenantCol.EscCol} = {tenantParam}";
                    }

                    cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {whereClause}";
                    var deleted = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    totalDeleted += deleted;
                }

                // Use CancellationToken.None so a cancelled caller token after a successful commit
                // does not cause a spurious OperationCanceledException for already-committed data.
                if (ownedTx) await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception originalEx)
            {
                // Preserve the original exception if rollback itself fails.
                if (ownedTx)
                {
                    try
                    {
                        await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); // Use None so cancelled caller token does not abort rollback
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(
                            "BulkDelete failed and rollback also failed. See inner exceptions for details.",
                            originalEx, rollbackEx);
                    }
                }
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalEx).Throw();
                throw; // unreachable - satisfies compiler
            }
            finally
            {
                if (ownedTx) await transaction.DisposeAsync().ConfigureAwait(false);
            }

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName);
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
        #endregion
    }
}
