using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {        /// <summary>
        /// Inserts a collection of entities using SQLite-optimized prepared statements in a single transaction.
        /// Uses prepared statement reuse and transaction batching; significantly faster than multiple-transaction approaches.
        /// </summary>
        /// <typeparam name="T">Type of entity being inserted.</typeparam>
        /// <param name="ctx">Current <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping for the destination table.</param>
        /// <param name="entities">Entities to insert.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows inserted.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkInsertAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var entityList = entities as ICollection<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;
            var sw = ctx.Options.Logger != null ? Stopwatch.StartNew() : null;

            var cols = m.Columns.Where(c => !c.IsDbGenerated).ToArray();

            var totalInserted = 0;

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                if (cols.Length == 0)
                {
                    // All columns are DB-generated - use DEFAULT VALUES syntax.
                    // DEFAULT VALUES does not support batching so we loop per entity.
                    await using var cmd = ctx.RawConnection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"INSERT INTO {m.EscTable} DEFAULT VALUES";
                    foreach (var _ in entityList)
                        totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }
                else
                {
                    // 2. Create ONE command and ONE set of parameters that will be reused.
                    await using var cmd = ctx.RawConnection.CreateCommand();
                    cmd.Transaction = transaction;
                    // Use INSERT without RETURNING - bulk path uses ExecuteNonQuery
                    // and doesn't hydrate generated keys, so RETURNING output is wasted work.
                    cmd.CommandText = BuildInsert(m, hydrateGeneratedKeys: false);

                    // Create parameter objects ONCE and add them to the command.
                    var parameters = new DbParameter[cols.Length];
                    for (int i = 0; i < cols.Length; i++)
                    {
                        var p = cmd.CreateParameter();
                        p.ParameterName = $"{ParamPrefix}{cols[i].PropName}";
                        cmd.Parameters.Add(p);
                        parameters[i] = p;
                    }

                    // 3. Prepare the command ONCE before the loop.
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    // 4. Loop through each entity and execute the prepared command.
                    foreach (var entity in entityList)
                    {
                        // 5. Simply update the values of the existing parameters. No new objects created.
                        for (int i = 0; i < cols.Length; i++)
                        {
                            parameters[i].Value = cols[i].Getter(entity) ?? DBNull.Value;
                        }

                        totalInserted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
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
                            "BulkInsert failed and rollback also failed. See inner exceptions for details.",
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

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkInsertAsync), m.EscTable, totalInserted, sw?.Elapsed ?? default);
            return totalInserted;
        }

        /// <summary>
        /// Updates multiple entities using a temp table approach for efficient bulk updates.
        /// Uses temp tables and UPDATE FROM pattern; significantly faster than the base class batched operations.
        /// </summary>
        /// <typeparam name="T">Type of entity being updated.</typeparam>
        /// <param name="ctx">Active <see cref="DbContext"/>.</param>
        /// <param name="m">Mapping metadata for the entity's table.</param>
        /// <param name="entities">Entities containing new values.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of rows updated.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            var tempTableName = $"\"BulkUpdate_{Guid.NewGuid():N}\"";
            var nonKeyCols = m.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToList();
            var keyCols = m.KeyColumns.ToList();

            var totalUpdated = 0;

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                // Create temp table with same schema
                var colDefs = string.Join(", ", m.Columns.Select(c => $"{Escape(c.PropName)} {GetSqliteType(c.Prop.PropertyType)}"));
                await using (var cmd = ctx.RawConnection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"CREATE TEMP TABLE {tempTableName} ({colDefs})";
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                // Insert entities into temp table using prepared statement
                await using (var cmd = ctx.RawConnection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    var insertCols = m.Columns.ToArray();
                    var paramPlaceholders = string.Join(", ", insertCols.Select(c => ParamPrefix + c.PropName));
                    var colNames = string.Join(", ", insertCols.Select(c => Escape(c.PropName)));
                    cmd.CommandText = $"INSERT INTO {tempTableName} ({colNames}) VALUES ({paramPlaceholders})";

                    var parameters = new DbParameter[insertCols.Length];
                    for (int i = 0; i < insertCols.Length; i++)
                    {
                        var p = cmd.CreateParameter();
                        p.ParameterName = ParamPrefix + insertCols[i].PropName;
                        cmd.Parameters.Add(p);
                        parameters[i] = p;
                    }

                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    foreach (var entity in entityList)
                    {
                        for (int i = 0; i < insertCols.Length; i++)
                        {
                            parameters[i].Value = insertCols[i].Getter(entity) ?? DBNull.Value;
                        }
                        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }
                }

                // Perform bulk update using temp table join
                await using (var cmd = ctx.RawConnection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    var keyMatchConditions = string.Join(" AND ", keyCols.Select(k => $"{tempTableName}.{Escape(k.PropName)} = {m.EscTable}.{k.EscCol}"));
                    // X3: Include timestamp in EXISTS match to enforce OCC
                    // Use IS (null-safe equality) instead of = because NULL = NULL is NULL (falsy) in SQL.
                    // SQLite's IS operator treats NULL IS NULL as TRUE, matching entities with null tokens.
                    var tsCondition = m.TimestampColumn != null
                        ? $" AND {tempTableName}.{Escape(m.TimestampColumn.PropName)} IS {m.EscTable}.{m.TimestampColumn.EscCol}"
                        : "";
                    var setClause = string.Join(", ", nonKeyCols.Select(c => $"{c.EscCol} = (SELECT {Escape(c.PropName)} FROM {tempTableName} WHERE {keyMatchConditions})"));
                    var whereClause = $"EXISTS (SELECT 1 FROM {tempTableName} WHERE {keyMatchConditions}{tsCondition})";
                    // X1: Add tenant predicate to prevent cross-tenant modifications
                    if (ctx.Options.TenantProvider != null)
                    {
                        var tenantCol = ctx.RequireTenantColumn(m, "SQLite bulk update");
                        var tenantParam = $"{ParamPrefix}__tenant_bulk";
                        cmd.AddParam(tenantParam, ctx.GetRequiredTenantId(m, "SQLite bulk update"));
                        whereClause += $" AND {m.EscTable}.{tenantCol.EscCol} = {tenantParam}";
                    }
                    cmd.CommandText = $"UPDATE {m.EscTable} SET {setClause} WHERE {whereClause}";
                    totalUpdated = await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                }

                // Clean up temp table
                await using (var cmd = ctx.RawConnection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = $"DROP TABLE {tempTableName}";
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkUpdateAsync), m.EscTable, totalUpdated, sw.Elapsed);
            return totalUpdated;
        }

        /// <summary>
        /// Maps a CLR type to its corresponding SQLite type affinity.
        /// </summary>
        private static string GetSqliteType(Type t)
        {
            t = Nullable.GetUnderlyingType(t) ?? t;
            if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte) || t == typeof(bool)) return "INTEGER";
            if (t == typeof(decimal) || t == typeof(double) || t == typeof(float)) return "REAL";
            if (t == typeof(byte[])) return "BLOB";
            return "TEXT";
        }

        /// <summary>
        /// Deletes entities in bulk using WHERE IN clauses for single-key tables
        /// or prepared statements for composite keys.
        /// Uses batched WHERE IN clauses; significantly faster than the base class operations.
        /// </summary>
        /// <typeparam name="T">Type of entity to delete.</typeparam>
        /// <param name="ctx">The <see cref="DbContext"/> managing the connection.</param>
        /// <param name="m">Mapping that provides key column information.</param>
        /// <param name="entities">Entities whose keys determine the rows to remove.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Total number of rows deleted.</returns>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Bulk operations build provider transfer structures from mapping metadata and may dispatch to reflection-loaded driver APIs; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Bulk operations reflect over entity and driver types; trimming may remove the required members. See docs/aot-trimming.md.")]
        public override async Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct) where T : class
        {
            ValidateConnection(ctx.RawConnection);
            var sw = Stopwatch.StartNew();
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;

            if (m.KeyColumns.Length == 0)
                throw new NormConfigurationException($"Cannot delete from '{m.EscTable}': no key columns defined.");

            var totalDeleted = 0;
            // Respect the provider parameter limit when batching deletes. Each row binds
            // one parameter per key column plus one for the concurrency token (OCC path);
            // a single tenant parameter is shared across the batch. Dividing MaxParameters
            // by the per-row cost (with headroom) prevents "too many SQL variables" — the
            // previous min(BulkBatchSize, MaxParameters) ignored per-row cost and overflowed
            // once the token and/or composite keys were bound.
            var paramsPerRow = Math.Max(1, m.KeyColumns.Length + (m.TimestampColumn != null ? 1 : 0));
            var batchSize = ctx.Options.BulkBatchSize;
            if (MaxParameters != int.MaxValue)
            {
                var tenantHeadroom = ctx.Options.TenantProvider != null ? 1 : 0;
                var maxByParams = Math.Max(1, (MaxParameters - tenantHeadroom - 5) / paramsPerRow);
                batchSize = Math.Min(batchSize, maxByParams);
            }
            if (batchSize <= 0) batchSize = 1;

            // Respect ambient CurrentTransaction; only create a new transaction if none is active.
            bool ownedTx = ctx.CurrentTransaction == null;
            DbTransaction transaction = ctx.CurrentTransaction
                ?? await ctx.RawConnection.BeginTransactionAsync(ct).ConfigureAwait(false);
            try
            {
                if (m.KeyColumns.Length == 1)
                {
                    var keyCol = m.KeyColumns[0];

                    for (int i = 0; i < entityList.Count; i += batchSize)
                    {
                        var batch = entityList.GetRange(i, Math.Min(batchSize, entityList.Count - i));
                        await using var cmd = ctx.RawConnection.CreateCommand();
                        cmd.Transaction = transaction;
                        cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

                        var paramIndex = 0;

                        // X1: Add tenant predicate to prevent cross-tenant deletes
                        var tenantSuffix = "";
                        if (ctx.Options.TenantProvider != null)
                        {
                            var tenantCol = ctx.RequireTenantColumn(m, "SQLite bulk delete");
                            var tenantParam = $"{ParamPrefix}__tenant_bulk";
                            cmd.AddParam(tenantParam, ctx.GetRequiredTenantId(m, "SQLite bulk delete"));
                            tenantSuffix = $" AND {tenantCol.EscCol} = {tenantParam}";
                        }

                        if (m.TimestampColumn != null)
                        {
                            // Optimistic concurrency: match each row by (key AND token) so a
                            // row whose token another writer bumped is skipped, not destroyed.
                            // Consistent with bulk update — stale rows are skipped and the
                            // reduced affected count is returned (no throw).
                            var tc = m.TimestampColumn;
                            var rowConds = new List<string>();
                            foreach (var entity in batch)
                            {
                                var kp = $"{ParamPrefix}p{paramIndex++}";
                                var tp = $"{ParamPrefix}p{paramIndex++}";
                                cmd.AddParam(kp, keyCol.Getter(entity));
                                cmd.AddParam(tp, tc.Getter(entity));
                                rowConds.Add($"({keyCol.EscCol} = {kp} AND ({tc.EscCol} = {tp} OR ({tc.EscCol} IS NULL AND {tp} IS NULL)))");
                            }
                            cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE ({string.Join(" OR ", rowConds)}){tenantSuffix}";
                        }
                        else
                        {
                            var paramNames = new List<string>();
                            foreach (var entity in batch)
                            {
                                var paramName = $"{ParamPrefix}p{paramIndex++}";
                                paramNames.Add(paramName);
                                cmd.AddParam(paramName, keyCol.Getter(entity));
                            }
                            cmd.CommandText = $"DELETE FROM {m.EscTable} WHERE {keyCol.EscCol} IN ({string.Join(",", paramNames)}){tenantSuffix}";
                        }
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
                }
                else
                {
                    // X1: Build delete SQL with optional tenant predicate
                    bool hasTenant = ctx.Options.TenantProvider != null;
                    var tenantColumn = hasTenant ? ctx.RequireTenantColumn(m, "SQLite bulk delete") : null;
                    string compositeDeleteSql;
                    if (hasTenant)
                    {
                        var whereParts = m.KeyColumns.Select(c => $"{c.EscCol}={ParamPrefix}{c.PropName}").ToList();
                        if (m.TimestampColumn != null)
                        {
                            var tc = m.TimestampColumn;
                            whereParts.Add($"({tc.EscCol}={ParamPrefix}{tc.PropName} OR ({tc.EscCol} IS NULL AND {ParamPrefix}{tc.PropName} IS NULL))");
                        }
                        whereParts.Add($"{tenantColumn!.EscCol}={ParamPrefix}__tenant_bulk");
                        compositeDeleteSql = $"DELETE FROM {m.EscTable} WHERE {string.Join(" AND ", whereParts)}";
                    }
                    else
                    {
                        compositeDeleteSql = BuildDelete(m);
                    }

                    await using var cmd = ctx.RawConnection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = compositeDeleteSql;
                    cmd.CommandTimeout = (int)ctx.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
                    await cmd.PrepareAsync(ct).ConfigureAwait(false);

                    foreach (var entity in entityList)
                    {
                        cmd.Parameters.Clear();
                        foreach (var col in m.KeyColumns)
                        {
                            cmd.AddParam(ParamPrefix + col.PropName, col.Getter(entity));
                        }
                        if (m.TimestampColumn != null)
                        {
                            cmd.AddParam(ParamPrefix + m.TimestampColumn.PropName, m.TimestampColumn.Getter(entity));
                        }
                        if (hasTenant)
                        {
                            cmd.AddParam($"{ParamPrefix}__tenant_bulk", ctx.GetRequiredTenantId(m, "SQLite bulk delete"));
                        }
                        totalDeleted += await cmd.ExecuteNonQueryWithInterceptionAsync(ctx, ct).ConfigureAwait(false);
                    }
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

            ctx.Options.CacheProvider?.InvalidateTag(m.TableName); // X2
            ctx.Options.Logger?.LogBulkOperation(nameof(BulkDeleteAsync), m.EscTable, totalDeleted, sw.Elapsed);
            return totalDeleted;
        }
    }
}
