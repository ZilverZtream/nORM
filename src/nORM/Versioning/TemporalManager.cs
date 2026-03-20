using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Versioning
{
    /// <summary>
    /// Handles creation of temporal history tables and triggers for entities configured for versioning.
    /// </summary>
    internal static class TemporalManager
    {
        /// <summary>
        /// Ensures that temporal infrastructure exists for all mapped entities.
        /// </summary>
        /// <param name="context">The <see cref="DbContext"/> containing the mappings.</param>
        /// <param name="conn">The already-open connection. Passed explicitly to avoid re-entering
        /// <see cref="DbContext.EnsureConnectionAsync"/> from within the temporal bootstrap, which
        /// would deadlock on the semaphore already held by the caller.</param>
        /// <param name="ct">Token used to cancel the initialization. Pre-cancellation causes the
        /// method to throw <see cref="OperationCanceledException"/> before any DDL is executed.</param>
        /// <returns>A task representing the initialization process.</returns>
        public static async Task InitializeAsync(DbContext context, DbConnection conn, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();

            var provider = context.Provider;
            await CreateTagsTableIfNotExistsAsync(context, conn, ct).ConfigureAwait(false);

            foreach (var mapping in context.GetAllMappings())
            {
                ct.ThrowIfCancellationRequested();

                if (await HistoryTableExistsAsync(context, conn, mapping, ct).ConfigureAwait(false))
                    continue;

                // Introspect live column types before generating history DDL so that
                // custom precision/length on main-table columns is mirrored in history table.
                var liveColumns = await provider
                    .IntrospectTableColumnsAsync(conn, mapping.TableName, ct)
                    .ConfigureAwait(false);
                var createHistoryTableSql = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);
                await ExecuteDdlAsync(context, conn, createHistoryTableSql, ct).ConfigureAwait(false);

                var createTriggersSql = provider.GenerateTemporalTriggersSql(mapping);
                await ExecuteDdlAsync(context, conn, createTriggersSql, ct).ConfigureAwait(false);
            }
        }

        private static async Task CreateTagsTableIfNotExistsAsync(DbContext context, DbConnection conn, CancellationToken ct)
        {
            // Use provider-specific DDL so that IF NOT EXISTS and column types
            // are correct for each database (SQL Server requires OBJECT_ID check + NVARCHAR/DATETIME2).
            var sql = context.Provider.GetCreateTagsTableSql();
            await ExecuteDdlAsync(context, conn, sql, ct).ConfigureAwait(false);
        }

        private static async Task<bool> HistoryTableExistsAsync(DbContext context, DbConnection conn, TableMapping mapping, CancellationToken ct)
        {
            var historyTable = context.Provider.Escape(mapping.TableName + "_History");
            // Use provider-specific probe SQL (SQL Server needs TOP 1, not LIMIT 1).
            var probeSql = context.Provider.GetHistoryTableExistsProbeSql(historyTable);
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = probeSql;
                await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                return true;
            }
            catch (DbException dbEx) when (context.Provider.IsObjectNotFoundError(dbEx))
            {
                // Only treat definitive "table not found" schema errors as absence.
                // Permission denied, transient connection faults, and syntax errors are NOT "table absent"
                // — they must propagate so the caller sees a real failure, not a phantom DDL re-creation.
                return false;
            }
            // All other exceptions (non-DbException, permission errors, connectivity) propagate.
        }

        private static readonly NormExceptionHandler s_ddlHandler = new(NullLogger.Instance);

        private static async Task ExecuteDdlAsync(DbContext context, DbConnection conn, string sql, CancellationToken ct)
        {
            var handler = s_ddlHandler;

            // S6-1: Split on both T-SQL GO batch separators and MySQL trigger delimiters.
            // MySQL's CREATE TRIGGER ... BEGIN ... END blocks must be sent as individual
            // DbCommand statements — the ADO.NET driver accepts BEGIN...END with internal
            // semicolons in a single command, so we only need to separate multiple triggers.
            // The "-- DELIMITER" marker is used by MySqlProvider.GenerateTemporalTriggersSql
            // to separate individual CREATE TRIGGER statements.
            var batchSeparators = new[]
            {
                "\r\nGO\r\n", "\nGO\n", "\r\nGO\n", "\nGO\r\n", "\rGO\r", "\nGO\r",
                "\n-- DELIMITER\n", "\r\n-- DELIMITER\r\n", "\r\n-- DELIMITER\n", "\n-- DELIMITER\r\n",
                "-- DELIMITER"
            };
            var batches = sql.Split(batchSeparators, StringSplitOptions.RemoveEmptyEntries);

            foreach (var batch in batches)
            {
                ct.ThrowIfCancellationRequested();

                var trimmed = batch.Trim();
                if (trimmed.Length == 0) continue;

                if (!IsValidDdl(trimmed))
                    throw new NormQueryException($"Invalid DDL: {trimmed}");

                await handler.ExecuteWithExceptionHandling(async () =>
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = trimmed;
                    await cmd.ExecuteNonQueryWithInterceptionAsync(context, ct).ConfigureAwait(false);
                    return 0;
                }, "ExecuteDdlAsync", new Dictionary<string, object> { ["Sql"] = trimmed }).ConfigureAwait(false);
            }
        }

        private static bool IsValidDdl(string ddl)
        {
            return ddl.StartsWith("create", StringComparison.OrdinalIgnoreCase)
                || ddl.StartsWith("alter", StringComparison.OrdinalIgnoreCase)
                || ddl.StartsWith("drop", StringComparison.OrdinalIgnoreCase)
                || ddl.StartsWith("if", StringComparison.OrdinalIgnoreCase);   // SQL Server: IF OBJECT_ID(...) IS NULL CREATE TABLE
        }
    }
}
