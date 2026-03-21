using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
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
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(conn);
            ct.ThrowIfCancellationRequested();

            await CreateTagsTableIfNotExistsAsync(context, conn, ct).ConfigureAwait(false);

            foreach (var mapping in context.GetAllMappings())
            {
                ct.ThrowIfCancellationRequested();

                if (await HistoryTableExistsAsync(context, conn, mapping, ct).ConfigureAwait(false))
                    continue;

                // Introspect live column types before generating history DDL so that
                // custom precision/length on main-table columns is mirrored in history table.
                var liveColumns = await context.Provider
                    .IntrospectTableColumnsAsync(conn, mapping.TableName, ct)
                    .ConfigureAwait(false);
                var createHistoryTableSql = context.Provider.GenerateCreateHistoryTableSql(mapping, liveColumns);
                await ExecuteDdlAsync(context, conn, createHistoryTableSql, ct).ConfigureAwait(false);

                var createTriggersSql = context.Provider.GenerateTemporalTriggersSql(mapping);
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

        // S6-1: Split on both T-SQL GO batch separators and MySQL trigger delimiters.
        // MySQL's CREATE TRIGGER ... BEGIN ... END blocks must be sent as individual
        // DbCommand statements — the ADO.NET driver accepts BEGIN...END with internal
        // semicolons in a single command, so we only need to separate multiple triggers.
        // The "-- DELIMITER" marker (with surrounding newlines) is used by
        // MySqlProvider.GenerateTemporalTriggersSql to separate individual CREATE TRIGGER statements.
        // NOTE: The bare "-- DELIMITER" form (no surrounding newlines) is intentionally excluded
        // to prevent accidental splits on inline comments within a single statement.
        private static readonly string[] s_batchSeparators =
        {
            "\r\nGO\r\n", "\nGO\n", "\r\nGO\n", "\nGO\r\n", "\rGO\r", "\nGO\r",
            "\n-- DELIMITER\n", "\r\n-- DELIMITER\r\n", "\r\n-- DELIMITER\n", "\n-- DELIMITER\r\n"
        };

        private static async Task ExecuteDdlAsync(DbContext context, DbConnection conn, string sql, CancellationToken ct)
        {
            // Use the context's configured logger so DDL failures surface in structured logs
            // rather than being silently discarded by NullLogger.
            var handler = new NormExceptionHandler(context.Options.Logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

            var batches = sql.Split(s_batchSeparators, StringSplitOptions.RemoveEmptyEntries);

            foreach (var batch in batches)
            {
                ct.ThrowIfCancellationRequested();

                var trimmed = batch.Trim();
                if (trimmed.Length == 0) continue;

                if (!IsValidDdl(trimmed))
                {
                    // Truncate the DDL to avoid leaking potentially sensitive schema details
                    // (table names, column names) in exception messages that may reach logs.
                    var preview = trimmed.Length > 80 ? trimmed[..80] + "…" : trimmed;
                    throw new NormQueryException($"Invalid DDL statement (must start with CREATE, ALTER, IF, DROP TRIGGER, DROP FUNCTION, or DROP PROCEDURE). Preview: {preview}");
                }

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
            if (ddl.StartsWith("create", StringComparison.OrdinalIgnoreCase))
                return true;
            if (ddl.StartsWith("alter", StringComparison.OrdinalIgnoreCase))
                return true;
            // IF OBJECT_ID(...) IS NULL ... — SQL Server conditional DDL.
            // Require a non-alpha/non-underscore character after "IF" to reject identifiers
            // like "IFERROR" or "INFORMATION_SCHEMA" that could begin a non-DDL statement.
            if (ddl.Length >= 3
                && ddl.StartsWith("if", StringComparison.OrdinalIgnoreCase)
                && !char.IsLetterOrDigit(ddl[2]) && ddl[2] != '_')
                return true;
            // Only allow DROP TRIGGER / DROP FUNCTION / DROP PROCEDURE (needed for temporal trigger
            // management, e.g. Postgres uses DROP TRIGGER IF EXISTS and DROP FUNCTION for idempotent
            // trigger replacement).
            // DROP TABLE, DROP DATABASE, DROP SCHEMA etc. are not legitimate temporal DDL
            // and must not be generated by provider methods.
            if (ddl.StartsWith("drop trigger", StringComparison.OrdinalIgnoreCase)
                || ddl.StartsWith("drop function", StringComparison.OrdinalIgnoreCase)
                || ddl.StartsWith("drop procedure", StringComparison.OrdinalIgnoreCase))
                return true;
            return false;
        }
    }
}
