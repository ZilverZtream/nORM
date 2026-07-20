using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;

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
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Temporal initialization walks navigation metadata that reflects over entity members; trimming may remove the required members.")]
        public static async Task InitializeAsync(DbContext context, DbConnection conn, CancellationToken ct = default)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(conn);
            ct.ThrowIfCancellationRequested();
            ThrowIfTemporalBootstrapTargetsProtectedDatabase(context.RawProvider, conn);

            await CreateTagsTableIfNotExistsAsync(context, conn, ct).ConfigureAwait(false);

            var providerNative = context.Options.TemporalStorageMode == nORM.Configuration.TemporalStorageMode.ProviderNative;
            if (providerNative && !context.RawProvider.SupportsProviderNativeTemporalTables)
                throw new NormUnsupportedFeatureException(
                    $"{context.RawProvider.GetType().Name} does not support provider-native temporal tables.");

            // The sweep must version only REAL entity tables. GetAllMappings also yields projection types
            // (anonymous shapes, and named DTOs that pick up a convention 'Id' key) that the query pipeline
            // cached while probing member types — versioning those tries to ALTER/CREATE a table that does not
            // exist. Physical-existence introspection is NOT a safe discriminator here: it is scoped to the
            // provider's default schema (dbo/public), so a real table an unqualified mapping resolves to in a
            // non-default login schema would introspect as absent and be silently skipped — permanent, silent
            // history loss. Instead, gate on the entity closure reachable from configured/queried roots through
            // navigations; projections are never in it, and a real table is included regardless of its schema.
            var realEntityTypes = BuildRealEntityTypeClosure(context);

            foreach (var mapping in context.GetAllMappings())
            {
                ct.ThrowIfCancellationRequested();

                if (!realEntityTypes.Contains(mapping.Type))
                    continue;

                // Temporal storage is only meaningful for a table with a primary key: provider-native
                // system-versioning requires one, and nORM-managed history correlates row versions by key.
                // A keyless entity (HasNoKey) can never be reconstructed by key, so it is not a candidate.
                if (mapping.KeyColumns.Length == 0)
                    continue;

                if (providerNative)
                {
                    var bootstrapSql = context.RawProvider.GenerateProviderNativeTemporalBootstrapSql(mapping);
                    await ExecuteDdlAsync(context, conn, bootstrapSql, ct).ConfigureAwait(false);
                }
                else
                {
                    // Introspect the LIVE physical column set: it mirrors custom precision/length AND covers
                    // columns that exist only physically (an owned collection's foreign key, a raw ADD COLUMN),
                    // which history tables/triggers built from the mapped property set alone would silently
                    // omit. When the table is in a non-default schema this returns empty and the DDL generators
                    // fall back to the mapped column set (typed by provider representation) — still correct.
                    var liveColumns = await IntrospectTableColumnsAsync(context, conn, mapping.TableName, ct)
                        .ConfigureAwait(false);

                    if (!await HistoryTableExistsAsync(context, conn, mapping, ct).ConfigureAwait(false))
                    {
                        var createHistoryTableSql = context.RawProvider.GenerateCreateHistoryTableSql(mapping, liveColumns);
                        await ExecuteDdlAsync(context, conn, createHistoryTableSql, ct).ConfigureAwait(false);
                    }

                    var createTriggersSql = context.RawProvider.GenerateTemporalTriggersSql(mapping, liveColumns);
                    await ExecuteDdlAsync(context, conn, createTriggersSql, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// The set of CLR types that are genuine mapped entities: configured/queried roots plus every entity
        /// reachable from them through relationship collections, reference navigations, owned collections, and
        /// many-to-many joins. Query projection types (DTOs, anonymous shapes) leak into the mapping cache via
        /// member-type probes but are never reachable this way, so they are excluded — without a physical-table
        /// probe that would be blind to non-default schemas.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Walks navigation metadata that reflects over entity members.")]
        private static HashSet<Type> BuildRealEntityTypeClosure(DbContext context)
        {
            var real = new HashSet<Type>();
            var queue = new Queue<TableMapping>();

            void Consider(Type? type)
            {
                if (type != null && real.Add(type))
                    queue.Enqueue(context.GetMapping(type));
            }

            foreach (var mapping in context.GetAllMappings())
                if (context.IsMapped(mapping.Type) && real.Add(mapping.Type))
                    queue.Enqueue(mapping);

            while (queue.Count > 0)
            {
                var mapping = queue.Dequeue();
                foreach (var relation in mapping.Relations.Values)
                    Consider(relation.DependentType);
                foreach (var referenceNav in mapping.ReferenceNavigations)
                    Consider(referenceNav.PropertyType);
                foreach (var owned in mapping.OwnedCollections)
                    Consider(owned.OwnedType);
                foreach (var join in mapping.ManyToManyJoins)
                    Consider(join.RightType);
            }

            return real;
        }

        private static void ThrowIfTemporalBootstrapTargetsProtectedDatabase(DatabaseProvider provider, DbConnection conn)
        {
            string? databaseName;
            try
            {
                databaseName = conn.Database;
            }
            catch (InvalidOperationException)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(databaseName))
                return;

            var trimmed = databaseName.Trim();
            if (!IsProtectedDatabaseName(provider, trimmed))
                return;

            throw new NormConfigurationException(
                $"Temporal versioning cannot bootstrap temporal storage tables or triggers in provider-owned database '{trimmed}'. Use an application database/schema such as 'normtest' for temporal storage.");
        }

        private static bool IsProtectedDatabaseName(DatabaseProvider provider, string databaseName)
            => provider switch
            {
                SqlServerProvider => databaseName.Equals("master", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("model", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("msdb", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
                PostgresProvider => databaseName.Equals("postgres", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("template0", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("template1", StringComparison.OrdinalIgnoreCase),
                MySqlProvider => databaseName.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("sys", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
                    || databaseName.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
                _ => false
            };

        private static async Task CreateTagsTableIfNotExistsAsync(DbContext context, DbConnection conn, CancellationToken ct)
        {
            // Use provider-specific DDL so that IF NOT EXISTS and column types
            // are correct for each database (SQL Server requires OBJECT_ID check + NVARCHAR/DATETIME2).
            var sql = context.RawProvider.GetCreateTagsTableSql();
            await ExecuteDdlAsync(context, conn, sql, ct).ConfigureAwait(false);
        }

        private static async Task<bool> HistoryTableExistsAsync(DbContext context, DbConnection conn, TableMapping mapping, CancellationToken ct)
        {
            var historyTable = context.RawProvider.Escape(mapping.TableName + "_History");
            // Use provider-specific probe SQL (SQL Server needs TOP 1, not LIMIT 1).
            var probeSql = context.RawProvider.GetHistoryTableExistsProbeSql(historyTable);
            try
            {
                var gate = CommandInterceptorExtensions.GetSerializedConnectionGate(conn, context);
                if (gate != null)
                    await gate.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = probeSql;
                    await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    return true;
                }
                finally
                {
                    gate?.Release();
                }
            }
            catch (DbException dbEx) when (context.RawProvider.IsObjectNotFoundError(dbEx))
            {
                // Only treat definitive "table not found" schema errors as absence.
                // Permission denied, transient connection faults, and syntax errors are NOT "table absent"
                // — they must propagate so the caller sees a real failure, not a phantom DDL re-creation.
                return false;
            }
            // All other exceptions (non-DbException, permission errors, connectivity) propagate.
        }

        private static async Task<IReadOnlyList<DatabaseProvider.LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbContext context, DbConnection conn, string tableName, CancellationToken ct)
        {
            var gate = CommandInterceptorExtensions.GetSerializedConnectionGate(conn, context);
            if (gate == null)
                return await context.RawProvider.IntrospectTableColumnsAsync(conn, tableName, ct).ConfigureAwait(false);

            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                return await context.RawProvider.IntrospectTableColumnsAsync(conn, tableName, ct).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
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
