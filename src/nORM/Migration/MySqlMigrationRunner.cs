using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Internal;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;

#nullable enable

namespace nORM.Migration
{
    /// <summary>
    /// Executes migrations against a MySQL database using a supplied connection and migrations assembly.
    /// </summary>
    public class MySqlMigrationRunner : IMigrationRunner, IAsyncDisposable, IDisposable
    {
        private readonly DbConnection _connection;
        private readonly Assembly _migrationsAssembly;
        private DbContext? _context;
        private const string HistoryTableName = "__NormMigrationsHistory";
        internal const string MigrationLockName = "__NormMigrationsLock";
        internal const int MigrationLockTimeoutSeconds = 30;
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlMigrationRunner"/> class.
        /// </summary>
        /// <param name="connection">Open connection to the target MySQL database.</param>
        /// <param name="migrationsAssembly">Assembly containing migration classes.</param>
        /// <param name="options">Optional DbContext configuration for interceptors.</param>
        public MySqlMigrationRunner(DbConnection connection, Assembly migrationsAssembly, DbContextOptions? options = null)
        {
            _connection = connection;
            _migrationsAssembly = migrationsAssembly;
            if (options != null && options.CommandInterceptors.Count > 0)
            {
                // Pass ownsConnection=false so the context does NOT dispose the
                // caller-supplied connection when the context itself is disposed.
                _context = new DbContext(connection, new MySqlProvider(new GenericParameterFactory(connection)), options, ownsConnection: false);
            }
        }

        /// <summary>
        /// Applies all pending migrations to the MySQL database.
        /// Acquires an exclusive named advisory lock (<c>GET_LOCK</c>) before reading the pending list,
        /// preventing concurrent deployers from running the same DDL simultaneously.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task ApplyMigrationsAsync(CancellationToken ct = default)
        {
            // Ensure the connection is open before calling BeginTransactionAsync.
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);

            await AcquireAdvisoryLockAsync(ct).ConfigureAwait(false);
            try
            {
                await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
                var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
                if (!pending.Any()) return;

                // MySQL DDL (ALTER TABLE, CREATE TABLE, DROP TABLE, etc.) implicitly auto-commits,
                // so a single wrapping transaction cannot roll back schema changes on failure.
                //
                // M1 durable-checkpoint strategy: before running each step, write a Partial row to
                // the history table (auto-committed, outside the per-step transaction). If the step
                // succeeds, UPDATE the row to Applied inside the transaction and commit. If the step
                // fails (or the process dies mid-flight), the Partial row remains and the NEXT run
                // will detect it and throw an informative error instead of silently re-executing
                // potentially-incompatible DDL. The operator can DELETE the Partial row after
                // manually inspecting / completing the schema, then rerun.
                foreach (var migration in pending)
                {
                    // M1 fix: start the transaction BEFORE writing the Partial checkpoint.
                    // Previously, the checkpoint was written first and then the transaction opened.
                    // If BeginTransactionAsync failed, a Partial row remained with no actual schema
                    // change, falsely blocking all future reruns. By starting the transaction first,
                    // a pre-DDL failure rolls back the checkpoint too (MySQL auto-commits DDL but
                    // the checkpoint INSERT is DML and participates in the transaction).
                    await using var transaction = await _connection.BeginTransactionAsync(ct).ConfigureAwait(false);

                    // Write durable Partial checkpoint inside the transaction.
                    // If the migration fails, the Partial row persists (auto-committed DDL),
                    // but if BeginTransactionAsync fails, no Partial row is created.
                    await InsertMigrationCheckpointAsync(migration, ct).ConfigureAwait(false);
                    try
                    {
                        migration.Up(_connection, (DbTransaction)transaction, ct);
                        await MarkMigrationCompletedAsync(migration, (DbTransaction)transaction, ct).ConfigureAwait(false);
                        // CancellationToken.None — commit must not be aborted mid-flight.
                        await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Attempt rollback; DDL may have already auto-committed (no-op for those statements).
                        // The Partial checkpoint row survives so the next run can report it explicitly.
                        try { await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false); }
                        catch { /* DDL auto-commit makes rollback a no-op; suppress to surface root cause */ }
                        throw;
                    }
                }
            }
            finally
            {
                await ReleaseAdvisoryLockAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Acquires a MySQL named advisory lock using <c>GET_LOCK(name, timeout)</c>.
        /// Returns 1 if acquired, 0 if timeout, NULL on error. Throws
        /// <see cref="InvalidOperationException"/> on timeout or error so the caller is
        /// not silently blocked.
        /// </summary>
        protected internal virtual async Task AcquireAdvisoryLockAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT GET_LOCK(@lockName, @lockTimeout)";
            var pName = cmd.CreateParameter();
            pName.ParameterName = "@lockName";
            pName.Value = MigrationLockName;
            cmd.Parameters.Add(pName);
            var pTimeout = cmd.CreateParameter();
            pTimeout.ParameterName = "@lockTimeout";
            pTimeout.Value = MigrationLockTimeoutSeconds;
            cmd.Parameters.Add(pTimeout);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result == null || result == DBNull.Value || Convert.ToInt32(result) != 1)
                throw new InvalidOperationException(
                    $"nORM: Failed to acquire migration advisory lock (GET_LOCK returned {result}). " +
                    "Another process may already be deploying migrations.");
        }

        /// <summary>
        /// Releases the MySQL named advisory lock acquired by <see cref="AcquireAdvisoryLockAsync"/>.
        /// </summary>
        protected internal virtual async Task ReleaseAdvisoryLockAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "DO RELEASE_LOCK(@lockName)";
            var pName = cmd.CreateParameter();
            pName.ParameterName = "@lockName";
            pName.Value = MigrationLockName;
            cmd.Parameters.Add(pName);
            try { await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false); }
            catch (Exception ex)
            {
                // M1: Best-effort release; surface failure for diagnostics rather than silently swallowing.
                // Do not propagate — throwing from a finally block would mask the original exception.
                var logger = _context?.Options?.Logger;
                if (logger != null)
                    logger.LogWarning(
                        "nORM: MySQL migration advisory-lock release failed: {Message}. " +
                        "A stale GET_LOCK entry may block future migrations until the connection resets.",
                        ex.Message);
                else
                    System.Diagnostics.Trace.TraceWarning(
                        $"nORM: MySQL migration advisory-lock release failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Determines whether there are migrations that have not yet been applied.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if pending migrations exist; otherwise <c>false</c>.</returns>
        public async Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default)
        {
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Any();
        }

        /// <summary>
        /// Retrieves the identifiers of all pending migrations.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An array containing the pending migration identifiers.</returns>
        public async Task<string[]> GetPendingMigrationsAsync(CancellationToken ct = default)
        {
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Select(p => $"{p.Version}_{p.Name}").ToArray();
        }

        /// <summary>
        /// Scans the migrations assembly and returns migrations that have not yet been applied to the
        /// target database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A list of pending <see cref="Migration"/> instances ordered by version.</returns>
        private async Task<List<Migration>> GetPendingMigrationsInternalAsync(CancellationToken ct)
        {
            var all = _migrationsAssembly.GetTypes()
                .Where(t => typeof(Migration).IsAssignableFrom(t) && !t.IsAbstract)
                .Select(t => (Migration)Activator.CreateInstance(t)!)
                .OrderBy(m => m.Version)
                .ToList();

            // Fail-fast on duplicate version numbers in assembly.
            var duplicates = all.GroupBy(m => m.Version).Where(g => g.Count() > 1).ToList();
            if (duplicates.Count > 0)
            {
                var desc = string.Join(", ", duplicates.Select(g =>
                    $"v{g.Key}: [{string.Join(", ", g.Select(m => m.Name))}]"));
                throw new InvalidOperationException(
                    $"Duplicate migration versions detected: {desc}. " +
                    "Each migration must have a unique Version.");
            }

            var applied = await GetAppliedMigrationsAsync(ct).ConfigureAwait(false);

            // M1: Detect Partial state — DDL auto-committed but step was not recorded as Applied.
            // Silently re-running an already-partially-applied migration can corrupt the schema.
            // Require operator intervention: inspect schema, then DELETE the Partial row to retry.
            var partials = applied
                .Where(kvp => string.Equals(kvp.Value.Status, "Partial", StringComparison.Ordinal))
                .ToList();
            if (partials.Count > 0)
            {
                var desc = string.Join(", ", partials.Select(p => $"v{p.Key} '{p.Value.Name}'"));
                throw new InvalidOperationException(
                    $"nORM: MySQL migration(s) are in Partial state (DDL auto-committed, step not recorded as Applied): {desc}. " +
                    $"Inspect the schema manually, then DELETE FROM `{HistoryTableName}` WHERE Version IN " +
                    $"({string.Join(",", partials.Select(p => p.Key))}) to retry.");
            }

            // Name drift check (Applied rows only; Partial rows already threw above).
            foreach (var m in all.Where(m => applied.TryGetValue(m.Version, out var info) &&
                string.Equals(info.Status, "Applied", StringComparison.Ordinal) &&
                !string.Equals(info.Name, m.Name, StringComparison.Ordinal)))
            {
                applied.TryGetValue(m.Version, out var recordedInfo);
                throw new InvalidOperationException(
                    $"Migration version {m.Version} name drift: recorded '{recordedInfo.Name}', found '{m.Name}'. " +
                    "Rename the migration class back to its original name or create a new migration version.");
            }

            // Pending = versions absent from history entirely (Partial rows already caused a throw above).
            return all.Where(m => !applied.ContainsKey(m.Version)).ToList();
        }

        /// <summary>
        /// M1: Inserts a durable <c>Partial</c> checkpoint row for the migration BEFORE the
        /// per-step transaction begins. If the step fails (including a mid-flight process kill),
        /// this row remains and is detected on the next run, triggering a loud error instead of
        /// silently re-applying already-partially-committed DDL.
        /// </summary>
        private async Task InsertMigrationCheckpointAsync(Migration migration, CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"INSERT INTO `{HistoryTableName}` (`Version`, `Name`, `AppliedOn`, `Status`) VALUES (@Version, @Name, @AppliedOn, 'Partial');";
            cmd.AddParam("@Version", migration.Version);
            cmd.AddParam("@Name", migration.Name);
            cmd.AddParam("@AppliedOn", DateTime.UtcNow);
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// M1: Updates the <c>Partial</c> checkpoint row to <c>Applied</c> inside the per-step
        /// transaction. Both this UPDATE and the transaction commit must succeed for the migration
        /// to be considered durable.
        /// </summary>
        private async Task MarkMigrationCompletedAsync(Migration migration, DbTransaction transaction, CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = $"UPDATE `{HistoryTableName}` SET `Status` = 'Applied' WHERE `Version` = @Version;";
            cmd.AddParam("@Version", migration.Version);
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieves all history rows (Applied and Partial) from the history table.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A dictionary mapping version numbers to <c>(Name, Status)</c> tuples.</returns>
        private async Task<Dictionary<long, (string Name, string Status)>> GetAppliedMigrationsAsync(CancellationToken ct)
        {
            var applied = new Dictionary<long, (string Name, string Status)>();
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT `Version`, `Name`, `Status` FROM `{HistoryTableName}`";
            try
            {
                await using var reader = await ExecuteReaderAsync(cmd, ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    applied[reader.GetInt64(0)] = (reader.GetString(1), reader.GetString(2));
                }
            }
            catch (DbException ex) when (IsTableNotFoundError(ex))
            {
                // MG-1: History table doesn't exist yet (first run) — return empty dict.
            }
            return applied;
        }

        /// <summary>
        /// MG-1: Returns true only when the exception indicates the history table does not exist yet.
        /// MySQL error 1146 = "Table doesn't exist".
        /// Transient errors (connection drops, permission failures, deadlocks) are NOT matched and
        /// will propagate to the caller.
        /// </summary>
        private static bool IsTableNotFoundError(DbException ex)
        {
            // Check by error number when the exception exposes it (MySqlConnector.MySqlException)
            var numberProp = ex.GetType().GetProperty("Number");
            if (numberProp != null && numberProp.GetValue(ex) is int number && number == 1146)
                return true;
            // Fallback: check the message text
            return ex.Message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("no such table", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Creates the migration history table (with Status column) if it does not exist,
        /// and upgrades existing tables that lack the Status column.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        private async Task EnsureHistoryTableAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE IF NOT EXISTS `{HistoryTableName}` " +
                "(Version BIGINT PRIMARY KEY, Name VARCHAR(255) NOT NULL, AppliedOn DATETIME(6) NOT NULL, " +
                "Status VARCHAR(20) NOT NULL DEFAULT 'Applied');";
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);

            // M1: Upgrade existing deployments that have the table without the Status column.
            // ALTER TABLE ADD COLUMN is idempotent via error suppression for "duplicate column name".
            await using var alterCmd = _connection.CreateCommand();
            alterCmd.CommandText = $"ALTER TABLE `{HistoryTableName}` ADD COLUMN Status VARCHAR(20) NOT NULL DEFAULT 'Applied'";
            try { await ExecuteNonQueryAsync(alterCmd, ct).ConfigureAwait(false); }
            catch (DbException ex) when (IsColumnAlreadyExistsError(ex)) { /* column already present — new schema */ }
        }

        /// <summary>
        /// Returns <c>true</c> when the exception indicates that the column already exists.
        /// MySQL error 1060 = "Duplicate column name"; SQLite message = "duplicate column name".
        /// </summary>
        private static bool IsColumnAlreadyExistsError(DbException ex)
        {
            var numberProp = ex.GetType().GetProperty("Number");
            if (numberProp?.GetValue(ex) is int n && n == 1060) return true;
            return ex.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Executes a non-query command, optionally routing through interceptors when a context is available.
        /// </summary>
        /// <param name="cmd">The command to execute.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>The number of rows affected.</returns>
        private Task<int> ExecuteNonQueryAsync(DbCommand cmd, CancellationToken ct)
            => _context != null ? cmd.ExecuteNonQueryWithInterceptionAsync(_context, ct) : cmd.ExecuteNonQueryAsync(ct);

        /// <summary>
        /// Executes a reader command, optionally routing through interceptors when a context is available.
        /// </summary>
        /// <param name="cmd">The command to execute.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A <see cref="DbDataReader"/> containing the results.</returns>
        private Task<DbDataReader> ExecuteReaderAsync(DbCommand cmd, CancellationToken ct)
            => _context != null ? cmd.ExecuteReaderWithInterceptionAsync(_context, CommandBehavior.Default, ct) : cmd.ExecuteReaderAsync(ct);

        /// <summary>Asynchronously disposes the internal <see cref="DbContext"/> created for interceptors.</summary>
        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                if (_context != null)
                {
                    await _context.DisposeAsync().ConfigureAwait(false);
                    _context = null;
                }
            }
        }

        /// <summary>Synchronously disposes the internal <see cref="DbContext"/> created for interceptors.</summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                if (_context != null)
                {
                    _context.Dispose();
                    _context = null;
                }
            }
        }

        private sealed class GenericParameterFactory : IDbParameterFactory
        {
            private readonly DbConnection _connection;

            /// <summary>
            /// Initializes a new instance of the parameter factory using the provided connection.
            /// </summary>
            public GenericParameterFactory(DbConnection connection) => _connection = connection;

            /// <summary>
            /// Creates a simple <see cref="DbParameter"/> with the given name and value using the
            /// underlying provider's <see cref="DbCommand"/> implementation.
            /// </summary>
            /// <param name="name">The parameter name including provider-specific prefix.</param>
            /// <param name="value">The value to assign.</param>
            /// <returns>The configured <see cref="DbParameter"/> instance.</returns>
            public DbParameter CreateParameter(string name, object? value)
            {
                using var cmd = _connection.CreateCommand();
                var p = cmd.CreateParameter();
                p.ParameterName = name;
                p.Value = value ?? DBNull.Value;
                return p;
            }
        }
    }
}
