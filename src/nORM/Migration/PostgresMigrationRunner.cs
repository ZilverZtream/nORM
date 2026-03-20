using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Migration
{
    /// <summary>
    /// Executes migrations against a PostgreSQL database using a supplied connection and migrations assembly.
    /// </summary>
    public class PostgresMigrationRunner : IMigrationRunner, IAsyncDisposable, IDisposable
    {
        private readonly DbConnection _connection;
        private readonly Assembly _migrationsAssembly;
        private DbContext? _context;
        private readonly ILogger? _logger;
        private const string HistoryTableName = "__NormMigrationsHistory";
        // Stable int8 key used for pg_advisory_lock. Derived from FNV-1a of "__NormMigrationsLock"
        // so it is consistent across deployments and does not collide with application-defined lock keys.
        internal const long MigrationLockKey = unchecked((long)0x62C3B8F921A4D507L);

        /// <summary>PostgreSQL SqlState 42P01: undefined_table (table does not exist).</summary>
        private const string PgSqlStateUndefinedTable = "42P01";

        /// <summary>Default timeout for advisory lock acquisition retry loop.</summary>
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);

        /// <summary>Delay between advisory lock acquisition retries, in milliseconds.</summary>
        private const int LockRetryDelayMs = 250;

        private volatile bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresMigrationRunner"/> class.
        /// </summary>
        /// <param name="connection">Open connection to the target PostgreSQL database.</param>
        /// <param name="migrationsAssembly">Assembly containing migration classes.</param>
        /// <param name="options">Optional DbContext configuration for interceptors.</param>
        /// <param name="logger">Optional logger for drift warnings and diagnostics.</param>
        public PostgresMigrationRunner(DbConnection connection, Assembly migrationsAssembly, DbContextOptions? options = null, ILogger? logger = null)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _migrationsAssembly = migrationsAssembly ?? throw new ArgumentNullException(nameof(migrationsAssembly));
            _logger = logger;
            if (options != null && options.CommandInterceptors.Count > 0)
            {
                // Pass ownsConnection=false so the context does NOT dispose the
                // caller-supplied connection when the context itself is disposed.
                _context = new DbContext(connection, new PostgresProvider(new GenericParameterFactory(connection)), options, ownsConnection: false);
            }
        }

        /// <summary>
        /// Applies all pending migrations to the PostgreSQL database.
        /// Acquires a session-level advisory lock (<c>pg_try_advisory_lock</c>) before reading the
        /// pending list, preventing concurrent deployers from running the same DDL simultaneously.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task ApplyMigrationsAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            // Ensure the connection is open before calling BeginTransactionAsync.
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);

            await AcquireAdvisoryLockAsync(ct).ConfigureAwait(false);
            try
            {
                await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
                var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
                if (pending.Count == 0) return;

                await using var transaction = await _connection.BeginTransactionAsync(ct).ConfigureAwait(false);
                foreach (var migration in pending)
                {
                    migration.Up(_connection, (DbTransaction)transaction, ct);
                    await MarkMigrationAppliedAsync(migration, (DbTransaction)transaction, ct).ConfigureAwait(false);
                }
                // Use CancellationToken.None — commit must not be aborted mid-flight.
                await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                await ReleaseAdvisoryLockAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Acquires a PostgreSQL session-level advisory lock using <c>pg_try_advisory_lock(bigint)</c>
        /// in a polling loop with bounded timeout.
        /// <para>
        /// Uses <c>pg_try_advisory_lock</c> (non-blocking) with a retry loop instead of the blocking
        /// <c>pg_advisory_lock</c> to prevent indefinite blocking when a stale session holds the lock.
        /// The default timeout (30 seconds) matches the operational expectations of automated deploys.
        /// MySQL and SQL Server runners already have bounded lock acquisition; this brings PostgreSQL
        /// to parity.
        /// </para>
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <param name="timeout">Maximum time to wait for the lock. Defaults to 30 seconds.</param>
        internal async Task AcquireAdvisoryLockAsync(CancellationToken ct, TimeSpan? timeout = null)
        {
            var effectiveTimeout = timeout ?? DefaultLockTimeout;
            var deadline = DateTime.UtcNow + effectiveTimeout;

            while (true)
            {
                ct.ThrowIfCancellationRequested();

                await using var cmd = _connection.CreateCommand();
                cmd.CommandText = $"SELECT pg_try_advisory_lock({MigrationLockKey})";
                var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);

                // pg_try_advisory_lock returns true if the lock was acquired
                if (result is bool acquired && acquired)
                    return;
                // Some drivers return string "t"/"f" instead of bool
                if (result is string s && s.Equals("t", StringComparison.OrdinalIgnoreCase))
                    return;

                if (DateTime.UtcNow >= deadline)
                    throw new TimeoutException(
                        $"Failed to acquire PostgreSQL migration advisory lock (key={MigrationLockKey}) " +
                        $"within {effectiveTimeout.TotalSeconds} seconds. " +
                        "Another migration runner or stale session may be holding the lock.");

                await Task.Delay(LockRetryDelayMs, ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Releases the PostgreSQL session-level advisory lock acquired by
        /// <see cref="AcquireAdvisoryLockAsync"/>.
        /// Uses <c>ExecuteScalarAsync</c> because <c>pg_advisory_unlock</c> returns a boolean result.
        /// </summary>
        internal async Task ReleaseAdvisoryLockAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT pg_advisory_unlock({MigrationLockKey})";
            try { await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false); }
            catch (DbException ex)
            {
                // Best-effort release; surface failure for diagnostics rather than silently swallowing.
                // Do not propagate — throwing from a finally block would mask the original exception.
                // Narrowed to DbException so that fatal CLR errors (OutOfMemoryException etc.) propagate.
                _logger?.LogWarning(
                    "nORM: PostgreSQL migration advisory-lock release failed: {Message}. " +
                    "A stale pg_advisory_lock entry may block future migrations until the session resets.",
                    ex.Message);
            }
        }

        /// <summary>
        /// Determines whether there are migrations that have not yet been applied.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if pending migrations exist; otherwise <c>false</c>.</returns>
        public async Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Count > 0;
        }

        /// <summary>
        /// Retrieves the identifiers of all pending migrations.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An array containing the pending migration identifiers.</returns>
        public async Task<string[]> GetPendingMigrationsAsync(CancellationToken ct = default)
        {
            ThrowIfDisposed();
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Select(p => $"{p.Version}_{p.Name}").ToArray();
        }

        /// <summary>
        /// Scans the migrations assembly and returns migrations that have not yet been applied to the
        /// target PostgreSQL database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A list of pending <see cref="Migration"/> instances ordered by version.</returns>
        private async Task<List<Migration>> GetPendingMigrationsInternalAsync(CancellationToken ct)
        {
            var all = _migrationsAssembly.GetTypes()
                .Where(t => typeof(Migration).IsAssignableFrom(t) && !t.IsAbstract)
                .Select(t => (Migration)(Activator.CreateInstance(t) ?? throw new InvalidOperationException($"Failed to create instance of migration type {t.FullName}")))
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

            foreach (var m in all.Where(m => applied.TryGetValue(m.Version, out var name) &&
                !string.Equals(name, m.Name, StringComparison.Ordinal)))
            {
                applied.TryGetValue(m.Version, out var recordedName);
                throw new InvalidOperationException(
                    $"Migration version {m.Version} name drift: recorded '{recordedName}', found '{m.Name}'. " +
                    "Rename the migration class back to its original name or create a new migration version.");
            }

            return all.Where(m => !applied.ContainsKey(m.Version)).ToList();
        }

        /// <summary>
        /// Records in the history table that the specified migration has been successfully applied.
        /// </summary>
        /// <param name="migration">The migration that was applied.</param>
        /// <param name="transaction">The active transaction.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        private async Task MarkMigrationAppliedAsync(Migration migration, DbTransaction transaction, CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = $"INSERT INTO \"{HistoryTableName}\" (\"Version\", \"Name\", \"AppliedOn\") VALUES (@Version, @Name, @AppliedOn);";
            cmd.AddParam("@Version", migration.Version);
            cmd.AddParam("@Name", migration.Name);
            cmd.AddParam("@AppliedOn", DateTime.UtcNow);
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieves the dictionary of migration versions and names that have already been applied
        /// to the database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A dictionary mapping version numbers to migration names.</returns>
        private async Task<Dictionary<long, string>> GetAppliedMigrationsAsync(CancellationToken ct)
        {
            var applied = new Dictionary<long, string>();
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT \"Version\", \"Name\" FROM \"{HistoryTableName}\"";
            try
            {
                await using var reader = await ExecuteReaderAsync(cmd, ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    applied[reader.GetInt64(0)] = reader.GetString(1);
                }
            }
            catch (DbException ex) when (IsTableNotFoundError(ex))
            {
                // History table doesn't exist yet (first run) — return empty dict.
                // All other DbException (transient failures, permission errors, etc.) propagate.
            }
            return applied;
        }

        /// <summary>
        /// Returns true only when the exception indicates the history table does not exist yet.
        /// PostgreSQL SqlState 42P01 = "undefined_table".
        /// Transient errors (connection drops, permission failures, deadlocks) are NOT matched and
        /// will propagate to the caller.
        /// </summary>
        private static bool IsTableNotFoundError(DbException ex)
        {
            // Check SqlState when the exception exposes it (Npgsql.PostgresException)
            var sqlStateProp = ex.GetType().GetProperty("SqlState");
            if (sqlStateProp != null && sqlStateProp.GetValue(ex) is string sqlState
                && string.Equals(sqlState, PgSqlStateUndefinedTable, StringComparison.Ordinal))
                return true;
            // Fallback: check the message text for common patterns
            return ex.Message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("no such table", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Creates the migration history table if it does not already exist.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        private async Task EnsureHistoryTableAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE IF NOT EXISTS \"{HistoryTableName}\" (Version BIGINT PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TIMESTAMP NOT NULL);";
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);
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

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this runner has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(PostgresMigrationRunner));
        }

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
            GC.SuppressFinalize(this);
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
            GC.SuppressFinalize(this);
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
