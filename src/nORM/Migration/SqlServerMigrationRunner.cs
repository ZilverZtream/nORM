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
    /// Executes database migrations against a SQL Server instance. The runner scans
    /// an assembly for migration classes and applies them in order while maintaining
    /// a history table to track applied versions.
    /// </summary>
    public class SqlServerMigrationRunner : IMigrationRunner, IAsyncDisposable, IDisposable
    {
        private readonly DbConnection _connection;
        private readonly Assembly _migrationsAssembly;
        private DbContext? _context;
        private readonly ILogger? _logger;
        internal const string HistoryTableName = "__NormMigrationsHistory";
        private bool _disposed = false;

        /// <summary>
        /// Creates a new migration runner for SQL Server.
        /// </summary>
        /// <param name="connection">Open connection to the target database.</param>
        /// <param name="migrationsAssembly">Assembly containing migration classes.</param>
        /// <param name="options">Optional context configuration used for executing migrations.</param>
        /// <param name="logger">Optional logger for drift warnings and diagnostics.</param>
        public SqlServerMigrationRunner(DbConnection connection, Assembly migrationsAssembly, DbContextOptions? options = null, ILogger? logger = null)
        {
            _connection = connection;
            _migrationsAssembly = migrationsAssembly;
            _logger = logger;
            if (options != null && options.CommandInterceptors.Count > 0)
            {
                // TX-1/MG-1: Pass ownsConnection=false so the context does NOT dispose the
                // caller-supplied connection when the context itself is disposed.
                _context = new DbContext(connection, new SqlServerProvider(), options, ownsConnection: false);
            }
        }

        /// <summary>
        /// Applies all pending migrations to the SQL Server database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task ApplyMigrationsAsync(CancellationToken ct = default)
        {
            // MIG-1: Ensure the connection is open before calling BeginTransactionAsync
            if (_connection.State != System.Data.ConnectionState.Open)
                await _connection.OpenAsync(ct).ConfigureAwait(false);

            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            if (!pending.Any()) return;

            await using var transaction = await _connection.BeginTransactionAsync(ct).ConfigureAwait(false);
            foreach (var migration in pending)
            {
                migration.Up(_connection, (DbTransaction)transaction);
                await MarkMigrationAppliedAsync(migration, (DbTransaction)transaction, ct).ConfigureAwait(false);
            }
            await transaction.CommitAsync(ct).ConfigureAwait(false);
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
        /// target SQL Server database.
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

            var applied = await GetAppliedMigrationsAsync(ct).ConfigureAwait(false);

            foreach (var m in all.Where(m => applied.TryGetValue(m.Version, out var name) &&
                !string.Equals(name, m.Name, StringComparison.Ordinal)))
            {
                applied.TryGetValue(m.Version, out var recordedName);
                _logger?.LogWarning("Migration {Version} name drift: recorded '{Recorded}', found '{Current}'",
                    m.Version, recordedName, m.Name);
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
            cmd.CommandText = $"INSERT INTO [{HistoryTableName}] (Version, Name, AppliedOn) VALUES (@Version, @Name, @AppliedOn)";
            cmd.AddParam("@Version", migration.Version);
            cmd.AddParam("@Name", migration.Name);
            cmd.AddParam("@AppliedOn", DateTime.UtcNow);
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieves the set of migration versions that have already been applied to the database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A set containing the version numbers of applied migrations.</returns>
        private async Task<Dictionary<long, string>> GetAppliedMigrationsAsync(CancellationToken ct)
        {
            var applied = new Dictionary<long, string>();
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT [Version], [Name] FROM [{HistoryTableName}]";
            try
            {
                await using var reader = await ExecuteReaderAsync(cmd, ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    applied[reader.GetInt64(0)] = reader.GetString(1);
                }
            }
            catch (DbException)
            {
                // History table probably doesn't exist yet, return empty dict.
            }
            return applied;
        }

        /// <summary>
        /// Creates the migration history table if it does not already exist.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        private async Task EnsureHistoryTableAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID(N'{HistoryTableName}', N'U') IS NULL CREATE TABLE [{HistoryTableName}] (Version BIGINT PRIMARY KEY, Name NVARCHAR(255) NOT NULL, AppliedOn DATETIME2 NOT NULL);";
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
        /// Asynchronously disposes the internal <see cref="DbContext"/> created for interceptors.
        /// </summary>
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

        /// <summary>
        /// Synchronously disposes the internal <see cref="DbContext"/> created for interceptors.
        /// </summary>
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
    }
}