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

#nullable enable

namespace nORM.Migration
{
    /// <summary>
    /// Executes migrations against a SQLite database using a supplied connection and migrations assembly.
    /// </summary>
    public class SqliteMigrationRunner : IMigrationRunner
    {
        private readonly DbConnection _connection;
        private readonly Assembly _migrationsAssembly;
        private readonly DbContext? _context;
        private const string HistoryTableName = "__NormMigrationsHistory";

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteMigrationRunner"/> class.
        /// </summary>
        /// <param name="connection">Open connection to the target SQLite database.</param>
        /// <param name="migrationsAssembly">Assembly containing migration classes.</param>
        /// <param name="options">Optional DbContext configuration for interceptors.</param>
        public SqliteMigrationRunner(DbConnection connection, Assembly migrationsAssembly, DbContextOptions? options = null)
        {
            _connection = connection;
            _migrationsAssembly = migrationsAssembly;
            if (options != null && options.CommandInterceptors.Count > 0)
            {
                _context = new DbContext(connection, new SqliteProvider(), options);
            }
        }

        /// <summary>
        /// Applies all pending migrations to the connected SQLite database. The method
        /// wraps migration execution in a transaction ensuring that either all migrations
        /// succeed or none are applied.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public async Task ApplyMigrationsAsync(CancellationToken ct = default)
        {
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
        /// Determines whether there are migrations in the assembly that have not yet been
        /// applied to the database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if migrations are pending; otherwise, <c>false</c>.</returns>
        public async Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default)
        {
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Any();
        }

        /// <summary>
        /// Retrieves the identifiers of migrations that have not yet been applied to the
        /// database in the format "<c>Version_Name</c>".
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>An array of pending migration identifiers.</returns>
        public async Task<string[]> GetPendingMigrationsAsync(CancellationToken ct = default)
        {
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Select(p => $"{p.Version}_{p.Name}").ToArray();
        }

        /// <summary>
        /// Scans the migrations assembly and returns migrations that have not yet been applied to the
        /// target SQLite database.
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

            var appliedVersions = await GetAppliedMigrationVersionsAsync(ct).ConfigureAwait(false);

            return all.Where(m => !appliedVersions.Contains(m.Version)).ToList();
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
        /// Retrieves the set of migration versions that have already been applied to the database.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A set containing the version numbers of applied migrations.</returns>
        private async Task<HashSet<long>> GetAppliedMigrationVersionsAsync(CancellationToken ct)
        {
            var versions = new HashSet<long>();
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT \"Version\" FROM \"{HistoryTableName}\"";
            try
            {
                await using var reader = await ExecuteReaderAsync(cmd, ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    versions.Add(reader.GetInt64(0));
                }
            }
            catch (DbException)
            {
                // History table probably doesn't exist yet, return empty set.
            }
            return versions;
        }

        /// <summary>
        /// Creates the migration history table if it does not already exist.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        private async Task EnsureHistoryTableAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE IF NOT EXISTS \"{HistoryTableName}\" (Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
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
        /// Executes the specified command expecting a result set and returns the corresponding
        /// <see cref="DbDataReader"/>. When the runner was created with <see cref="DbContextOptions"/>
        /// containing command interceptors, the command is executed through the interception
        /// pipeline so that logging, diagnostics and other cross‑cutting concerns are honored.
        /// Otherwise the command is executed directly against the underlying connection.
        /// </summary>
        /// <param name="cmd">The fully prepared <see cref="DbCommand"/> to execute.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task whose result is the <see cref="DbDataReader"/> produced by the command.</returns>
        private Task<DbDataReader> ExecuteReaderAsync(DbCommand cmd, CancellationToken ct)
            => _context != null
                ? cmd.ExecuteReaderWithInterceptionAsync(_context, CommandBehavior.Default, ct)
                : cmd.ExecuteReaderAsync(ct);
    }
}
