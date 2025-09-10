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
    public class SqlServerMigrationRunner : IMigrationRunner
    {
        private readonly DbConnection _connection;
        private readonly Assembly _migrationsAssembly;
        private readonly DbContext? _context;
        internal const string HistoryTableName = "__NormMigrationsHistory";

        public SqlServerMigrationRunner(DbConnection connection, Assembly migrationsAssembly, DbContextOptions? options = null)
        {
            _connection = connection;
            _migrationsAssembly = migrationsAssembly;
            if (options != null && options.CommandInterceptors.Count > 0)
            {
                _context = new DbContext(connection, new SqlServerProvider(), options);
            }
        }

        /// <summary>
        /// Applies all pending migrations to the SQL Server database.
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
        /// Determines whether there are migrations that have not yet been applied.
        /// </summary>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns><c>true</c> if pending migrations exist; otherwise <c>false</c>.</returns>
        public async Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default)
        {
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
            await EnsureHistoryTableAsync(ct).ConfigureAwait(false);
            var pending = await GetPendingMigrationsInternalAsync(ct).ConfigureAwait(false);
            return pending.Select(p => $"{p.Version}_{p.Name}").ToArray();
        }

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

        private async Task<HashSet<long>> GetAppliedMigrationVersionsAsync(CancellationToken ct)
        {
            var versions = new HashSet<long>();
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT [Version] FROM [{HistoryTableName}]";
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

        private async Task EnsureHistoryTableAsync(CancellationToken ct)
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID(N'{HistoryTableName}', N'U') IS NULL CREATE TABLE [{HistoryTableName}] (Version BIGINT PRIMARY KEY, Name NVARCHAR(255) NOT NULL, AppliedOn DATETIME2 NOT NULL);";
            await ExecuteNonQueryAsync(cmd, ct).ConfigureAwait(false);
        }

        private Task<int> ExecuteNonQueryAsync(DbCommand cmd, CancellationToken ct)
            => _context != null ? cmd.ExecuteNonQueryWithInterceptionAsync(_context, ct) : cmd.ExecuteNonQueryAsync(ct);

        private Task<DbDataReader> ExecuteReaderAsync(DbCommand cmd, CancellationToken ct)
            => _context != null ? cmd.ExecuteReaderWithInterceptionAsync(_context, CommandBehavior.Default, ct) : cmd.ExecuteReaderAsync(ct);
    }
}