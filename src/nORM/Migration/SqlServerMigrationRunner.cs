using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;

#nullable enable

namespace nORM.Migration
{
    public class SqlServerMigrationRunner : IMigrationRunner
    {
        private readonly DbConnection _connection;
        private readonly Assembly _migrationsAssembly;
        private const string HistoryTableName = "__NormMigrationsHistory";

        public SqlServerMigrationRunner(DbConnection connection, Assembly migrationsAssembly)
        {
            _connection = connection;
            _migrationsAssembly = migrationsAssembly;
        }

        public async Task ApplyMigrationsAsync(CancellationToken ct = default)
        {
            await EnsureHistoryTableAsync(ct);
            var pending = await GetPendingMigrationsInternalAsync(ct);
            if (!pending.Any()) return;

            await using var transaction = await _connection.BeginTransactionAsync(ct);
            foreach (var migration in pending)
            {
                migration.Up(_connection, (DbTransaction)transaction);
                await MarkMigrationAppliedAsync(migration, (DbTransaction)transaction, ct);
            }
            await transaction.CommitAsync(ct);
        }

        public async Task<bool> HasPendingMigrationsAsync(CancellationToken ct = default)
        {
            await EnsureHistoryTableAsync(ct);
            var pending = await GetPendingMigrationsInternalAsync(ct);
            return pending.Any();
        }

        public async Task<string[]> GetPendingMigrationsAsync(CancellationToken ct = default)
        {
            await EnsureHistoryTableAsync(ct);
            var pending = await GetPendingMigrationsInternalAsync(ct);
            return pending.Select(p => $"{p.Version}_{p.Name}").ToArray();
        }

        private async Task<List<Migration>> GetPendingMigrationsInternalAsync(CancellationToken ct)
        {
            var all = _migrationsAssembly.GetTypes()
                .Where(t => typeof(Migration).IsAssignableFrom(t) && !t.IsAbstract)
                .Select(t => (Migration)Activator.CreateInstance(t)!)
                .OrderBy(m => m.Version)
                .ToList();

            var appliedVersions = await GetAppliedMigrationVersionsAsync(ct);

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
            await cmd.ExecuteNonQueryAsync(ct);
        }

        private async Task<HashSet<long>> GetAppliedMigrationVersionsAsync(CancellationToken ct)
        {
            var versions = new HashSet<long>();
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT [Version] FROM [{HistoryTableName}]";
            try
            {
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
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
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}