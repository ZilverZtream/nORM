#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldSkippedObjectInfo(string? Schema, string Name, string Kind, string Detail, string? Comment)
    {
        public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
    }

    internal static class ScaffoldSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
                return await ScaffoldSqliteSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await ScaffoldSqlServerSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await ScaffoldPostgresSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await ScaffoldMySqlSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            return Array.Empty<ScaffoldSkippedObjectInfo>();
        }

        public static Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
            => ScaffoldSqliteSkippedObjectDiscovery.GetSqliteSchemasAsync(connection);

        public static string SqliteSchemaResult(string schema)
            => ScaffoldSqliteSkippedObjectDiscovery.SqliteSchemaResult(schema);
    }
}
