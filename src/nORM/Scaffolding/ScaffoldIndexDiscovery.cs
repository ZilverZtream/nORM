#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
                return await ScaffoldSqliteIndexDiscovery.GetIndexesAsync(connection, provider, tables).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await QueryIndexesAsync(connection, SqlServerIndexSql).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await QueryIndexesAsync(connection, PostgresIndexSql).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await GetMySqlIndexesAsync(connection, provider, tables).ConfigureAwait(false);

            return Array.Empty<ScaffoldIndexInfo>();
        }
    }
}
