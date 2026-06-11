#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
                return await GetSqliteForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await QueryForeignKeysAsync(connection, SqlServerForeignKeySql).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await QueryForeignKeysAsync(connection, PostgresForeignKeySql).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await QueryForeignKeysAsync(connection, MySqlForeignKeySql).ConfigureAwait(false);

            return Array.Empty<ScaffoldForeignKeyInfo>();
        }
    }
}
