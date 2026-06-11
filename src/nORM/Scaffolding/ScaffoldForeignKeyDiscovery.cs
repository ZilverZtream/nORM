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
            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqliteForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await QueryForeignKeysAsync(connection, SqlServerForeignKeySql).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await QueryForeignKeysAsync(connection, PostgresForeignKeySql).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await QueryForeignKeysAsync(connection, MySqlForeignKeySql).ConfigureAwait(false);

            return Array.Empty<ScaffoldForeignKeyInfo>();
        }
    }
}
