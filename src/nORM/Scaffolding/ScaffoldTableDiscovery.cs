#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldTableInfo>> GetTablesAsync(
            DbConnection connection,
            DatabaseProvider provider)
        {
            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqliteTablesAsync(connection, provider).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await GetSqlServerTablesAsync(connection).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await GetPostgresTablesAsync(connection).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlTablesAsync(connection).ConfigureAwait(false);

            return await GetSchemaTablesAsync(connection).ConfigureAwait(false);
        }
    }
}
