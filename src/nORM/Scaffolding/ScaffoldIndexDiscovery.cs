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
            if (ScaffoldProviderKind.IsSqlite(provider))
                return await ScaffoldSqliteIndexDiscovery.GetIndexesAsync(connection, provider, tables).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await QueryIndexesAsync(connection, SqlServerIndexSql, tables).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await QueryIndexesAsync(connection, PostgresIndexSql, tables).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlIndexesAsync(connection, provider, tables).ConfigureAwait(false);

            return Array.Empty<ScaffoldIndexInfo>();
        }
    }
}
