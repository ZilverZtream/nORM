#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetStringBinaryFacetsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(table => TableKey(table.Schema, table.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (tableKeys.Count == 0)
                return new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await GetSqlServerStringBinaryFacetsAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await GetPostgresStringBinaryFacetsAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlStringBinaryFacetsAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqliteDeclaredStringBinaryFacetsAsync(connection, provider, tables).ConfigureAwait(false);

            return new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
