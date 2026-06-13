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
        public static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetColumnStoreTypesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(table => TableKey(table.Schema, table.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (tableKeys.Count == 0)
                return EmptyColumnStoreTypeMap();

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await GetSqlServerColumnStoreTypesAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await GetPostgresColumnStoreTypesAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlColumnStoreTypesAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqliteColumnStoreTypesAsync(connection, provider, tables).ConfigureAwait(false);

            return EmptyColumnStoreTypeMap();
        }

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> EmptyColumnStoreTypeMap()
            => new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
    }
}
