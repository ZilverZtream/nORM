#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldKeyDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (provider is SqliteProvider)
                return await GetSqlitePrimaryKeyColumnNameMapAsync(connection, provider, tables).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, SqlServerPrimaryKeyColumnSql).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, PostgresPrimaryKeyColumnSql).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, MySqlPrimaryKeyColumnSql).ConfigureAwait(false);

            return await GetProviderSchemaPrimaryKeyColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
        }

        public static async Task<IReadOnlyDictionary<string, string>> GetPrimaryKeyConstraintNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, SqlServerPrimaryKeyConstraintNameSql).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, PostgresPrimaryKeyConstraintNameSql).ConfigureAwait(false);

            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
