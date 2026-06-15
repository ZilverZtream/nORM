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
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqlitePrimaryKeyColumnNameMapAsync(connection, provider, tables).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, SqlServerPrimaryKeyColumnSql).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, PostgresPrimaryKeyColumnSql).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, MySqlPrimaryKeyColumnSql).ConfigureAwait(false);

            return await GetProviderSchemaPrimaryKeyColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
        }

        public static async Task<IReadOnlyDictionary<string, string>> GetPrimaryKeyConstraintNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqlitePrimaryKeyConstraintNameMapAsync(connection, provider, tables).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, SqlServerPrimaryKeyConstraintNameSql).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, PostgresPrimaryKeyConstraintNameSql).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await QueryPrimaryKeyConstraintNameMapAsync(connection, tableKeys, MySqlPrimaryKeyConstraintNameSql).ConfigureAwait(false);

            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
