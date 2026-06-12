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
                return new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlServer(provider))
            {
                return await QueryColumnStoreTypeMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                           t.name AS TableName,
                           c.name AS ColumnName,
                           COALESCE(base_ty.name, ty.name) AS StoreType
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    LEFT JOIN sys.types base_ty
                      ON ty.is_user_defined = 1
                     AND base_ty.user_type_id = ty.system_type_id
                     AND base_ty.is_user_defined = 0
                    WHERE t.is_ms_shipped = 0
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                return await QueryColumnStoreTypeMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema,
                           table_name AS TableName,
                           column_name AS ColumnName,
                           data_type AS StoreType
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsMySql(provider))
            {
                return await QueryColumnStoreTypeMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema,
                           table_name AS TableName,
                           column_name AS ColumnName,
                           data_type AS StoreType
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsSqlite(provider))
            {
                var result = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var name = Convert.ToString(reader["name"]);
                        var type = Convert.ToString(reader["type"]);
                        if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(type))
                            columns[name!] = type!;
                    }

                    result[TableKey(table.Schema, table.Name)] = columns;
                }

                return result;
            }

            return new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> QueryColumnStoreTypeMapAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                var columnName = Convert.ToString(reader["ColumnName"]);
                var storeType = Convert.ToString(reader["StoreType"]);
                if (string.IsNullOrWhiteSpace(tableName)
                    || string.IsNullOrWhiteSpace(columnName)
                    || string.IsNullOrWhiteSpace(storeType))
                {
                    continue;
                }

                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName!);
                if (!tableKeys.Contains(tableKey))
                    continue;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns[columnName!] = storeType!;
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }
    }
}
