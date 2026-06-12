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

        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetSqlServerStringBinaryFacetsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnFacetMapAsync(connection, tableKeys, """
                SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                       t.name AS TableName,
                       c.name AS ColumnName,
                       CASE
                           WHEN COALESCE(base_ty.name, ty.name) IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN CONVERT(int, c.max_length / 2)
                           WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length > 0 THEN CONVERT(int, c.max_length)
                           ELSE NULL
                       END AS MaxLength,
                       CASE
                           WHEN COALESCE(base_ty.name, ty.name) IN ('nchar', 'nvarchar') THEN CONVERT(int, 1)
                           WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'varchar') THEN CONVERT(int, 0)
                           ELSE NULL
                       END AS IsUnicode,
                       CASE
                           WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'nchar', 'binary') THEN CONVERT(int, 1)
                           ELSE CONVERT(int, 0)
                       END AS IsFixedLength
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                LEFT JOIN sys.types base_ty
                  ON ty.is_user_defined = 1
                 AND base_ty.user_type_id = ty.system_type_id
                 AND base_ty.is_user_defined = 0
                WHERE t.is_ms_shipped = 0
                  AND COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary')
                """);

        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetPostgresStringBinaryFacetsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnFacetMapAsync(connection, tableKeys, """
                SELECT table_schema AS TableSchema,
                       table_name AS TableName,
                       column_name AS ColumnName,
                       character_maximum_length::int AS MaxLength,
                       NULL::int AS IsUnicode,
                       CASE WHEN data_type = 'character' OR udt_name = 'bpchar' THEN 1 ELSE 0 END AS IsFixedLength
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND (data_type IN ('character varying', 'character') OR udt_name IN ('varchar', 'bpchar'))
                """);

        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetMySqlStringBinaryFacetsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnFacetMapAsync(connection, tableKeys, """
                SELECT NULL AS TableSchema,
                       table_name AS TableName,
                       column_name AS ColumnName,
                       character_maximum_length AS MaxLength,
                       NULL AS IsUnicode,
                       CASE WHEN data_type IN ('char', 'binary') THEN 1 ELSE 0 END AS IsFixedLength
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND data_type IN ('char', 'varchar', 'binary', 'varbinary')
                """);

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetSqliteDeclaredStringBinaryFacetsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var result = new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                var columns = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var name = Convert.ToString(reader["name"]);
                    var declaredType = Convert.ToString(reader["type"]);
                    if (!string.IsNullOrWhiteSpace(name)
                        && ScaffoldSqliteDdlParser.TryParseDeclaredStringBinaryFacet(declaredType, out var facet))
                    {
                        columns[name!] = facet;
                    }
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
        }
    }
}
