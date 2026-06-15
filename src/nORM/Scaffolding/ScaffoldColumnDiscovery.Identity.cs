#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlite(provider))
            {
                var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        rows.Add((
                            Convert.ToString(reader["name"]) ?? string.Empty,
                            Convert.ToString(reader["type"]) ?? string.Empty,
                            ReaderHasColumn(reader, "pk")
                                ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                                : 0));
                    }

                    var primaryKeyColumns = rows.Where(row => row.PrimaryKeyOrdinal > 0).ToArray();
                    if (primaryKeyColumns.Length != 1)
                        continue;

                    var key = primaryKeyColumns[0];
                    if (key.Type.Contains("INT", StringComparison.OrdinalIgnoreCase))
                    {
                        var tableKey = TableKey(table.Schema, table.Name);
                        result[tableKey] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { key.Name };
                    }
                }

                return ToReadOnlySetDictionary(result);
            }

            if (ScaffoldProviderKind.IsSqlServer(provider))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName
                    FROM sys.identity_columns ic
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    INNER JOIN sys.tables t ON t.object_id = ic.object_id
                    WHERE t.is_ms_shipped = 0
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns c
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (
                          is_identity = 'YES'
                          OR (
                              column_default LIKE 'nextval(%'
                              AND pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL
                          )
                      )
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsMySql(provider))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND LOWER(extra) LIKE '%auto_increment%'
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
