using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetEnumColumnFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT c.table_schema AS TableSchema,
                       c.table_name AS TableName,
                       c.column_name AS ColumnName,
                       'ENUM (' ||
                       CASE WHEN c.udt_schema IS NOT NULL AND c.udt_schema <> '' THEN c.udt_schema || '.' ELSE '' END ||
                       c.udt_name || ': ' ||
                       string_agg(quote_literal(e.enumlabel), ',' ORDER BY e.enumsortorder) ||
                       ')' AS Detail
                FROM information_schema.columns c
                INNER JOIN pg_namespace ns ON ns.nspname = COALESCE(c.udt_schema, c.table_schema)
                INNER JOIN pg_type t ON t.typnamespace = ns.oid AND t.typname = c.udt_name AND t.typtype = 'e'
                INNER JOIN pg_enum e ON e.enumtypid = t.oid
                WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND c.data_type = 'USER-DEFINED'
                  AND c.domain_name IS NULL
                GROUP BY c.table_schema, c.table_name, c.column_name, c.udt_schema, c.udt_name
                """;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(
                    NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])),
                    Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                features.Add(new ScaffoldUnsupportedFeatureInfo(
                    tableKey,
                    "ProviderSpecificColumnType",
                    Convert.ToString(reader["ColumnName"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty));
            }

            return features;
        }
    }
}
