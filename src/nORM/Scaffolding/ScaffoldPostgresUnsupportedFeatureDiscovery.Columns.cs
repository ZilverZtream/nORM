using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresUnsupportedFeatureDiscovery
    {
        private static Task AddDefaultColumnFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Default' AS Kind, column_default::text AS Detail
                FROM information_schema.columns c
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND column_default IS NOT NULL
                  AND is_identity <> 'YES'
                  AND NOT (
                      column_default LIKE 'nextval(%'
                      AND pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL
                  )
                """);

        private static Task AddComputedColumnFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Computed' AS Kind, (generation_expression || ' STORED')::text AS Detail
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND is_generated <> 'NEVER'
                """);

        private static Task AddCollationFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Collation' AS Kind, collation_name::text AS Detail
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND collation_name IS NOT NULL
                """);

        private static Task AddPrecisionScaleFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'PrecisionScale' AS Kind,
                    CASE
                        WHEN numeric_scale IS NULL THEN ('numeric(' || numeric_precision || ')')::text
                        ELSE ('numeric(' || numeric_precision || ',' || numeric_scale || ')')::text
                    END AS Detail
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND data_type = 'numeric'
                  AND numeric_precision IS NOT NULL
                """);
    }
}
