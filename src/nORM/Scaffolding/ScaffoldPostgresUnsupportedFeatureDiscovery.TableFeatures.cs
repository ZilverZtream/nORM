using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresUnsupportedFeatureDiscovery
    {
        private static Task AddTriggerFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT event_object_schema AS TableSchema, event_object_table AS TableName, trigger_name AS ObjectName, 'Trigger' AS Kind,
                    ('PostgreSQL trigger; timing=' || action_timing || '; event=' || event_manipulation || '; orientation=' || action_orientation)::text AS Detail
                FROM information_schema.triggers
                WHERE event_object_schema NOT IN ('pg_catalog', 'information_schema')
                """);

        private static Task AddCheckConstraintFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, con.conname AS ObjectName, 'CheckConstraint' AS Kind, pg_get_constraintdef(con.oid)::text AS Detail
                FROM pg_constraint con
                INNER JOIN pg_class tbl ON tbl.oid = con.conrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                WHERE con.contype = 'c'
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                """);

        private static Task MarkDefaultNamedCheckConstraintFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => ScaffoldSyntheticFeatureNameMarker.MarkFeaturesAsync(
                connection,
                features,
                tableKeys,
                """
                SELECT ns.nspname AS TableSchema,
                       tbl.relname AS TableName,
                       con.conname AS ConstraintName
                FROM pg_constraint con
                INNER JOIN pg_class tbl ON tbl.oid = con.conrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                WHERE con.contype = 'c'
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND con.conname = LEFT(
                      tbl.relname || '_' ||
                      CASE
                          WHEN array_length(con.conkey, 1) > 0
                          THEN (
                              SELECT string_agg(att.attname, '_' ORDER BY key.ord) || '_'
                              FROM unnest(con.conkey) WITH ORDINALITY AS key(attnum, ord)
                              INNER JOIN pg_attribute att
                                  ON att.attrelid = tbl.oid
                                 AND att.attnum = key.attnum
                          )
                          ELSE ''
                      END || 'check',
                      63)
                """,
                "CheckConstraint");
    }
}
