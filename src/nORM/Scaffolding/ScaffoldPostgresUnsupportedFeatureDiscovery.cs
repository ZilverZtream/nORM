using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static class ScaffoldPostgresUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            await AddDefaultColumnFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddComputedColumnFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddTriggerFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddCheckConstraintFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddCollationFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddProviderSpecificColumnFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddPrecisionScaleFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddPartialIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddExpressionIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddIncludedColumnIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddDescendingIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddProviderSpecificIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            return features;
        }

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

        private static Task AddDefaultColumnFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Default' AS Kind, column_default::text AS Detail
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND column_default IS NOT NULL
                  AND is_identity <> 'YES'
                  AND column_default NOT LIKE 'nextval(%'
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

        private static Task AddCollationFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Collation' AS Kind, collation_name::text AS Detail
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND collation_name IS NOT NULL
                """);

        private static Task AddProviderSpecificColumnFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'ProviderSpecificColumnType' AS Kind,
                    (CASE
                        WHEN domain_name IS NOT NULL AND domain_name <> ''
                             AND data_type = 'USER-DEFINED'
                             AND EXISTS (
                                 SELECT 1
                                 FROM pg_type enum_type
                                 INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                                 WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                                   AND enum_type.typname = udt_name
                                   AND enum_type.typtype = 'e'
                             )
                        THEN 'DOMAIN (' ||
                             CASE WHEN domain_schema IS NOT NULL AND domain_schema <> '' THEN domain_schema || '.' ELSE '' END ||
                             domain_name || ' -> ENUM (' ||
                             CASE WHEN udt_schema IS NOT NULL AND udt_schema <> '' THEN udt_schema || '.' ELSE '' END ||
                             udt_name || ': ' ||
                             COALESCE((
                                 SELECT string_agg(quote_literal(enum_value.enumlabel), ',' ORDER BY enum_value.enumsortorder)
                                 FROM pg_type enum_type
                                 INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                                 INNER JOIN pg_enum enum_value ON enum_value.enumtypid = enum_type.oid
                                 WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                                   AND enum_type.typname = udt_name
                                   AND enum_type.typtype = 'e'
                             ), '') ||
                             '))'
                        WHEN domain_name IS NOT NULL AND domain_name <> ''
                        THEN 'DOMAIN (' ||
                             CASE WHEN domain_schema IS NOT NULL AND domain_schema <> '' THEN domain_schema || '.' ELSE '' END ||
                             domain_name || ' -> ' ||
                             CASE
                                 WHEN data_type IN ('ARRAY', 'USER-DEFINED') AND udt_name IS NOT NULL AND udt_name <> ''
                                 THEN data_type || ' (' || udt_name || ')'
                                 ELSE data_type
                             END ||
                             CASE
                                 WHEN data_type IN ('character varying', 'character') AND character_maximum_length IS NOT NULL
                                 THEN '(' || character_maximum_length::text || ')'
                                 WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL AND numeric_scale IS NULL
                                 THEN '(' || numeric_precision::text || ')'
                                 WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL
                                 THEN '(' || numeric_precision::text || ',' || numeric_scale::text || ')'
                                 ELSE ''
                             END ||
                             ')'
                        WHEN data_type = 'USER-DEFINED'
                             AND EXISTS (
                                 SELECT 1
                                 FROM pg_type enum_type
                                 INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                                 WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                                   AND enum_type.typname = udt_name
                                   AND enum_type.typtype = 'e'
                             )
                        THEN 'ENUM (' ||
                             CASE WHEN udt_schema IS NOT NULL AND udt_schema <> '' THEN udt_schema || '.' ELSE '' END ||
                             udt_name || ': ' ||
                             COALESCE((
                                 SELECT string_agg(quote_literal(enum_value.enumlabel), ',' ORDER BY enum_value.enumsortorder)
                                 FROM pg_type enum_type
                                 INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                                 INNER JOIN pg_enum enum_value ON enum_value.enumtypid = enum_type.oid
                                 WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                                   AND enum_type.typname = udt_name
                                   AND enum_type.typtype = 'e'
                             ), '') ||
                             ')'
                        WHEN udt_name IS NULL OR udt_name = '' THEN data_type
                        ELSE data_type || ' (' || udt_name || ')'
                    END)::text AS Detail
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND (
                      domain_name IS NOT NULL
                      OR
                      data_type IN ('ARRAY', 'USER-DEFINED', 'json', 'jsonb', 'xml')
                      OR udt_name IN ('json', 'jsonb', 'inet', 'cidr', 'macaddr', 'macaddr8', 'tsvector', 'tsquery')
                  )
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

        private static Task AddPartialIndexFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'PartialIndex' AS Kind, 'PostgreSQL partial index'::text AS Detail
                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                WHERE ix.indisprimary = false
                  AND ix.indpred IS NOT NULL
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                """);

        private static Task AddExpressionIndexFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'ExpressionIndex' AS Kind, pg_get_indexdef(ix.indexrelid)::text AS Detail
                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                WHERE ix.indisprimary = false
                  AND ix.indexprs IS NOT NULL
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                """);

        private static Task AddIncludedColumnIndexFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'IncludedColumnIndex' AS Kind, pg_get_indexdef(ix.indexrelid)::text AS Detail
                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                WHERE ix.indisprimary = false
                  AND ix.indnatts <> ix.indnkeyatts
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                """);

        private static Task AddDescendingIndexFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'DescendingIndex' AS Kind, pg_get_indexdef(ix.indexrelid)::text AS Detail
                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                WHERE ix.indisprimary = false
                  AND EXISTS (
                      SELECT 1
                      FROM unnest(ix.indkey) WITH ORDINALITY AS key(attnum, ord)
                      WHERE key.ord <= ix.indnkeyatts
                        AND (ix.indoption[key.ord - 1] & 1) = 1
                  )
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                """);

        private static async Task AddProviderSpecificIndexFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
        {
            await AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'ProviderSpecificIndex' AS Kind,
                    ('PostgreSQL provider-specific index; accessMethod=' || am.amname ||
                     '; indexSql=' || pg_get_indexdef(ix.indexrelid))::text AS Detail
                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                INNER JOIN pg_am am ON am.oid = idx.relam
                WHERE ix.indisprimary = false
                  AND am.amname <> 'btree'
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                """).ConfigureAwait(false);

            await AddFeaturesAsync(connection, features, tableKeys, """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'ProviderSpecificIndex' AS Kind,
                    ('PostgreSQL btree index with provider-specific key options' ||
                     '; accessMethod=btree' ||
                     '; hasNullsNotDistinct=' ||
                     CASE WHEN COALESCE((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false) THEN 'true' ELSE 'false' END ||
                     '; hasNonDefaultOperatorClass=' ||
                     CASE WHEN EXISTS (
                         SELECT 1
                         FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                         INNER JOIN pg_opclass option_opclass
                             ON option_opclass.oid = ix.indclass[option_key.ord - 1]
                         WHERE option_key.ord <= ix.indnkeyatts
                           AND option_opclass.opcdefault = false
                     ) THEN 'true' ELSE 'false' END ||
                     '; hasIndexCollation=' ||
                     CASE WHEN EXISTS (
                         SELECT 1
                         FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                         INNER JOIN pg_attribute option_att
                             ON option_att.attrelid = tbl.oid
                            AND option_att.attnum = option_key.attnum
                         WHERE option_key.ord <= ix.indnkeyatts
                           AND ix.indcollation[option_key.ord - 1] <> 0
                           AND ix.indcollation[option_key.ord - 1] <> option_att.attcollation
                     ) THEN 'true' ELSE 'false' END ||
                     '; hasNonDefaultNullOrdering=' ||
                     CASE WHEN EXISTS (
                         SELECT 1
                         FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                         WHERE option_key.ord <= ix.indnkeyatts
                           AND (
                               ((ix.indoption[option_key.ord - 1] & 1) = 0 AND (ix.indoption[option_key.ord - 1] & 2) = 2)
                               OR ((ix.indoption[option_key.ord - 1] & 1) = 1 AND (ix.indoption[option_key.ord - 1] & 2) = 0)
                           )
                     ) THEN 'true' ELSE 'false' END ||
                     '; indexSql=' || pg_get_indexdef(ix.indexrelid))::text AS Detail
                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                INNER JOIN pg_am am ON am.oid = idx.relam
                WHERE ix.indisprimary = false
                  AND am.amname = 'btree'
                  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND (
                      (
                          COALESCE((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false) = true
                          AND ix.indexprs IS NOT NULL
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                          LEFT JOIN pg_attribute option_att
                              ON option_att.attrelid = tbl.oid
                             AND option_att.attnum = option_key.attnum
                          INNER JOIN pg_opclass option_opclass
                              ON option_opclass.oid = ix.indclass[option_key.ord - 1]
                          WHERE option_key.ord <= ix.indnkeyatts
                            AND (
                                option_opclass.opcdefault = false
                                OR (
                                    option_att.attnum IS NOT NULL
                                    AND ix.indcollation[option_key.ord - 1] <> 0
                                    AND ix.indcollation[option_key.ord - 1] <> option_att.attcollation
                                )
                            )
                      )
                      OR (
                          ix.indexprs IS NOT NULL
                          AND EXISTS (
                              SELECT 1
                              FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                              WHERE option_key.ord <= ix.indnkeyatts
                                AND (
                                    ((ix.indoption[option_key.ord - 1] & 1) = 0 AND (ix.indoption[option_key.ord - 1] & 2) = 2)
                                    OR ((ix.indoption[option_key.ord - 1] & 1) = 1 AND (ix.indoption[option_key.ord - 1] & 2) = 0)
                                )
                          )
                      )
                  )
                """).ConfigureAwait(false);
        }

        private static async Task AddFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys,
            string sql)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                features.Add(new ScaffoldUnsupportedFeatureInfo(
                    tableKey,
                    Convert.ToString(reader["Kind"]) ?? string.Empty,
                    Convert.ToString(reader["ObjectName"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty));
            }
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
