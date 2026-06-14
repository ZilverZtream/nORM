namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
        private const string MySqlColumnFeatureSql = """
            SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Default' AS Kind,
                CASE
                    WHEN LOWER(COALESCE(extra, '')) LIKE '%on update%'
                    THEN TRIM(CONCAT(COALESCE(column_default, ''), ' ', extra))
                    WHEN data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set')
                         AND (LOWER(column_default) LIKE 'lower(%' OR LOWER(column_default) LIKE 'upper(%')
                    THEN REPLACE(column_default, CHAR(92, 39), CHAR(39))
                    WHEN data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set')
                    THEN QUOTE(column_default)
                    WHEN data_type IN ('date', 'datetime', 'timestamp', 'time')
                         AND LOWER(column_default) NOT LIKE 'current_timestamp%'
                         AND LOWER(column_default) NOT LIKE 'current_time%'
                         AND LOWER(column_default) NOT LIKE 'localtime%'
                         AND LOWER(column_default) NOT LIKE 'localtimestamp%'
                         AND LOWER(column_default) NOT LIKE 'now(%'
                         AND LOWER(column_default) NOT LIKE 'utc_timestamp(%'
                         AND LOWER(column_default) NOT LIKE 'sysdate(%'
                         AND LOWER(column_default) NOT LIKE 'current_date%'
                         AND column_default NOT LIKE '%()'
                    THEN QUOTE(column_default)
                    ELSE column_default
                END AS Detail
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND (
                  column_default IS NOT NULL
                  OR LOWER(COALESCE(extra, '')) LIKE '%on update%'
              )
            UNION ALL
            SELECT NULL, table_name, column_name, 'Computed',
                CONCAT(
                    generation_expression,
                    CASE
                        WHEN LOWER(COALESCE(extra, '')) LIKE '%stored generated%' THEN ' STORED'
                        WHEN LOWER(COALESCE(extra, '')) LIKE '%virtual generated%' THEN ' VIRTUAL'
                        ELSE ''
                    END)
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND generation_expression IS NOT NULL AND generation_expression <> ''
            UNION ALL
            SELECT NULL, c.table_name, c.column_name, 'Collation', c.collation_name
            FROM information_schema.columns c
            INNER JOIN information_schema.schemata s ON s.schema_name = c.table_schema
            WHERE c.table_schema = DATABASE()
              AND c.collation_name IS NOT NULL
              AND c.collation_name <> s.default_collation_name
            UNION ALL
            SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType',
                CASE
                    WHEN data_type IN ('enum', 'set')
                         AND column_type IS NOT NULL
                         AND column_type <> ''
                    THEN column_type
                    ELSE data_type
                END
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND data_type IN (
                  'json',
                  'geometry',
                  'point',
                  'linestring',
                  'polygon',
                  'multipoint',
                  'multilinestring',
                  'multipolygon',
                  'geometrycollection',
                  'enum',
                  'set',
                  'year'
              )
            UNION ALL
            SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType', column_type
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND LOWER(COALESCE(column_type, '')) LIKE '%unsigned%'
              AND data_type IN ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint', 'decimal', 'numeric')
            UNION ALL
            SELECT NULL, table_name, column_name, 'PrecisionScale',
                CASE
                    WHEN numeric_scale IS NULL THEN CONCAT(data_type, '(', numeric_precision, ')')
                    ELSE CONCAT(data_type, '(', numeric_precision, ',', numeric_scale, ')')
                END
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND data_type IN ('decimal', 'numeric')
              AND numeric_precision IS NOT NULL
            """;
    }
}
