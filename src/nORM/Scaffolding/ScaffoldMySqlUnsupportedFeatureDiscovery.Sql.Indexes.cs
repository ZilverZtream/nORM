namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
        private const string MySqlIndexFeatureSql = """
            SELECT DISTINCT NULL AS TableSchema, table_name AS TableName, index_name AS ObjectName, 'DescendingIndex' AS Kind, 'MySQL descending index key' AS Detail
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND index_name <> 'PRIMARY'
              AND collation = 'D'
            UNION ALL
            SELECT NULL, s.table_name, s.index_name, 'PrefixIndex',
                CONCAT(
                    'MySQL prefix index; prefixColumns=',
                    GROUP_CONCAT(
                        CONCAT(
                            s.column_name,
                            ':',
                            s.sub_part,
                            '/',
                            COALESCE(CAST(c.character_maximum_length AS CHAR), '')
                        )
                        ORDER BY s.seq_in_index
                        SEPARATOR ','
                    )
                )
            FROM information_schema.statistics s
            INNER JOIN information_schema.columns c
                ON c.table_schema = s.table_schema
               AND c.table_name = s.table_name
               AND c.column_name = s.column_name
            WHERE s.table_schema = DATABASE()
              AND s.index_name <> 'PRIMARY'
              AND s.sub_part IS NOT NULL
              AND (
                  c.character_maximum_length IS NULL
                  OR s.sub_part < c.character_maximum_length
              )
            GROUP BY s.table_name, s.index_name
            UNION ALL
            SELECT DISTINCT NULL, table_name, index_name, 'ProviderSpecificIndex',
                CONCAT('MySQL provider-specific index; indexType=', index_type)
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND index_name <> 'PRIMARY'
              AND UPPER(COALESCE(NULLIF(index_type, ''), 'BTREE')) <> 'BTREE'
            """;
    }
}
