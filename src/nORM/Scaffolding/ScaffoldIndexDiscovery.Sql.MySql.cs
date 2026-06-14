#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        private const string MySqlIndexSql = """
            SELECT
                NULL AS TableSchema,
                s.table_name AS TableName,
                s.column_name AS ColumnName,
                s.index_name AS IndexName,
                CASE WHEN s.non_unique = 0 THEN 1 ELSE 0 END AS IsUnique,
                COUNT(*) OVER (PARTITION BY s.table_schema, s.table_name, s.index_name) AS ColumnCount,
                s.seq_in_index - 1 AS Ordinal,
                CASE WHEN UPPER(COALESCE(s.collation, 'A')) = 'D' THEN 1 ELSE 0 END AS IsDescending,
                CASE
                    WHEN s.non_unique = 0
                     AND (
                         LOWER(s.index_name) = LOWER(FIRST_VALUE(s.column_name) OVER (PARTITION BY s.table_schema, s.table_name, s.index_name ORDER BY s.seq_in_index))
                         OR LOWER(s.index_name) = LOWER(CONCAT(FIRST_VALUE(s.column_name) OVER (PARTITION BY s.table_schema, s.table_name, s.index_name ORDER BY s.seq_in_index), '_UNIQUE'))
                     )
                    THEN 1 ELSE 0
                END AS IsSyntheticName
            FROM information_schema.statistics s
            INNER JOIN information_schema.columns c
                ON c.table_schema = s.table_schema
               AND c.table_name = s.table_name
               AND c.column_name = s.column_name
            WHERE s.table_schema = DATABASE()
              AND s.index_name <> 'PRIMARY'
              AND UPPER(COALESCE(NULLIF(s.index_type, ''), 'BTREE')) = 'BTREE'
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.statistics bad
                  INNER JOIN information_schema.columns bad_col
                      ON bad_col.table_schema = bad.table_schema
                     AND bad_col.table_name = bad.table_name
                     AND bad_col.column_name = bad.column_name
                  WHERE bad.table_schema = s.table_schema
                    AND bad.table_name = s.table_name
                    AND bad.index_name = s.index_name
                    AND bad.sub_part IS NOT NULL
                    AND (
                        bad_col.character_maximum_length IS NULL
                        OR bad.sub_part < bad_col.character_maximum_length
                    )
              )
            ORDER BY s.table_schema, s.table_name, s.index_name, s.seq_in_index
            """;
    }
}
