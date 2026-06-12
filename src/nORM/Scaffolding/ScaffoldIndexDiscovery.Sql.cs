#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        private const string SqlServerIndexSql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS TableSchema,
                t.name AS TableName,
                c.name AS ColumnName,
                i.name AS IndexName,
                i.is_unique AS IsUnique,
                SUM(CASE WHEN ic.is_included_column = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY i.object_id, i.index_id) AS ColumnCount,
                CASE WHEN ic.is_included_column = 1 THEN 2147483647 ELSE ic.key_ordinal - 1 END AS Ordinal,
                ic.is_descending_key AS IsDescending,
                ic.is_included_column AS IsIncluded,
                CASE WHEN kc.is_system_named = 1 THEN 1 ELSE 0 END AS IsSyntheticName,
                i.filter_definition AS FilterSql
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
            LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id AND kc.type = 'UQ'
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.is_hypothetical = 0
              AND i.type IN (1, 2)
              AND i.name IS NOT NULL
            ORDER BY SCHEMA_NAME(t.schema_id), t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
            """;

        private const string PostgresIndexSql = """
            SELECT
                ns.nspname AS TableSchema,
                tbl.relname AS TableName,
                att.attname AS ColumnName,
                idx.relname AS IndexName,
                ix.indisunique AS IsUnique,
                ix.indnkeyatts AS ColumnCount,
                CASE WHEN key.ord > ix.indnkeyatts THEN 2147483647 ELSE key.ord - 1 END AS Ordinal,
                CASE WHEN key.ord <= ix.indnkeyatts AND (ix.indoption[key.ord - 1] & 1) = 1 THEN 1 ELSE 0 END AS IsDescending,
                CASE WHEN key.ord > ix.indnkeyatts THEN 1 ELSE 0 END AS IsIncluded,
                CASE
                    WHEN unique_constraint.conname = LEFT(
                        tbl.relname || '_' ||
                        (
                            SELECT string_agg(unique_att.attname, '_' ORDER BY unique_key.ord)
                            FROM unnest(unique_constraint.conkey) WITH ORDINALITY AS unique_key(attnum, ord)
                            INNER JOIN pg_attribute unique_att
                                ON unique_att.attrelid = tbl.oid
                               AND unique_att.attnum = unique_key.attnum
                        ) || '_key',
                        63)
                    THEN true ELSE false
                END AS IsSyntheticName,
                CASE
                    WHEN key.ord <= ix.indnkeyatts
                     AND (ix.indoption[key.ord - 1] & 1) = 0
                     AND (ix.indoption[key.ord - 1] & 2) = 2 THEN 'First'
                    WHEN key.ord <= ix.indnkeyatts
                     AND (ix.indoption[key.ord - 1] & 1) = 1
                     AND (ix.indoption[key.ord - 1] & 2) = 0 THEN 'Last'
                    ELSE NULL
                END AS NullSortOrder,
                COALESCE((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false) AS NullsNotDistinct,
                CASE WHEN ix.indpred IS NULL THEN NULL ELSE pg_get_expr(ix.indpred, ix.indrelid) END AS FilterSql
            FROM pg_index ix
            INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
            INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
            INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            INNER JOIN pg_am am ON am.oid = idx.relam
            INNER JOIN unnest(ix.indkey) WITH ORDINALITY AS key(attnum, ord) ON true
            INNER JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = key.attnum
            LEFT JOIN pg_constraint unique_constraint ON unique_constraint.conindid = ix.indexrelid AND unique_constraint.contype = 'u'
            WHERE ix.indisprimary = false
              AND ix.indexprs IS NULL
              AND am.amname = 'btree'
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                  SELECT 1
                  FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                  INNER JOIN pg_attribute option_att
                      ON option_att.attrelid = tbl.oid
                     AND option_att.attnum = option_key.attnum
                  INNER JOIN pg_opclass option_opclass
                      ON option_opclass.oid = ix.indclass[option_key.ord - 1]
                  WHERE option_key.ord <= ix.indnkeyatts
                    AND (
                        option_opclass.opcdefault = false
                        OR (
                            ix.indcollation[option_key.ord - 1] <> 0
                            AND ix.indcollation[option_key.ord - 1] <> option_att.attcollation
                        )
                    )
              )
            ORDER BY ns.nspname, tbl.relname, idx.relname, key.ord
            """;

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
