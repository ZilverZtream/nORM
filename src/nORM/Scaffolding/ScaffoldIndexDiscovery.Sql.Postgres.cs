#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
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
    }
}
