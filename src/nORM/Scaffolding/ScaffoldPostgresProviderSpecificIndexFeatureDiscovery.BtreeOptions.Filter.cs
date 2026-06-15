namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificIndexFeatureDiscovery
    {
        private const string BtreeOptionSourceSql = """

                FROM pg_index ix
                INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                INNER JOIN pg_am am ON am.oid = idx.relam
            """;

        private const string BtreeOptionFilterSql = """

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
            """;
    }
}
