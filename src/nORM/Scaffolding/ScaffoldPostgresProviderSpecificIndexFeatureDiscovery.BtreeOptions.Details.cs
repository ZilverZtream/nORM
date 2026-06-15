namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificIndexFeatureDiscovery
    {
        private const string BtreeOptionNullsNotDistinctDetailSql = """

                     '; hasNullsNotDistinct=' ||
                     CASE WHEN COALESCE((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false) THEN 'true' ELSE 'false' END ||
            """;

        private const string BtreeOptionOperatorClassDetailSql = """

                     '; hasNonDefaultOperatorClass=' ||
                     CASE WHEN EXISTS (
                         SELECT 1
                         FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                         INNER JOIN pg_opclass option_opclass
                             ON option_opclass.oid = ix.indclass[option_key.ord - 1]
                         WHERE option_key.ord <= ix.indnkeyatts
                           AND option_opclass.opcdefault = false
                     ) THEN 'true' ELSE 'false' END ||
            """;

        private const string BtreeOptionCollationDetailSql = """

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
            """;

        private const string BtreeOptionNullOrderingDetailSql = """

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
            """;

        private const string BtreeOptionIndexSqlDetailSql = """

                     '; indexSql=' || pg_get_indexdef(ix.indexrelid))::text AS Detail
            """;
    }
}
