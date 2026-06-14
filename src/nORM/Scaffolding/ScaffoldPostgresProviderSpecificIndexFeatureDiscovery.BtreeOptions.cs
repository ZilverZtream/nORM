using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificIndexFeatureDiscovery
    {
        private static Task AddProviderSpecificBtreeOptionFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => ScaffoldUnsupportedFeatureDiscoveryReader.AddFeaturesAsync(connection, features, tableKeys, """
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
                """);
    }
}
