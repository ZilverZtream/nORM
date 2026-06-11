using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresUnsupportedFeatureDiscovery
    {
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
    }
}
