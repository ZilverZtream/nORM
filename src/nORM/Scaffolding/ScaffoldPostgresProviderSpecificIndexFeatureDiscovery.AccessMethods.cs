using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificIndexFeatureDiscovery
    {
        private static Task AddProviderSpecificAccessMethodFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => ScaffoldUnsupportedFeatureDiscoveryReader.AddFeaturesAsync(connection, features, tableKeys, """
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
                """);
    }
}
