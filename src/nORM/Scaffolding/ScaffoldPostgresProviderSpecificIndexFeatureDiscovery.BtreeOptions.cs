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
            => ScaffoldUnsupportedFeatureDiscoveryReader.AddFeaturesAsync(connection, features, tableKeys, ProviderSpecificBtreeOptionSql);

        private static readonly string ProviderSpecificBtreeOptionSql = string.Concat(
            BtreeOptionSelectSql,
            BtreeOptionNullsNotDistinctDetailSql,
            BtreeOptionOperatorClassDetailSql,
            BtreeOptionCollationDetailSql,
            BtreeOptionNullOrderingDetailSql,
            BtreeOptionIndexSqlDetailSql,
            BtreeOptionSourceSql,
            BtreeOptionFilterSql);

        private const string BtreeOptionSelectSql = """
                SELECT ns.nspname AS TableSchema, tbl.relname AS TableName, idx.relname AS ObjectName, 'ProviderSpecificIndex' AS Kind,
                    ('PostgreSQL btree index with provider-specific key options' ||
                     '; accessMethod=btree' ||
            """;
    }
}
