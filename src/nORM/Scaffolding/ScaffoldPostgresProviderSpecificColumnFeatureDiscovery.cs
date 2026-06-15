using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificColumnFeatureDiscovery
    {
        public static Task AddFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => ScaffoldUnsupportedFeatureDiscoveryReader.AddFeaturesAsync(
                connection,
                features,
                tableKeys,
                ProviderSpecificColumnSql);

        private static readonly string ProviderSpecificColumnSql = string.Concat(
            ProviderSpecificColumnHeaderSql,
            ProviderSpecificDomainColumnDetailSql,
            ProviderSpecificEnumColumnDetailSql,
            ProviderSpecificColumnFallbackDetailSql,
            ProviderSpecificColumnFilterSql);

        private const string ProviderSpecificColumnHeaderSql = """
            SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'ProviderSpecificColumnType' AS Kind,
                (CASE
            """;
    }
}
