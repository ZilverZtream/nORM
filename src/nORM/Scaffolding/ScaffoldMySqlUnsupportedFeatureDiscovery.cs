using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            await AddCatalogFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await MarkDefaultNamedCheckConstraintFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            features.AddRange(await GetExpressionIndexFeaturesAsync(connection, provider, tables).ConfigureAwait(false));
            return features;
        }

        private static Task AddCatalogFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, MySqlUnsupportedFeatureSql);

        private static Task MarkDefaultNamedCheckConstraintFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => ScaffoldSyntheticFeatureNameMarker.MarkFeaturesAsync(
                connection,
                features,
                tableKeys,
                MySqlDefaultNamedCheckConstraintSql,
                "CheckConstraint");
    }
}
