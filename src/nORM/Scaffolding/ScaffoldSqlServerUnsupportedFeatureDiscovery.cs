using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            await AddCatalogFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await MarkSystemNamedCheckConstraintFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await MarkNamedDefaultConstraintFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            return features;
        }

        private static Task AddCatalogFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, SqlServerUnsupportedFeatureSql);
    }
}
