using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            await AddDefaultColumnFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddComputedColumnFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddTriggerFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddCheckConstraintFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddCollationFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await ScaffoldPostgresProviderSpecificColumnFeatureDiscovery.AddFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddPrecisionScaleFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddPartialIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddExpressionIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddIncludedColumnIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddDescendingIndexFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await ScaffoldPostgresProviderSpecificIndexFeatureDiscovery.AddFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            return features;
        }
    }
}
