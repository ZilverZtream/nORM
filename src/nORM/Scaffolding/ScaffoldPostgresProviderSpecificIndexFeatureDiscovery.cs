using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificIndexFeatureDiscovery
    {
        public static async Task AddFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
        {
            await AddProviderSpecificAccessMethodFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            await AddProviderSpecificBtreeOptionFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
        }
    }
}
