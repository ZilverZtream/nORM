#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        public static ScaffoldFeatureConfigurationsInfo BuildFeatureConfigurations(
            IReadOnlyList<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
            => ScaffoldFeatureConfigurationPipeline.Build(
                unsupportedFeatures,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);
    }
}
