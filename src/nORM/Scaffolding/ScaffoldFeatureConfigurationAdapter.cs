#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        public static ScaffoldFeatureConfigurations BuildFeatureConfigurations(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
        {
            var featureInputs = ConvertFeatureInputs(unsupportedFeatures);
            var configurations = ScaffoldFeatureConfigurationBuilder.BuildFeatureConfigurations(
                featureInputs,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);
            var generatedFeatureIndexes = configurations.GeneratedFeatureIndexes.ToArray();
            var generatedFeatureIndexSet = generatedFeatureIndexes.ToHashSet();
            var generatedModelFeatureDiagnostics = generatedFeatureIndexes
                .Select(index => unsupportedFeatures[index])
                .ToArray();
            for (var i = unsupportedFeatures.Count - 1; i >= 0; i--)
            {
                if (generatedFeatureIndexSet.Contains(i))
                    unsupportedFeatures.RemoveAt(i);
            }

            return ConvertFeatureConfigurations(configurations, generatedModelFeatureDiagnostics);
        }
    }
}
