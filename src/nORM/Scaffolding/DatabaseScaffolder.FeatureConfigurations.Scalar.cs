#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildScaffoldDefaultValueMap(features, columnPropertiesByTable);

        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable);

        private static IReadOnlyList<ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                decimalPrecisionByTable);

        private static IReadOnlyList<ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable);

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static IReadOnlyList<ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static IReadOnlyList<ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);
    }
}
