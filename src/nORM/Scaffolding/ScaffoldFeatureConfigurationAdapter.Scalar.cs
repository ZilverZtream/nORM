#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(
                ConvertFeatureInputs(features),
                columnPropertiesByTable);

        public static IReadOnlyList<ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultConstraintNamesByTable)
            => ConvertDefaultValueConfigurations(ScaffoldFeatureConfigurationBuilder.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable,
                defaultConstraintNamesByTable));

        public static IReadOnlyList<ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ConvertPrecisionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertDecimalPrecisionInfoMap(decimalPrecisionByTable)));

        public static IReadOnlyList<ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ConvertColumnFacetConfigurations(ScaffoldFeatureConfigurationBuilder.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable));

        public static IReadOnlyList<ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertIdentityOptionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyList<ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertCollationConfigurations(ScaffoldFeatureConfigurationBuilder.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyList<ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertComputedColumnConfigurations(ScaffoldFeatureConfigurationBuilder.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertDecimalPrecisionMap(ScaffoldFeatureConfigurationBuilder.BuildDecimalPrecisionMap(ConvertFeatureInputs(features)));
    }
}
