#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(
                ConvertFeatureInputs(features),
                columnPropertiesByTable);

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultConstraintNamesByTable)
            => ConvertDefaultValueConfigurations(ScaffoldFeatureConfigurationBuilder.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable,
                defaultConstraintNamesByTable));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ConvertPrecisionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertDecimalPrecisionInfoMap(decimalPrecisionByTable)));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ConvertColumnFacetConfigurations(ScaffoldFeatureConfigurationBuilder.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertIdentityOptionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertCollationConfigurations(ScaffoldFeatureConfigurationBuilder.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertComputedColumnConfigurations(ScaffoldFeatureConfigurationBuilder.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
            => ConvertDecimalPrecisionMap(ScaffoldFeatureConfigurationBuilder.BuildDecimalPrecisionMap(ConvertFeatureInputs(features)));
    }
}
