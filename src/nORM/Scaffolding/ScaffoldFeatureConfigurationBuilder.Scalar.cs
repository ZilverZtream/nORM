#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldFeatureInput> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(features, columnPropertiesByTable);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultConstraintNameMap(
            IEnumerable<ScaffoldFeatureInput> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildScaffoldDefaultConstraintNameMap(features, columnPropertiesByTable);

        public static IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultConstraintNamesByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildDefaultValueConfigurations(entityByTable, columnPropertiesByTable, defaultValuesByTable, defaultConstraintNamesByTable);

        public static IReadOnlyList<ScaffoldPrecisionConfigurationInfo> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildPrecisionConfigurations(entityByTable, columnPropertiesByTable, decimalPrecisionByTable);

        public static IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildColumnFacetConfigurations(entityByTable, columnPropertiesByTable, columnFacetsByTable);

        public static IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(entityByTable, columnPropertiesByTable, features);

        public static IReadOnlyList<ScaffoldCollationConfigurationInfo> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildCollationConfigurations(entityByTable, columnPropertiesByTable, features);

        public static IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldScalarFeatureConfigurationBuilder.BuildComputedColumnConfigurations(entityByTable, columnPropertiesByTable, features);
    }
}
