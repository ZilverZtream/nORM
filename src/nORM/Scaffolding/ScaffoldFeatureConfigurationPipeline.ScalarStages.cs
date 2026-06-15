#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationPipeline
    {
        private static ProviderColumnTypeStage BuildProviderColumnTypeStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            List<int> generatedFeatureIndexes)
        {
            var providerSpecificColumnTypesByTable = ScaffoldFeatureConfigurationBuilder.BuildFeatureDetailMap(remainingFeatures, "ProviderSpecificColumnType");
            var enumCheckConstraintConfigurations = ScaffoldFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveSupportedProviderSpecificColumnTypeDiagnostics(remainingFeatures, columnPropertiesByTable, generatedFeatureIndexes);
            return new ProviderColumnTypeStage(providerSpecificColumnTypesByTable, enumCheckConstraintConfigurations);
        }

        private static DefaultStage BuildDefaultStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            List<int> generatedFeatureIndexes)
        {
            var defaultValuesByTable = ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(remainingFeatures, columnPropertiesByTable);
            var defaultConstraintNamesByTable = ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultConstraintNameMap(remainingFeatures, columnPropertiesByTable);
            ScaffoldFeatureConfigurationBuilder.RemoveDefaultDiagnostics(remainingFeatures, defaultValuesByTable, generatedFeatureIndexes);
            var providerSpecificDefaultTableKeys = ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(remainingFeatures, "Default");
            return new DefaultStage(defaultValuesByTable, defaultConstraintNamesByTable, providerSpecificDefaultTableKeys);
        }

        private static IReadOnlyList<ScaffoldCollationConfigurationInfo> BuildCollationStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            List<int> generatedFeatureIndexes)
        {
            var collationConfigurations = ScaffoldFeatureConfigurationBuilder.BuildCollationConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveCollationDiagnostics(remainingFeatures, collationConfigurations, generatedFeatureIndexes);
            return collationConfigurations;
        }

        private static ComputedColumnStage BuildComputedColumnStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            List<int> generatedFeatureIndexes)
        {
            var computedColumnConfigurations = ScaffoldFeatureConfigurationBuilder.BuildComputedColumnConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            var computedColumnsByTable = ScaffoldFeatureConfigurationBuilder.BuildFeatureNameMap(remainingFeatures, "Computed", "RowVersion");
            ScaffoldFeatureConfigurationBuilder.RemoveComputedColumnDiagnostics(remainingFeatures, computedColumnConfigurations, generatedFeatureIndexes);
            return new ComputedColumnStage(computedColumnConfigurations, computedColumnsByTable);
        }

        private static DecimalFacetStage BuildDecimalFacetStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            List<int> generatedFeatureIndexes)
        {
            var decimalPrecisionByTable = ScaffoldFeatureConfigurationBuilder.BuildDecimalPrecisionMap(remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveDecimalPrecisionDiagnostics(remainingFeatures, generatedFeatureIndexes);
            var precisionConfigurations = ScaffoldFeatureConfigurationBuilder.BuildPrecisionConfigurations(entityByTable, columnPropertiesByTable, decimalPrecisionByTable);
            return new DecimalFacetStage(decimalPrecisionByTable, precisionConfigurations);
        }
    }
}
