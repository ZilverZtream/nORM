#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldFeatureConfigurationPipeline
    {
        public static ScaffoldFeatureConfigurationsInfo Build(
            IReadOnlyList<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
        {
            var remainingFeatures = unsupportedFeatures.ToList();
            var generatedFeatureIndexes = new List<int>();

            var providerSpecificColumnTypesByTable = ScaffoldFeatureConfigurationBuilder.BuildFeatureDetailMap(remainingFeatures, "ProviderSpecificColumnType");
            var enumCheckConstraintConfigurations = ScaffoldFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveSupportedProviderSpecificColumnTypeDiagnostics(remainingFeatures, columnPropertiesByTable, generatedFeatureIndexes);

            var defaultValuesByTable = ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(remainingFeatures, columnPropertiesByTable);
            var defaultConstraintNamesByTable = ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultConstraintNameMap(remainingFeatures, columnPropertiesByTable);
            ScaffoldFeatureConfigurationBuilder.RemoveDefaultDiagnostics(remainingFeatures, defaultValuesByTable, generatedFeatureIndexes);
            var providerSpecificDefaultTableKeys = ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(remainingFeatures, "Default");

            var checkConstraints = ScaffoldFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(entityByTable, remainingFeatures)
                .Concat(enumCheckConstraintConfigurations)
                .ToArray();
            ScaffoldFeatureConfigurationBuilder.RemoveCheckConstraintDiagnostics(remainingFeatures, checkConstraints, generatedFeatureIndexes);
            ScaffoldFeatureConfigurationBuilder.RemoveGeneratedProviderValueCheckDiagnostics(remainingFeatures, columnPropertiesByTable, enumCheckConstraintConfigurations, generatedFeatureIndexes);

            var expressionIndexConfigurations = ScaffoldFeatureConfigurationBuilder.BuildExpressionIndexConfigurations(entityByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveExpressionIndexDiagnostics(remainingFeatures, expressionIndexConfigurations, generatedFeatureIndexes);

            var collationConfigurations = ScaffoldFeatureConfigurationBuilder.BuildCollationConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveCollationDiagnostics(remainingFeatures, collationConfigurations, generatedFeatureIndexes);

            var computedColumnConfigurations = ScaffoldFeatureConfigurationBuilder.BuildComputedColumnConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            var computedColumnsByTable = ScaffoldFeatureConfigurationBuilder.BuildFeatureNameMap(remainingFeatures, "Computed", "RowVersion");
            ScaffoldFeatureConfigurationBuilder.RemoveComputedColumnDiagnostics(remainingFeatures, computedColumnConfigurations, generatedFeatureIndexes);

            var decimalPrecisionByTable = ScaffoldFeatureConfigurationBuilder.BuildDecimalPrecisionMap(remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveDecimalPrecisionDiagnostics(remainingFeatures, generatedFeatureIndexes);
            var precisionConfigurations = ScaffoldFeatureConfigurationBuilder.BuildPrecisionConfigurations(entityByTable, columnPropertiesByTable, decimalPrecisionByTable);
            var columnFacetConfigurations = ScaffoldFeatureConfigurationBuilder.BuildColumnFacetConfigurations(entityByTable, columnPropertiesByTable, stringBinaryFacetsByTable);

            var rowVersionColumnsByTable = ScaffoldFeatureConfigurationBuilder.BuildFeatureNameMap(remainingFeatures, "RowVersion");
            var providerNativeTemporalTableKeys = ScaffoldFeatureConfigurationBuilder.BuildProviderNativeTemporalTableKeys(remainingFeatures);
            var providerOwnedTriggerTableKeys = ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(remainingFeatures, "Trigger");

            var identityOptionConfigurations = ScaffoldFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(entityByTable, columnPropertiesByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveIdentityOptionDiagnostics(remainingFeatures, identityOptionConfigurations, generatedFeatureIndexes);
            var providerSpecificIdentityStrategyTableKeys = ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(remainingFeatures, "IdentityStrategy");
            var providerOwnedWriteBlockedTableKeys = ScaffoldFeatureConfigurationBuilder.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

            return new ScaffoldFeatureConfigurationsInfo(
                generatedFeatureIndexes,
                providerSpecificColumnTypesByTable,
                defaultValuesByTable,
                defaultConstraintNamesByTable,
                providerSpecificDefaultTableKeys,
                checkConstraints,
                expressionIndexConfigurations,
                collationConfigurations,
                computedColumnConfigurations,
                computedColumnsByTable,
                decimalPrecisionByTable,
                precisionConfigurations,
                columnFacetConfigurations,
                rowVersionColumnsByTable,
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                identityOptionConfigurations,
                providerSpecificIdentityStrategyTableKeys,
                providerOwnedWriteBlockedTableKeys);
        }
    }
}
