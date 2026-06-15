#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationPipeline
    {
        public static ScaffoldFeatureConfigurationsInfo Build(
            IReadOnlyList<ScaffoldFeatureInput> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
        {
            var remainingFeatures = unsupportedFeatures.ToList();
            var generatedFeatureIndexes = new List<int>();

            var providerColumnTypes = BuildProviderColumnTypeStage(
                remainingFeatures,
                entityByTable,
                columnPropertiesByTable,
                generatedFeatureIndexes);
            var defaults = BuildDefaultStage(
                remainingFeatures,
                columnPropertiesByTable,
                generatedFeatureIndexes);
            var checkConstraints = BuildCheckConstraintsStage(
                remainingFeatures,
                entityByTable,
                columnPropertiesByTable,
                providerColumnTypes.EnumCheckConstraintConfigurations,
                generatedFeatureIndexes);
            var expressionIndexConfigurations = BuildExpressionIndexStage(
                remainingFeatures,
                entityByTable,
                generatedFeatureIndexes);
            var collationConfigurations = BuildCollationStage(
                remainingFeatures,
                entityByTable,
                columnPropertiesByTable,
                generatedFeatureIndexes);
            var computedColumns = BuildComputedColumnStage(
                remainingFeatures,
                entityByTable,
                columnPropertiesByTable,
                generatedFeatureIndexes);
            var decimalFacets = BuildDecimalFacetStage(
                remainingFeatures,
                entityByTable,
                columnPropertiesByTable,
                generatedFeatureIndexes);
            var columnFacetConfigurations = ScaffoldFeatureConfigurationBuilder.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);
            var providerObjects = BuildProviderObjectStage(
                remainingFeatures,
                entityByTable,
                columnPropertiesByTable,
                defaults.ProviderSpecificDefaultTableKeys,
                providerColumnTypes.ProviderSpecificColumnTypesByTable,
                generatedFeatureIndexes);

            return new ScaffoldFeatureConfigurationsInfo(
                generatedFeatureIndexes,
                providerColumnTypes.ProviderSpecificColumnTypesByTable,
                defaults.DefaultValuesByTable,
                defaults.DefaultConstraintNamesByTable,
                defaults.ProviderSpecificDefaultTableKeys,
                checkConstraints,
                expressionIndexConfigurations,
                collationConfigurations,
                computedColumns.ComputedColumnConfigurations,
                computedColumns.ComputedColumnsByTable,
                decimalFacets.DecimalPrecisionByTable,
                decimalFacets.PrecisionConfigurations,
                columnFacetConfigurations,
                providerObjects.RowVersionColumnsByTable,
                providerObjects.ProviderNativeTemporalTableKeys,
                providerObjects.ProviderOwnedTriggerTableKeys,
                providerObjects.IdentityOptionConfigurations,
                providerObjects.ProviderSpecificIdentityStrategyTableKeys,
                providerObjects.ProviderOwnedWriteBlockedTableKeys);
        }

    }
}
