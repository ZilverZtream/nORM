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

        private static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildCheckConstraintsStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> enumCheckConstraintConfigurations,
            List<int> generatedFeatureIndexes)
        {
            var checkConstraints = ScaffoldFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(entityByTable, remainingFeatures)
                .Concat(enumCheckConstraintConfigurations)
                .ToArray();
            ScaffoldFeatureConfigurationBuilder.RemoveCheckConstraintDiagnostics(remainingFeatures, checkConstraints, generatedFeatureIndexes);
            ScaffoldFeatureConfigurationBuilder.RemoveGeneratedProviderValueCheckDiagnostics(remainingFeatures, columnPropertiesByTable, enumCheckConstraintConfigurations, generatedFeatureIndexes);
            return checkConstraints;
        }

        private static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            List<int> generatedFeatureIndexes)
        {
            var expressionIndexConfigurations = ScaffoldFeatureConfigurationBuilder.BuildExpressionIndexConfigurations(entityByTable, remainingFeatures);
            ScaffoldFeatureConfigurationBuilder.RemoveExpressionIndexDiagnostics(remainingFeatures, expressionIndexConfigurations, generatedFeatureIndexes);
            return expressionIndexConfigurations;
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

        private static ProviderObjectStage BuildProviderObjectStage(
            List<ScaffoldFeatureInput> remainingFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable,
            List<int> generatedFeatureIndexes)
        {
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

            return new ProviderObjectStage(
                rowVersionColumnsByTable,
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                identityOptionConfigurations,
                providerSpecificIdentityStrategyTableKeys,
                providerOwnedWriteBlockedTableKeys);
        }

        private readonly record struct ProviderColumnTypeStage(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ProviderSpecificColumnTypesByTable,
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> EnumCheckConstraintConfigurations);

        private readonly record struct DefaultStage(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultValuesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultConstraintNamesByTable,
            IReadOnlySet<string> ProviderSpecificDefaultTableKeys);

        private readonly record struct ComputedColumnStage(
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> ComputedColumnConfigurations,
            IReadOnlyDictionary<string, IReadOnlySet<string>> ComputedColumnsByTable);

        private readonly record struct DecimalFacetStage(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> DecimalPrecisionByTable,
            IReadOnlyList<ScaffoldPrecisionConfigurationInfo> PrecisionConfigurations);

        private readonly record struct ProviderObjectStage(
            IReadOnlyDictionary<string, IReadOnlySet<string>> RowVersionColumnsByTable,
            IReadOnlySet<string> ProviderNativeTemporalTableKeys,
            IReadOnlySet<string> ProviderOwnedTriggerTableKeys,
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> IdentityOptionConfigurations,
            IReadOnlySet<string> ProviderSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys);
    }
}
