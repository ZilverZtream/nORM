#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationPipeline
    {
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
    }
}
