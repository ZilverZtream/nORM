#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureNameMap(features, kinds);

        private static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildProviderNativeTemporalTableKeys(features);

        private static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        private static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureTableKeys(features, kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureDetailMap(features, kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildDecimalPrecisionMap(features);
    }
}
