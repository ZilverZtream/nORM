#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        public static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureNameMap(ConvertFeatureInputs(features), kinds);

        public static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationBuilder.BuildProviderNativeTemporalTableKeys(ConvertFeatureInputs(features));

        public static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureConfigurationBuilder.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        public static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(ConvertFeatureInputs(features), kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureDetailMap(ConvertFeatureInputs(features), kinds);
    }
}
