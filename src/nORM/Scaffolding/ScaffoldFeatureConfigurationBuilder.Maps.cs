#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
            => ScaffoldFeatureMapBuilder.BuildFeatureNameMap(features, kinds);

        public static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldFeatureMapBuilder.BuildProviderNativeTemporalTableKeys(features);

        public static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureMapBuilder.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        public static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
            => ScaffoldFeatureMapBuilder.BuildFeatureTableKeys(features, kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
            => ScaffoldFeatureMapBuilder.BuildFeatureDetailMap(features, kinds);

        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldFeatureInput> features)
            => ScaffoldFeatureMapBuilder.BuildDecimalPrecisionMap(features);
    }
}
