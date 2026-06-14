#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureMapBuilder
    {
        public static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
        {
            var tableKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            tableKeys.UnionWith(providerNativeTemporalTableKeys);
            tableKeys.UnionWith(providerOwnedTriggerTableKeys);
            tableKeys.UnionWith(providerSpecificIdentityStrategyTableKeys);
            tableKeys.UnionWith(providerSpecificDefaultTableKeys);
            foreach (var (tableKey, providerSpecificColumnTypes) in providerSpecificColumnTypesByTable)
            {
                if (ScaffoldProviderSpecificTypeClassifier.HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes))
                    tableKeys.Add(tableKey);
            }

            return tableKeys;
        }
    }
}
