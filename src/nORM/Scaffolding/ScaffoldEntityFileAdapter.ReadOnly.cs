#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityFileAdapter
    {
        public static bool ShouldMarkScaffoldedEntityReadOnly(
            string tableKey,
            IReadOnlySet<string> queryArtifactTableKeys,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => queryArtifactTableKeys.Contains(tableKey)
               || providerOwnedWriteBlockedTableKeys.Contains(tableKey)
               || !primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
               || primaryKeyColumns.Count == 0;

        public static bool ShouldMarkScaffoldedEntityReadOnly(
            string tableKey,
            IReadOnlySet<string> queryArtifactTableKeys,
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => queryArtifactTableKeys.Contains(tableKey)
               || providerNativeTemporalTableKeys.Contains(tableKey)
               || providerOwnedTriggerTableKeys.Contains(tableKey)
               || providerSpecificIdentityStrategyTableKeys.Contains(tableKey)
               || providerSpecificDefaultTableKeys.Contains(tableKey)
               || ScaffoldProviderSpecificTypeClassifier.HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes)
               || !primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
               || primaryKeyColumns.Count == 0;
    }
}
