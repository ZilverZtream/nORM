#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationPipeline
    {
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
