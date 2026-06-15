#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldFeatureConfigurations(
        IReadOnlyList<ScaffoldUnsupportedFeature> GeneratedModelFeatureDiagnostics,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ProviderSpecificColumnTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultValuesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultConstraintNamesByTable,
        IReadOnlySet<string> ProviderSpecificDefaultTableKeys,
        IReadOnlyList<ScaffoldCheckConstraintConfiguration> CheckConstraints,
        IReadOnlyList<ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
        IReadOnlyList<ScaffoldCollationConfiguration> CollationConfigurations,
        IReadOnlyList<ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
        IReadOnlyDictionary<string, IReadOnlySet<string>> ComputedColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> DecimalPrecisionByTable,
        IReadOnlyList<ScaffoldPrecisionConfiguration> PrecisionConfigurations,
        IReadOnlyList<ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations,
        IReadOnlyDictionary<string, IReadOnlySet<string>> RowVersionColumnsByTable,
        IReadOnlySet<string> ProviderNativeTemporalTableKeys,
        IReadOnlySet<string> ProviderOwnedTriggerTableKeys,
        IReadOnlyList<ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
        IReadOnlySet<string> ProviderSpecificIdentityStrategyTableKeys,
        IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys);

    internal sealed record ScaffoldEntityFileSet(
        IReadOnlyList<(string Path, string Content)> GeneratedFiles,
        IReadOnlyList<string> EntityNames);

    internal readonly record struct ScaffoldManyToManyNavigation(
        string TargetEntityName,
        string CollectionNavigationName);
}
