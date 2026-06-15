#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldFeatureConfigurations(
        IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> GeneratedModelFeatureDiagnostics,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ProviderSpecificColumnTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultValuesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultConstraintNamesByTable,
        IReadOnlySet<string> ProviderSpecificDefaultTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> CheckConstraints,
        IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> CollationConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
        IReadOnlyDictionary<string, IReadOnlySet<string>> ComputedColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>> DecimalPrecisionByTable,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> PrecisionConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations,
        IReadOnlyDictionary<string, IReadOnlySet<string>> RowVersionColumnsByTable,
        IReadOnlySet<string> ProviderNativeTemporalTableKeys,
        IReadOnlySet<string> ProviderOwnedTriggerTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
        IReadOnlySet<string> ProviderSpecificIdentityStrategyTableKeys,
        IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys);

    internal sealed record ScaffoldEntityFileSet(
        IReadOnlyList<(string Path, string Content)> GeneratedFiles,
        IReadOnlyList<string> EntityNames);

    internal readonly record struct ScaffoldManyToManyNavigation(
        string TargetEntityName,
        string CollectionNavigationName);
}
