#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldFeatureInput(
        int SourceIndex,
        ScaffoldUnsupportedFeatureInfo Feature);

    internal sealed record ScaffoldFeatureConfigurationsInfo(
        IReadOnlyList<int> GeneratedFeatureIndexes,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ProviderSpecificColumnTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultValuesByTable,
        IReadOnlySet<string> ProviderSpecificDefaultTableKeys,
        IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> CheckConstraints,
        IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> ExpressionIndexConfigurations,
        IReadOnlyList<ScaffoldCollationConfigurationInfo> CollationConfigurations,
        IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> ComputedColumnConfigurations,
        IReadOnlyDictionary<string, IReadOnlySet<string>> ComputedColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> DecimalPrecisionByTable,
        IReadOnlyList<ScaffoldPrecisionConfigurationInfo> PrecisionConfigurations,
        IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> ColumnFacetConfigurations,
        IReadOnlyDictionary<string, IReadOnlySet<string>> RowVersionColumnsByTable,
        IReadOnlySet<string> ProviderNativeTemporalTableKeys,
        IReadOnlySet<string> ProviderOwnedTriggerTableKeys,
        IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> IdentityOptionConfigurations,
        IReadOnlySet<string> ProviderSpecificIdentityStrategyTableKeys,
        IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys);

    internal readonly record struct ScaffoldDefaultValueConfigurationInfo(
        string TableKey,
        string EntityName,
        string ColumnName,
        string PropertyName,
        string DefaultValueSql);

    internal readonly record struct ScaffoldIdentityOptionConfigurationInfo(
        string TableKey,
        string EntityName,
        string ColumnName,
        string PropertyName,
        long Seed,
        long Increment);

    internal readonly record struct ScaffoldPrecisionConfigurationInfo(
        string TableKey,
        string EntityName,
        string ColumnName,
        string PropertyName,
        int Precision,
        int? Scale);

    internal readonly record struct ScaffoldColumnFacetConfigurationInfo(
        string TableKey,
        string EntityName,
        string ColumnName,
        string PropertyName,
        int? MaxLength,
        bool? IsUnicode,
        bool IsFixedLength);

    internal readonly record struct ScaffoldCheckConstraintConfigurationInfo(
        string TableKey,
        string EntityName,
        string Name,
        string Sql);

    internal readonly record struct ScaffoldComputedColumnConfigurationInfo(
        string TableKey,
        string EntityName,
        string ColumnName,
        string PropertyName,
        string Sql,
        bool Stored);

    internal readonly record struct ScaffoldExpressionIndexConfigurationInfo(
        string TableKey,
        string EntityName,
        string Name,
        string ExpressionSql,
        bool IsUnique,
        string? FilterSql);

    internal readonly record struct ScaffoldCollationConfigurationInfo(
        string TableKey,
        string EntityName,
        string ColumnName,
        string PropertyName,
        string Collation);

    internal readonly record struct ScaffoldDecimalPrecisionInfo(
        int Precision,
        int? Scale);
}
