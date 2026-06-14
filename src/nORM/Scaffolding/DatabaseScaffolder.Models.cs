#nullable enable
using System.Collections.Generic;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
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

        internal readonly record struct ScaffoldTable(string Name, string? Schema);

        internal readonly record struct ScaffoldSkippedObject(
            string? Schema,
            string Name,
            string Kind,
            string Detail,
            string? Comment);

        internal readonly record struct ScaffoldPrimaryKey(
            string EntityName,
            string[] PropertyNames,
            string? ConstraintName);

        internal readonly record struct ScaffoldDefaultValueConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string DefaultValueSql,
            string? ConstraintName);

        internal readonly record struct ScaffoldIdentityOptionConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            long Seed,
            long Increment);

        internal readonly record struct ScaffoldPrecisionConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            int Precision,
            int? Scale);

        internal readonly record struct ScaffoldColumnFacetConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            int? MaxLength,
            bool? IsUnicode,
            bool IsFixedLength);

        internal readonly record struct ScaffoldCheckConstraintConfiguration(
            string TableKey,
            string EntityName,
            string Name,
            string Sql);

        internal readonly record struct ScaffoldComputedColumnConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string Sql,
            bool Stored);

        internal readonly record struct ScaffoldExpressionIndexConfiguration(
            string TableKey,
            string EntityName,
            string Name,
            string ExpressionSql,
            bool IsUnique,
            string? FilterSql)
        {
            public string[]? IncludedColumnNames { get; init; }

            public IndexNullSortOrder NullSortOrder { get; init; }

            public bool NullsNotDistinct { get; init; }
        }

        internal readonly record struct ScaffoldCollationConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string Collation);

        internal readonly record struct ScaffoldDecimalPrecision(
            int Precision,
            int? Scale);

        internal readonly record struct ScaffoldUnsupportedFeature(
            string TableKey,
            string Kind,
            string Name,
            string Detail)
        {
            public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
        }
    }
}
