#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyList<(string Name, string Sql)> ExtractSqliteCheckConstraints(string tableName, string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractCheckConstraints(tableName, createTableSql);

        private static IReadOnlyDictionary<string, (string Sql, bool Stored)> ExtractSqliteGeneratedColumns(string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractGeneratedColumns(createTableSql);

        private static int FindMatchingParenthesis(string sql, int openIndex)
            => ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);

        private static IReadOnlyDictionary<string, string> ExtractSqliteColumnCollations(string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractColumnCollations(createTableSql);

        private static IReadOnlyList<string> SplitTopLevelCommaSeparated(string sql)
            => ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(sql);

        private static bool IsSqliteProviderSpecificDeclaredType(string? declaredType)
            => ScaffoldSqliteDdlParser.IsProviderSpecificDeclaredType(declaredType);

        private static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => ScaffoldSqliteDdlParser.IsUnsafeProviderSpecificDeclaredType(normalizedDeclaredType);

        private static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(normalizedDeclaredType, token);

        private static ScaffoldFeatureConfigurations BuildFeatureConfigurations(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureConfigurations(
                unsupportedFeatures,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);

        private static void RestoreGeneratedManyToManyUnsupportedFeatures(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IEnumerable<ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics,
            IReadOnlySet<string> manyToManyJoinTableKeys)
            => ScaffoldModelCompositionBuilder.RestoreGeneratedManyToManyUnsupportedFeatures(
                unsupportedFeatures,
                generatedModelFeatureDiagnostics,
                manyToManyJoinTableKeys);

        private static bool ShouldRestoreGeneratedManyToManyUnsupportedFeature(string kind)
            => ScaffoldModelCompositionBuilder.ShouldRestoreGeneratedManyToManyUnsupportedFeature(kind);

        private static bool IsScaffoldableProviderSpecificColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsScaffoldableProviderSpecificColumnType(detail);

        private static bool IsSafePostgresUserDefinedScalarColumnType(string normalized)
            => ScaffoldProviderSpecificTypeClassifier.IsSafePostgresUserDefinedScalarColumnType(normalized);

        private static bool HasWriteBlockingProviderSpecificColumnTypes(IReadOnlyDictionary<string, string>? providerSpecificColumnTypes)
            => ScaffoldProviderSpecificTypeClassifier.HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes);

        private static bool ShouldMarkScaffoldedEntityReadOnly(
            string tableKey,
            IReadOnlySet<string> queryArtifactTableKeys,
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
                tableKey,
                queryArtifactTableKeys,
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypes,
                primaryKeyColumnsByTable);

        private static bool IsWriteBlockingProviderSpecificColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsWriteBlockingProviderSpecificColumnType(detail);

        private static bool IsSafeMySqlUnsignedDecimalType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsSafeMySqlUnsignedDecimalType(detail);

        private static bool IsSafeSqlServerAliasColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsSafeSqlServerAliasColumnType(detail);

        private static bool TryGetSqlServerAliasBaseTypeText(string? detail, out string typeText)
            => ScaffoldProviderSpecificTypeClassifier.TryGetSqlServerAliasBaseTypeText(detail, out typeText);

        private static bool IsSafeSqlServerAliasBaseType(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.IsSafeSqlServerAliasBaseType(typeText);

        private static bool IsSafePostgresDomainColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsSafePostgresDomainColumnType(detail);
    }
}
