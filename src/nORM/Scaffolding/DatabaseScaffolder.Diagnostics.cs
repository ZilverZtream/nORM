#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static void AddMissingPrimaryKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldUnsupportedDiagnosticAdapter.AddMissingPrimaryKeyDiagnostics(
                features,
                tables,
                primaryKeyColumnsByTable,
                columnPropertiesByTable);

        private static void AddReferentialActionDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys)
            => ScaffoldUnsupportedDiagnosticAdapter.AddReferentialActionDiagnostics(features, foreignKeys);

        private static void RemoveSupportedDescendingIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedDescendingIndexDiagnostics(features, indexes);

        private static void RemoveSupportedIncludedColumnIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedIncludedColumnIndexDiagnostics(features, indexes);

        private static void RemoveSupportedPartialIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedPartialIndexDiagnostics(features, indexes);

        private static void AddRelationshipPrincipalKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => features.AddRange(ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildPrincipalKeyDiagnostics(
                    ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable,
                    ConvertIndexInfos(indexes))));

        private static void AddRelationshipDependentKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => features.AddRange(ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildDependentKeyDiagnostics(
                    ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable)));

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

        private static string ScaffoldDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
            => ScaffoldDiagnosticsAdapter.ScaffoldDiagnostics(
                foreignKeys,
                unsupportedFeatures,
                skippedObjects,
                primaryKeyColumnsByTable,
                indexes,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys,
                _stringBuilderPool);

        private static ScaffoldCompositeForeignKeyDiagnosticInfo[] BuildCompositeForeignKeyDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldDiagnosticsAdapter.BuildCompositeForeignKeyDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable);

        private static ScaffoldPossibleJoinTableDiagnosticInfo[] BuildPossibleJoinTableDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys)
            => ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);

        private static IReadOnlyDictionary<string, object?> BuildPossibleJoinTableMetadata(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableMetadata(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        private static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldDiagnosticsAdapter.BuildCompositeForeignKeyMetadata(
                rows,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable);

        private static string[] BuildPossibleJoinTableReasons(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableReasons(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(ScaffoldSkippedObject obj)
            => ScaffoldSchemaDiscoveryAdapter.BuildSkippedObjectMetadata(obj);

        private static IReadOnlyDictionary<string, object?> BuildUnsupportedFeatureMetadata(ScaffoldUnsupportedFeature feature)
            => ScaffoldDiagnosticsAdapter.BuildUnsupportedFeatureMetadata(feature);

        private static bool TryParseMetadataBoolean(string value, out bool parsed)
            => ScaffoldDiagnosticsAdapter.TryParseMetadataBoolean(value, out parsed);

        private static string ScaffoldDiagnosticsJson(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
            => ScaffoldDiagnosticsAdapter.ScaffoldDiagnosticsJson(
                foreignKeys,
                unsupportedFeatures,
                skippedObjects,
                primaryKeyColumnsByTable,
                indexes,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);
    }
}
