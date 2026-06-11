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
