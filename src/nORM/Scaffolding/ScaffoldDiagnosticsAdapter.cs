#nullable enable
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    internal static class ScaffoldDiagnosticsAdapter
    {
        public static string ScaffoldDiagnostics(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys,
            ObjectPool<StringBuilder> stringBuilderPool)
            => ScaffoldDiagnosticReportBuilder.WriteMarkdown(
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                ConvertUnsupportedFeatureInfos(unsupportedFeatures),
                ScaffoldSchemaDiscoveryAdapter.ConvertSkippedObjectInfos(skippedObjects),
                primaryKeyColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys,
                stringBuilderPool);

        public static ScaffoldCompositeForeignKeyDiagnosticInfo[] BuildCompositeForeignKeyDiagnostics(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldJoinTableDiagnosticBuilder.BuildCompositeForeignKeyDiagnostics(
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                nonNullableColumnsByTable);

        public static ScaffoldPossibleJoinTableDiagnosticInfo[] BuildPossibleJoinTableDiagnostics(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
            => ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableDiagnostics(
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);

        public static IReadOnlyDictionary<string, object?> BuildPossibleJoinTableMetadata(
            string tableKey,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableMetadata(
                tableKey,
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                providerOwnedWriteBlockedTableKeys);

        public static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldJoinTableDiagnosticBuilder.BuildCompositeForeignKeyMetadata(
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(rows),
                primaryKeyColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                nonNullableColumnsByTable);

        public static string[] BuildPossibleJoinTableReasons(
            string tableKey,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableReasons(
                tableKey,
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                providerOwnedWriteBlockedTableKeys);

        public static IReadOnlyDictionary<string, object?> BuildUnsupportedFeatureMetadata(
            DatabaseScaffolder.ScaffoldUnsupportedFeature feature)
            => ScaffoldUnsupportedFeatureMetadataBuilder.BuildMetadata(
                new ScaffoldUnsupportedFeatureInfo(feature.TableKey, feature.Kind, feature.Name, feature.Detail)
                {
                    Metadata = feature.Metadata
                });

        public static bool TryParseMetadataBoolean(string value, out bool parsed)
            => ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(value, out parsed);

        public static string ScaffoldDiagnosticsJson(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
            => ScaffoldDiagnosticReportBuilder.WriteJson(
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                ConvertUnsupportedFeatureInfos(unsupportedFeatures),
                ScaffoldSchemaDiscoveryAdapter.ConvertSkippedObjectInfos(skippedObjects),
                primaryKeyColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes),
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);

        public static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> ConvertUnsupportedFeatureInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> features)
        {
            var converted = new ScaffoldUnsupportedFeatureInfo[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                converted[i] = new ScaffoldUnsupportedFeatureInfo(
                    feature.TableKey,
                    feature.Kind,
                    feature.Name,
                    feature.Detail)
                {
                    Metadata = BuildUnsupportedFeatureMetadata(feature)
                };
            }

            return converted;
        }
    }
}
