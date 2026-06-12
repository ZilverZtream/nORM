#nullable enable
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsAdapter
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
    }
}
