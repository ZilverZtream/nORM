#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
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
