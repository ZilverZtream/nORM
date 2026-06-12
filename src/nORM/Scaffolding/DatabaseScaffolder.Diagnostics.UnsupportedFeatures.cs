#nullable enable
using System.Collections.Generic;

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
    }
}
