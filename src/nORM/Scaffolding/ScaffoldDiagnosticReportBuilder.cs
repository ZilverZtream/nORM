#nullable enable
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    internal static class ScaffoldDiagnosticReportBuilder
    {
        public static string WriteMarkdown(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys,
            ObjectPool<StringBuilder> stringBuilderPool)
        {
            var report = BuildReport(
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

            if (report.IsEmpty)
                return string.Empty;

            var sb = stringBuilderPool.Get();
            try
            {
                ScaffoldDiagnosticsWriter.AppendMarkdown(
                    sb,
                    report.CompositeForeignKeys,
                    report.PossibleJoinTables,
                    report.UnsupportedFeatures,
                    report.SkippedObjects);
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                stringBuilderPool.Return(sb);
            }
        }

        public static string WriteJson(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys)
        {
            var report = BuildReport(
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

            return ScaffoldDiagnosticsWriter.WriteJson(
                report.CompositeForeignKeys,
                report.PossibleJoinTables,
                report.UnsupportedFeatures,
                report.SkippedObjects);
        }

        private static ScaffoldDiagnosticReport BuildReport(
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys)
        {
            var compositeForeignKeys = ScaffoldJoinTableDiagnosticBuilder.BuildCompositeForeignKeyDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable);
            var possibleJoinTables = ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);

            return new ScaffoldDiagnosticReport(
                compositeForeignKeys,
                possibleJoinTables,
                unsupportedFeatures,
                skippedObjects);
        }

        private readonly record struct ScaffoldDiagnosticReport(
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> CompositeForeignKeys,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> PossibleJoinTables,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> UnsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObjectInfo> SkippedObjects)
        {
            public bool IsEmpty =>
                CompositeForeignKeys.Count == 0
                && PossibleJoinTables.Count == 0
                && UnsupportedFeatures.Count == 0
                && SkippedObjects.Count == 0;
        }
    }
}
