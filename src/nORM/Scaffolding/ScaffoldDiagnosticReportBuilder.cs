#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Database scaffolding emits dynamic entity types and traverses live mapping metadata; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Database scaffolding reflects over provider and entity metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal static class ScaffoldDiagnosticReportBuilder
    {
        public static string WriteMarkdown(
            ScaffoldDiagnosticReportRequest request,
            ObjectPool<StringBuilder> stringBuilderPool)
        {
            var report = BuildReport(request);

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

        public static string WriteJson(ScaffoldDiagnosticReportRequest request)
        {
            var report = BuildReport(request);

            return ScaffoldDiagnosticsWriter.WriteJson(
                report.CompositeForeignKeys,
                report.PossibleJoinTables,
                report.UnsupportedFeatures,
                report.SkippedObjects);
        }

        private static ScaffoldDiagnosticReport BuildReport(ScaffoldDiagnosticReportRequest request)
        {
            var compositeForeignKeys = request.SuppressRelationshipDiagnostics
                ? Array.Empty<ScaffoldCompositeForeignKeyDiagnosticInfo>()
                : ScaffoldJoinTableDiagnosticBuilder.BuildCompositeForeignKeyDiagnostics(
                    request.ForeignKeys,
                    request.PrimaryKeyColumnsByTable,
                    request.Indexes,
                    request.NonNullableColumnsByTable);
            var possibleJoinTables = request.SuppressRelationshipDiagnostics
                ? Array.Empty<ScaffoldPossibleJoinTableDiagnosticInfo>()
                : ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableDiagnostics(
                    request.ForeignKeys,
                    request.PrimaryKeyColumnsByTable,
                    request.ColumnPropertiesByTable,
                    request.NonNullableColumnsByTable,
                    request.DatabaseGeneratedColumnsByTable,
                    request.IdentityColumnsByTable,
                    request.Indexes,
                    request.ProviderOwnedWriteBlockedTableKeys,
                    request.EmittedManyToManyJoinTableKeys);

            return new ScaffoldDiagnosticReport(
                compositeForeignKeys,
                possibleJoinTables,
                request.UnsupportedFeatures,
                request.SkippedObjects);
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
