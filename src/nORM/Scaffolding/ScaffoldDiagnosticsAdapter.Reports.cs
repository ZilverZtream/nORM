#nullable enable
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsAdapter
    {
        public static string ScaffoldDiagnostics(
            ScaffoldDiagnosticsRequest request,
            ObjectPool<StringBuilder> stringBuilderPool)
            => ScaffoldDiagnosticReportBuilder.WriteMarkdown(
                ConvertReportRequest(request),
                stringBuilderPool);

        public static string ScaffoldDiagnosticsJson(ScaffoldDiagnosticsRequest request)
            => ScaffoldDiagnosticReportBuilder.WriteJson(
                ConvertReportRequest(request));

        private static ScaffoldDiagnosticReportRequest ConvertReportRequest(ScaffoldDiagnosticsRequest request)
            => new(
                ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(request.ForeignKeys),
                ConvertUnsupportedFeatureInfos(request.UnsupportedFeatures),
                ScaffoldSchemaDiscoveryAdapter.ConvertSkippedObjectInfos(request.SkippedObjects),
                request.PrimaryKeyColumnsByTable,
                ScaffoldRelationshipAdapter.ConvertIndexInfos(request.Indexes),
                request.ColumnPropertiesByTable,
                request.NonNullableColumnsByTable,
                request.DatabaseGeneratedColumnsByTable,
                request.IdentityColumnsByTable,
                request.ProviderOwnedWriteBlockedTableKeys,
                request.EmittedManyToManyJoinTableKeys,
                request.SuppressRelationshipDiagnostics);
    }
}
