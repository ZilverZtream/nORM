#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldOutputPlanBuilder
    {
        private static string BuildDiagnostics(ScaffoldOutputPlanRequest request)
        {
            return ScaffoldDiagnosticsAdapter.ScaffoldDiagnostics(
                BuildDiagnosticsRequest(request),
                request.StringBuilderPool);
        }

        private static string? BuildDiagnosticsJson(
            ScaffoldOutputPlanRequest request,
            string diagnostics)
        {
            if (string.IsNullOrWhiteSpace(diagnostics))
                return null;

            return ScaffoldDiagnosticsAdapter.ScaffoldDiagnosticsJson(
                BuildDiagnosticsRequest(request));
        }

        private static ScaffoldDiagnosticsRequest BuildDiagnosticsRequest(ScaffoldOutputPlanRequest request)
        {
            var discovery = request.Discovery;
            return new ScaffoldDiagnosticsRequest(
                discovery.ForeignKeys,
                discovery.UnsupportedFeatures,
                discovery.SkippedObjects,
                discovery.PrimaryKeyColumnsByTable,
                discovery.Indexes,
                discovery.ColumnPropertiesByTable,
                discovery.NonNullableColumnsByTable,
                discovery.FeatureConfigurations.ComputedColumnsByTable,
                discovery.IdentityColumnsByTable,
                discovery.FeatureConfigurations.ProviderOwnedWriteBlockedTableKeys,
                request.Composition.ManyToManyJoinTableKeys,
                request.Options.NoRelationships);
        }
    }
}
