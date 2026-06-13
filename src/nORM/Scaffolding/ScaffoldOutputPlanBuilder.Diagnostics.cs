#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldOutputPlanBuilder
    {
        private static string BuildDiagnostics(ScaffoldOutputPlanRequest request)
        {
            var discovery = request.Discovery;
            return ScaffoldDiagnosticsAdapter.ScaffoldDiagnostics(
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
                request.StringBuilderPool);
        }

        private static string? BuildDiagnosticsJson(
            ScaffoldOutputPlanRequest request,
            string diagnostics)
        {
            if (string.IsNullOrWhiteSpace(diagnostics))
                return null;

            var discovery = request.Discovery;
            return ScaffoldDiagnosticsAdapter.ScaffoldDiagnosticsJson(
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
                request.Composition.ManyToManyJoinTableKeys);
        }
    }
}
