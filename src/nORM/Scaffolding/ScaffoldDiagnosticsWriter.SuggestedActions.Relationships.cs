#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static string? SuggestedActionForRelationshipFeature(string kind)
            => kind switch
            {
                "ReferentialAction" => "Generated navigations are suppressed for this FK. Review the provider-specific referential action token and add explicit relationship configuration only after preserving its semantics.",
                "RelationshipPrincipalKey" => "Add a primary key or exact ordered unfiltered unique index for the referenced principal columns, or configure the relationship manually before relying on generated navigations.",
                "RelationshipDependentKey" => "Add a primary key to the dependent table before relying on generated navigations/includes, or keep the keyless type read-only and configure explicit query projections.",
                _ => null
            };
    }
}
