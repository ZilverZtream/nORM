#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        public static string SuggestedActionForCompositeForeignKey()
            => "Keep scalar columns and add the composite relationship manually, or simplify the relationship to a single-column surrogate key before relying on generated navigations.";

        public static string SuggestedActionForPossibleJoinTable()
            => "If this is a safe pure join table, verify all FK columns are NOT NULL, both FKs target generated primary keys or exact ordered unfiltered unique indexes, and the bridge uses either an FK-column primary key or a generated surrogate primary key plus an exact unfiltered unique index over the FK columns; then use the generated UsingTable mapping. Keep payload, duplicate-pair, or domain-behavior bridges as explicit join entities.";

        public static string SuggestedActionForUnsupportedFeature(string kind)
            => SuggestedActionForRelationshipFeature(kind)
                ?? SuggestedActionForIndexFeature(kind)
                ?? SuggestedActionForObjectOrTableFeature(kind)
                ?? SuggestedActionForScalarFeature(kind)
                ?? "Review the provider-owned object and add explicit model configuration or migration code for the intended behavior.";

        public static string SuggestedActionForSkippedObject(string kind)
            => SuggestedActionForQueryObject(kind)
                ?? SuggestedActionForRoutineObject(kind)
                ?? SuggestedActionForProviderObject(kind)
                ?? "Keep this database object in provider migrations or hand-written integration code.";
    }
}
