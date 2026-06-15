#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static string? SuggestedActionForObjectOrTableFeature(string kind)
            => kind switch
            {
                "Trigger" => "Generated code marks this type with [ReadOnlyEntity]. Keep the trigger in provider migrations and add an explicit hand-modeled writable type only after testing side effects nORM cannot infer.",
                "TemporalTable" => "Choose provider-native temporal intentionally or migrate to nORM-managed temporal history; do not assume scaffolding round-trips native temporal DDL.",
                "MissingPrimaryKey" => "Generated code marks this type with [ReadOnlyEntity] so query materialization works but nORM writes are rejected; add a primary key before using generated writes or navigations.",
                _ => null
            };

        private static string? SuggestedActionForQueryObject(string kind)
            => kind switch
            {
                "View" => "Ordinary views scaffold as read-oriented query artifacts by default. Review this provider-specific skipped view metadata and keep it behind explicit provider-bound query code if it cannot be represented as a read-only entity.",
                "MaterializedView" => "Materialized views scaffold as read-oriented query artifacts by default. Keep refresh behavior provider-bound and review any provider-specific skipped metadata manually.",
                "VirtualTable" => "Select the virtual table explicitly with --table/--schema or use --emit-query-artifacts/ScaffoldOptions.EmitQueryArtifacts for a read-oriented query artifact, or keep the virtual table behind provider-bound query/index code.",
                "VirtualTableShadow" => "Do not map SQLite virtual-table shadow storage as domain entities; keep it provider-owned with the virtual table.",
                _ => null
            };

        private static string? SuggestedActionForRoutineObject(string kind)
            => kind switch
            {
                "Routine" => "Keep routine calls behind explicit raw SQL/stored-procedure code and document the provider-bound contract.",
                "Event" => "Keep scheduled event behavior in provider operations/migrations; v1 scaffolding emits table models only.",
                _ => null
            };

        private static string? SuggestedActionForProviderObject(string kind)
            => kind switch
            {
                "Sequence" => "Configure generated-key behavior explicitly or keep sequence DDL in provider migrations.",
                "Synonym" => "Select local table/view synonyms explicitly with --table/--schema, use --emit-query-artifacts, resolve the synonym to a supported base table, or keep non-query/remote synonyms behind provider-bound integration code.",
                _ => null
            };
    }
}
