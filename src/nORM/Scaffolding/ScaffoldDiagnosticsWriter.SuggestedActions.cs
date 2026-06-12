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
            => kind switch
            {
                "Default" => "Generated code marks this type with [ReadOnlyEntity]. Move default semantics into application/model configuration, convert the default to emitted HasDefaultValueSql metadata, or keep provider DDL in migrations with a hand-modeled writable type.",
                "Computed" => "Use explicit HasComputedColumnSql model configuration or keep the provider-specific generated expression in migrations.",
                "CheckConstraint" => "Use explicit HasCheckConstraint model configuration or keep the provider-specific CHECK predicate in migrations.",
                "Collation" => "Keep collation-sensitive behavior in provider migrations and add explicit application/query tests before relying on generated code for comparisons or ordering.",
                "ProviderSpecificColumnType" => "Keep this provider-specific type behind explicit provider migrations/converters or remodel it to a portable CLR/database shape before claiming provider mobility.",
                "ReferentialAction" => "Generated navigations are suppressed for this FK. Review the provider-specific referential action token and add explicit relationship configuration only after preserving its semantics.",
                "RelationshipPrincipalKey" => "Add a primary key or exact ordered unfiltered unique index for the referenced principal columns, or configure the relationship manually before relying on generated navigations.",
                "RelationshipDependentKey" => "Add a primary key to the dependent table before relying on generated navigations/includes, or keep the keyless type read-only and configure explicit query projections.",
                "RowVersion" => "Keep provider-managed rowversion/timestamp semantics in migrations; scaffolded code marks the column as [Timestamp] and database-generated but cannot recreate provider DDL.",
                "IdentityStrategy" => "Parsed SQL Server IDENTITY(seed, increment) metadata is scaffolded into HasIdentityOptions; unparsed provider-specific identity strategies make the generated type [ReadOnlyEntity] until key generation is hand-modeled.",
                "Trigger" => "Generated code marks this type with [ReadOnlyEntity]. Keep the trigger in provider migrations and add an explicit hand-modeled writable type only after testing side effects nORM cannot infer.",
                "PartialIndex" => "Ordinary SQL Server/PostgreSQL/SQLite filtered and partial column indexes are emitted with IndexAttribute.FilterSql, and supported expression-index predicates are emitted with HasExpressionIndex filter metadata. Keep this remaining filtered/partial shape in provider migrations when it cannot be attached safely to generated index metadata.",
                "ExpressionIndex" => "SQLite, ordinary PostgreSQL B-tree, and MySQL expression indexes are emitted with HasExpressionIndex when their key/filter shape is representable, including PostgreSQL expression-index INCLUDE and null-semantics facets. Keep this remaining expression-index shape in provider migrations or replace it with a generated column plus an ordinary index.",
                "IncludedColumnIndex" => "Ordinary SQL Server/PostgreSQL included-column indexes are emitted with IndexAttribute.IsIncluded, and PostgreSQL expression-index INCLUDE columns are emitted with HasExpressionIndex when the DDL exposes exact column names. Keep this diagnostic in provider migrations only when the included-column facet cannot be attached safely to generated index metadata.",
                "DescendingIndex" => "Review this descending index shape; ordinary column-key descending indexes are generated, but this one was not safe to map as provider-neutral index metadata.",
                "PrefixIndex" => "Keep the MySQL prefix index in provider migrations; prefix uniqueness is not full-column uniqueness and is not used for generated alternate-key relationships.",
                "ProviderSpecificIndex" => "Ordinary B-tree/rowstore column-index metadata is emitted when it is representable, including supported filters, included columns, PostgreSQL null ordering, and NULLS NOT DISTINCT uniqueness. Keep this remaining provider-specific index implementation in migrations or remodel it with supported generated metadata where equivalent.",
                "TemporalTable" => "Choose provider-native temporal intentionally or migrate to nORM-managed temporal history; do not assume scaffolding round-trips native temporal DDL.",
                "MissingPrimaryKey" => "Generated code marks this type with [ReadOnlyEntity] so query materialization works but nORM writes are rejected; add a primary key before using generated writes or navigations.",
                _ => "Review the provider-owned object and add explicit model configuration or migration code for the intended behavior."
            };

        public static string SuggestedActionForSkippedObject(string kind)
            => kind switch
            {
                "View" => "Ordinary views scaffold as read-oriented query artifacts by default. Review this provider-specific skipped view metadata and keep it behind explicit provider-bound query code if it cannot be represented as a read-only entity.",
                "Routine" => "Keep routine calls behind explicit raw SQL/stored-procedure code and document the provider-bound contract.",
                "Sequence" => "Configure generated-key behavior explicitly or keep sequence DDL in provider migrations.",
                "Synonym" => "Select local table/view synonyms explicitly with --table/--schema, use --emit-query-artifacts, resolve the synonym to a supported base table, or keep non-query/remote synonyms behind provider-bound integration code.",
                "MaterializedView" => "Materialized views scaffold as read-oriented query artifacts by default. Keep refresh behavior provider-bound and review any provider-specific skipped metadata manually.",
                "Event" => "Keep scheduled event behavior in provider operations/migrations; v1 scaffolding emits table models only.",
                "VirtualTable" => "Select the virtual table explicitly with --table/--schema or use --emit-query-artifacts/ScaffoldOptions.EmitQueryArtifacts for a read-oriented query artifact, or keep the virtual table behind provider-bound query/index code.",
                "VirtualTableShadow" => "Do not map SQLite virtual-table shadow storage as domain entities; keep it provider-owned with the virtual table.",
                _ => "Keep this database object in provider migrations or hand-written integration code."
            };
    }
}
