#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static string? SuggestedActionForIndexFeature(string kind)
            => kind switch
            {
                "PartialIndex" => "Ordinary SQL Server/PostgreSQL/SQLite filtered and partial column indexes are emitted with IndexAttribute.FilterSql, and supported expression-index predicates are emitted with HasExpressionIndex filter metadata. Keep this remaining filtered/partial shape in provider migrations when it cannot be attached safely to generated index metadata.",
                "ExpressionIndex" => "SQLite, ordinary PostgreSQL B-tree, and MySQL expression indexes are emitted with HasExpressionIndex when their key/filter shape is representable, including PostgreSQL expression-index INCLUDE and null-semantics facets. Keep this remaining expression-index shape in provider migrations or replace it with a generated column plus an ordinary index.",
                "IncludedColumnIndex" => "Ordinary SQL Server/PostgreSQL included-column indexes are emitted with IndexAttribute.IsIncluded, and PostgreSQL expression-index INCLUDE columns are emitted with HasExpressionIndex when the DDL exposes exact column names. Keep this diagnostic in provider migrations only when the included-column facet cannot be attached safely to generated index metadata.",
                "DescendingIndex" => "Review this descending index shape; ordinary column-key descending indexes are generated, but this one was not safe to map as provider-neutral index metadata.",
                "PrefixIndex" => "Keep the MySQL prefix index in provider migrations; prefix uniqueness is not full-column uniqueness and is not used for generated alternate-key relationships.",
                "ProviderSpecificIndex" => "Ordinary B-tree/rowstore column-index metadata is emitted when it is representable, including supported filters, included columns, PostgreSQL null ordering, and NULLS NOT DISTINCT uniqueness. Keep this remaining provider-specific index implementation in migrations or remodel it with supported generated metadata where equivalent.",
                _ => null
            };
    }
}
