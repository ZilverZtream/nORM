#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static string? SuggestedActionForScalarFeature(string kind)
            => kind switch
            {
                "Default" => "Generated code marks this type with [ReadOnlyEntity]. Move default semantics into application/model configuration, convert the default to emitted HasDefaultValueSql metadata, or keep provider DDL in migrations with a hand-modeled writable type.",
                "Computed" => "Use explicit HasComputedColumnSql model configuration or keep the provider-specific generated expression in migrations.",
                "CheckConstraint" => "Use explicit HasCheckConstraint model configuration or keep the provider-specific CHECK predicate in migrations.",
                "Collation" => "Keep collation-sensitive behavior in provider migrations and add explicit application/query tests before relying on generated code for comparisons or ordering.",
                "ProviderSpecificColumnType" => "Keep this provider-specific type behind explicit provider migrations/converters or remodel it to a portable CLR/database shape before claiming provider mobility.",
                "PrecisionScale" => "Parsed decimal precision/scale is emitted with HasPrecision. Review this unparsed numeric facet and add explicit model configuration or provider migration DDL before relying on generated decimal semantics.",
                "RowVersion" => "Keep provider-managed rowversion/timestamp DDL in migrations; scaffolded code marks the column as [Timestamp] and database-generated so generated writes and concurrency checks can use it.",
                "IdentityStrategy" => "Parsed SQL Server IDENTITY(seed, increment) metadata is scaffolded into HasIdentityOptions; unparsed provider-specific identity strategies make the generated type [ReadOnlyEntity] until key generation is hand-modeled.",
                _ => null
            };
    }
}
