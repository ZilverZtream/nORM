namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificColumnFeatureDiscovery
    {
        private const string ProviderSpecificDomainColumnDetailSql = """
                    WHEN domain_name IS NOT NULL AND domain_name <> ''
                         AND data_type = 'USER-DEFINED'
                         AND EXISTS (
                             SELECT 1
                             FROM pg_type enum_type
                             INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                             WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                               AND enum_type.typname = udt_name
                               AND enum_type.typtype = 'e'
                         )
                    THEN 'DOMAIN (' ||
                         CASE WHEN domain_schema IS NOT NULL AND domain_schema <> '' THEN domain_schema || '.' ELSE '' END ||
                         domain_name || ' -> ENUM (' ||
                         CASE WHEN udt_schema IS NOT NULL AND udt_schema <> '' THEN udt_schema || '.' ELSE '' END ||
                         udt_name || ': ' ||
                         COALESCE((
                             SELECT string_agg(quote_literal(enum_value.enumlabel), ',' ORDER BY enum_value.enumsortorder)
                             FROM pg_type enum_type
                             INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                             INNER JOIN pg_enum enum_value ON enum_value.enumtypid = enum_type.oid
                             WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                               AND enum_type.typname = udt_name
                               AND enum_type.typtype = 'e'
                         ), '') ||
                         '))'
                    WHEN domain_name IS NOT NULL AND domain_name <> ''
                    THEN 'DOMAIN (' ||
                         CASE WHEN domain_schema IS NOT NULL AND domain_schema <> '' THEN domain_schema || '.' ELSE '' END ||
                         domain_name || ' -> ' ||
                         CASE
                             WHEN data_type IN ('ARRAY', 'USER-DEFINED') AND udt_name IS NOT NULL AND udt_name <> ''
                             THEN data_type || ' (' || udt_name || ')'
                             ELSE data_type
                         END ||
                         CASE
                             WHEN data_type IN ('character varying', 'character') AND character_maximum_length IS NOT NULL
                             THEN '(' || character_maximum_length::text || ')'
                             WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL AND numeric_scale IS NULL
                             THEN '(' || numeric_precision::text || ')'
                             WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL
                             THEN '(' || numeric_precision::text || ',' || numeric_scale::text || ')'
                             ELSE ''
                         END ||
                         ')'
            """;
    }
}
