namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificColumnFeatureDiscovery
    {
        private const string ProviderSpecificEnumColumnDetailSql = """
                    WHEN data_type = 'USER-DEFINED'
                         AND EXISTS (
                             SELECT 1
                             FROM pg_type enum_type
                             INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                             WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                               AND enum_type.typname = udt_name
                               AND enum_type.typtype = 'e'
                         )
                    THEN 'ENUM (' ||
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
                         ')'
            """;
    }
}
