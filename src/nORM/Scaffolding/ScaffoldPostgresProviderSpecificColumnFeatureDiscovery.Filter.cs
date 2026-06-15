namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresProviderSpecificColumnFeatureDiscovery
    {
        private const string ProviderSpecificColumnFallbackDetailSql = """
                    WHEN udt_name IS NULL OR udt_name = '' THEN data_type
                    ELSE data_type || ' (' || udt_name || ')'
                END)::text AS Detail
            FROM information_schema.columns
            """;

        private const string ProviderSpecificColumnFilterSql = """
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND (
                  domain_name IS NOT NULL
                  OR
                  data_type IN ('ARRAY', 'USER-DEFINED', 'json', 'jsonb', 'xml')
                  OR udt_name IN ('json', 'jsonb', 'inet', 'cidr', 'macaddr', 'macaddr8', 'tsvector', 'tsquery')
              )
            """;
    }
}
