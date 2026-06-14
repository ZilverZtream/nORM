#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityProviderTypeMetadataReader
    {
        private const string PostgresDomainColumnCastTypeSql = """
            SELECT column_name AS ColumnName,
                   CASE
                       WHEN data_type = 'USER-DEFINED'
                            AND EXISTS (
                                SELECT 1
                                FROM pg_type enum_type
                                INNER JOIN pg_namespace enum_ns ON enum_ns.oid = enum_type.typnamespace
                                WHERE enum_ns.nspname = COALESCE(udt_schema, table_schema)
                                  AND enum_type.typname = udt_name
                                  AND enum_type.typtype = 'e'
                            )
                       THEN 'text'
                       WHEN data_type IN ('ARRAY', 'USER-DEFINED')
                            AND udt_name IS NOT NULL
                            AND udt_name <> ''
                       THEN data_type || ' (' || udt_name || ')'
                       WHEN data_type IN ('character varying', 'character')
                            AND character_maximum_length IS NOT NULL
                       THEN data_type || '(' || character_maximum_length::text || ')'
                       WHEN data_type = 'numeric'
                            AND numeric_precision IS NOT NULL
                            AND numeric_scale IS NULL
                       THEN data_type || '(' || numeric_precision::text || ')'
                       WHEN data_type = 'numeric'
                            AND numeric_precision IS NOT NULL
                            AND numeric_scale IS NOT NULL
                       THEN data_type || '(' || numeric_precision::text || ',' || numeric_scale::text || ')'
                       ELSE data_type
                   END AS DataType
            FROM information_schema.columns
            WHERE table_name = @tableName
              AND (@schemaName IS NULL OR table_schema = @schemaName)
              AND (
                  domain_name IS NOT NULL
                  OR data_type = 'USER-DEFINED'
                  OR (
                      data_type = 'ARRAY'
                      AND COALESCE(udt_name, '') IN (
                          '_int2',
                          '_int4',
                          '_int8',
                          '_float4',
                          '_float8',
                          '_numeric',
                          '_bool',
                          '_uuid',
                          '_text',
                          '_varchar',
                          '_bpchar',
                          '_citext',
                          '_bytea',
                          '_date',
                          '_time',
                          '_timetz',
                          '_interval',
                          '_timestamp',
                          '_timestamptz'
                      )
                  )
              )
            """;
    }
}
