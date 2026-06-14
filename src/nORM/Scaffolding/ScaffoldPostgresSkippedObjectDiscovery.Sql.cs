#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresSkippedObjectDiscovery
    {
        private const string SkippedObjectSql = """
            SELECT table_schema AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'PostgreSQL view' AS Detail
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT sequence_schema, sequence_name, 'Sequence', 'PostgreSQL sequence; dataType=' || data_type
            FROM information_schema.sequences seq
            WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_class sequence_class
                  INNER JOIN pg_namespace sequence_schema_ns ON sequence_schema_ns.oid = sequence_class.relnamespace
                  INNER JOIN pg_depend dependency ON dependency.objid = sequence_class.oid
                  WHERE sequence_class.relkind = 'S'
                    AND sequence_schema_ns.nspname = seq.sequence_schema
                    AND sequence_class.relname = seq.sequence_name
                    AND dependency.deptype IN ('a', 'i')
              )
            UNION ALL
            SELECT schemaname, matviewname, 'MaterializedView', 'PostgreSQL materialized view'
            FROM pg_matviews
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT r.routine_schema, r.routine_name, 'Routine',
                   'PostgreSQL ' || LOWER(r.routine_type) || '; parameters=' ||
                   COALESCE((
                       SELECT COUNT(*)
                       FROM information_schema.parameters p
                       WHERE p.specific_schema = r.specific_schema
                         AND p.specific_name = r.specific_name
                         AND p.parameter_mode IS NOT NULL
                   ), 0)::text ||
                   '; outputParameters=' ||
                   COALESCE((
                       SELECT COUNT(*)
                       FROM information_schema.parameters p
                       WHERE p.specific_schema = r.specific_schema
                         AND p.specific_name = r.specific_name
                         AND p.parameter_mode IN ('OUT', 'INOUT')
                   ), 0)::text ||
                   '; parameterModes=' ||
                   COALESCE((
                       SELECT string_agg(
                           COALESCE(p.parameter_name, 'return') || ':' || COALESCE(p.parameter_mode, 'RETURN') || ':' ||
                           CASE
                               WHEN p.data_type = 'USER-DEFINED'
                                    AND p.udt_name IS NOT NULL
                                    AND p.udt_name <> ''
                                    AND EXISTS (
                                        SELECT 1
                                        FROM pg_type domain_type
                                        INNER JOIN pg_namespace domain_ns ON domain_ns.oid = domain_type.typnamespace
                                        WHERE domain_ns.nspname = COALESCE(p.udt_schema, p.specific_schema)
                                          AND domain_type.typname = p.udt_name
                                          AND domain_type.typtype = 'd'
                                    )
                               THEN 'DOMAIN (' ||
                                    CASE WHEN p.udt_schema IS NOT NULL AND p.udt_schema <> '' THEN p.udt_schema || '.' ELSE '' END ||
                                    p.udt_name || ' -> ' ||
                                    COALESCE((
                                        SELECT CASE
                                            WHEN base_type.typtype = 'e'
                                            THEN 'ENUM (' ||
                                                 CASE WHEN base_ns.nspname IS NOT NULL AND base_ns.nspname <> '' THEN base_ns.nspname || '.' ELSE '' END ||
                                                 base_type.typname || ': ' ||
                                                 COALESCE((
                                                     SELECT string_agg(quote_literal(enum_value.enumlabel), ',' ORDER BY enum_value.enumsortorder)
                                                     FROM pg_enum enum_value
                                                     WHERE enum_value.enumtypid = base_type.oid
                                                 ), '') ||
                                                 ')'
                                            ELSE pg_catalog.format_type(domain_type.typbasetype, domain_type.typtypmod)
                                        END
                                        FROM pg_type domain_type
                                        INNER JOIN pg_namespace domain_ns ON domain_ns.oid = domain_type.typnamespace
                                        INNER JOIN pg_type base_type ON base_type.oid = domain_type.typbasetype
                                        INNER JOIN pg_namespace base_ns ON base_ns.oid = base_type.typnamespace
                                        WHERE domain_ns.nspname = COALESCE(p.udt_schema, p.specific_schema)
                                          AND domain_type.typname = p.udt_name
                                          AND domain_type.typtype = 'd'
                                    ), '') ||
                                    ')'
                               ELSE
                                   CASE
                                       WHEN p.data_type IN ('ARRAY', 'USER-DEFINED')
                                            AND p.udt_name IS NOT NULL
                                            AND p.udt_name <> ''
                                       THEN p.data_type || ' (' || p.udt_name || ')'
                                       ELSE COALESCE(p.data_type, '')
                                   END ||
                                   CASE
                                       WHEN p.character_maximum_length IS NOT NULL THEN '(' || p.character_maximum_length::text || ')'
                                       WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NULL THEN '(' || p.numeric_precision::text || ')'
                                       WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NOT NULL THEN '(' || p.numeric_precision::text || ',' || p.numeric_scale::text || ')'
                                       ELSE ''
                                   END
                           END,
                           ',' ORDER BY p.ordinal_position)
                       FROM information_schema.parameters p
                       WHERE p.specific_schema = r.specific_schema
                         AND p.specific_name = r.specific_name
                         AND p.parameter_mode IS NOT NULL
                   ), '') ||
                   '; callShape=' ||
                   CASE
                       WHEN UPPER(r.routine_type) = 'FUNCTION' AND EXISTS (
                           SELECT 1
                           FROM pg_proc routine_proc
                           INNER JOIN pg_namespace routine_ns ON routine_ns.oid = routine_proc.pronamespace
                           WHERE routine_ns.nspname = r.specific_schema
                             AND routine_proc.proname = r.routine_name
                             AND routine_proc.proretset
                       ) THEN 'table-valued-function'
                       WHEN UPPER(r.routine_type) = 'FUNCTION' AND LOWER(COALESCE(r.data_type, '')) IN ('record', 'table') THEN 'table-valued-function'
                       WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                       ELSE ''
                   END ||
                   '; dataType=' || COALESCE(r.data_type, '')
            FROM information_schema.routines r
            WHERE r.routine_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY ObjectSchema, ObjectName
            """;
    }
}
