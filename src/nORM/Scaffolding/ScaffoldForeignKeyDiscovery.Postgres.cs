#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private const string PostgresForeignKeySql = """
            SELECT
                dep_ns.nspname AS DependentSchema,
                dep.relname AS DependentTable,
                dep_att.attname AS DependentColumn,
                principal_ns.nspname AS PrincipalSchema,
                principal.relname AS PrincipalTable,
                principal_att.attname AS PrincipalColumn,
                con.conname AS ConstraintName,
                CASE
                    WHEN con.conname = LEFT(
                        dep.relname || '_' ||
                        (
                            SELECT string_agg(dep_name.attname, '_' ORDER BY dep_key.ord)
                            FROM unnest(con.conkey) WITH ORDINALITY AS dep_key(attnum, ord)
                            INNER JOIN pg_attribute dep_name
                                ON dep_name.attrelid = dep.oid
                               AND dep_name.attnum = dep_key.attnum
                        ) || '_fkey',
                        63)
                    THEN true ELSE false
                END AS IsSyntheticConstraintName,
                array_length(con.conkey, 1) AS ColumnCount,
                CASE con.confdeltype
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                    WHEN 'r' THEN 'RESTRICT'
                    ELSE 'NO ACTION'
                END AS OnDelete,
                CASE con.confupdtype
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                    WHEN 'r' THEN 'RESTRICT'
                    ELSE 'NO ACTION'
                END ||
                CASE con.confmatchtype
                    WHEN 'f' THEN ' MATCH FULL'
                    WHEN 'p' THEN ' MATCH PARTIAL'
                    ELSE ''
                END ||
                CASE
                    WHEN con.condeferrable AND con.condeferred THEN ' DEFERRABLE INITIALLY DEFERRED'
                    WHEN con.condeferrable THEN ' DEFERRABLE INITIALLY IMMEDIATE'
                    ELSE ''
                END AS OnUpdate
            FROM pg_constraint con
            INNER JOIN pg_class dep ON dep.oid = con.conrelid
            INNER JOIN pg_namespace dep_ns ON dep_ns.oid = dep.relnamespace
            INNER JOIN pg_class principal ON principal.oid = con.confrelid
            INNER JOIN pg_namespace principal_ns ON principal_ns.oid = principal.relnamespace
            INNER JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS key_pair(dep_attnum, principal_attnum, ord) ON true
            INNER JOIN pg_attribute dep_att ON dep_att.attrelid = dep.oid AND dep_att.attnum = key_pair.dep_attnum
            INNER JOIN pg_attribute principal_att ON principal_att.attrelid = principal.oid AND principal_att.attnum = key_pair.principal_attnum
            WHERE con.contype = 'f'
              AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY dep_ns.nspname, dep.relname, con.conname, key_pair.ord
            """;
    }
}
