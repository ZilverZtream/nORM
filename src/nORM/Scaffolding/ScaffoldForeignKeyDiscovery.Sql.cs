#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private const string SqlServerForeignKeySql = """
            SELECT
                SCHEMA_NAME(dep.schema_id) AS DependentSchema,
                dep.name AS DependentTable,
                dep_col.name AS DependentColumn,
                SCHEMA_NAME(principal.schema_id) AS PrincipalSchema,
                principal.name AS PrincipalTable,
                principal_col.name AS PrincipalColumn,
                fk.name AS ConstraintName,
                fk.is_system_named AS IsSyntheticConstraintName,
                COUNT(*) OVER (PARTITION BY fk.object_id) AS ColumnCount,
                fk.delete_referential_action_desc AS OnDelete,
                fk.update_referential_action_desc AS OnUpdate
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            INNER JOIN sys.tables dep ON dep.object_id = fk.parent_object_id
            INNER JOIN sys.columns dep_col ON dep_col.object_id = dep.object_id AND dep_col.column_id = fkc.parent_column_id
            INNER JOIN sys.tables principal ON principal.object_id = fk.referenced_object_id
            INNER JOIN sys.columns principal_col ON principal_col.object_id = principal.object_id AND principal_col.column_id = fkc.referenced_column_id
            WHERE dep.is_ms_shipped = 0 AND principal.is_ms_shipped = 0
            ORDER BY SCHEMA_NAME(dep.schema_id), dep.name, fk.name, fkc.constraint_column_id
            """;

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

        private const string MySqlForeignKeySql = """
            SELECT
                NULL AS DependentSchema,
                kcu.table_name AS DependentTable,
                kcu.column_name AS DependentColumn,
                NULL AS PrincipalSchema,
                kcu.referenced_table_name AS PrincipalTable,
                kcu.referenced_column_name AS PrincipalColumn,
                kcu.constraint_name AS ConstraintName,
                CASE
                    WHEN LOWER(LEFT(kcu.constraint_name, CHAR_LENGTH(kcu.table_name) + 6)) = LOWER(CONCAT(kcu.table_name, '_ibfk_'))
                     AND SUBSTRING(kcu.constraint_name, CHAR_LENGTH(kcu.table_name) + 7) REGEXP '^[0-9]+$'
                    THEN 1 ELSE 0
                END AS IsSyntheticConstraintName,
                COUNT(*) OVER (PARTITION BY kcu.constraint_schema, kcu.table_name, kcu.constraint_name) AS ColumnCount,
                rc.delete_rule AS OnDelete,
                rc.update_rule AS OnUpdate
            FROM information_schema.key_column_usage kcu
            INNER JOIN information_schema.referential_constraints rc
                ON rc.constraint_schema = kcu.constraint_schema
               AND rc.constraint_name = kcu.constraint_name
               AND rc.table_name = kcu.table_name
            WHERE kcu.table_schema = DATABASE()
              AND kcu.referenced_table_name IS NOT NULL
            ORDER BY kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position
            """;
    }
}
