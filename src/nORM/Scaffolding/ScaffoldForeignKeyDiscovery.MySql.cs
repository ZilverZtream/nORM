#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
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
