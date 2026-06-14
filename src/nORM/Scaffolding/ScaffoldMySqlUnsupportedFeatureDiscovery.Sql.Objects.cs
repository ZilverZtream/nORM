namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
        private const string MySqlObjectFeatureSql = """
            SELECT NULL AS TableSchema, event_object_table AS TableName, trigger_name AS ObjectName, 'Trigger' AS Kind,
                CONCAT('MySQL trigger; timing=', action_timing, '; event=', event_manipulation, '; orientation=', action_orientation) AS Detail
            FROM information_schema.triggers
            WHERE trigger_schema = DATABASE()
            UNION ALL
            SELECT NULL, tc.table_name, tc.constraint_name, 'CheckConstraint', cc.check_clause
            FROM information_schema.table_constraints tc
            INNER JOIN information_schema.check_constraints cc
                ON cc.constraint_schema = tc.constraint_schema
               AND cc.constraint_name = tc.constraint_name
            WHERE tc.table_schema = DATABASE() AND tc.constraint_type = 'CHECK'
            """;

        private const string MySqlDefaultNamedCheckConstraintSql = """
            SELECT NULL AS TableSchema,
                   tc.table_name AS TableName,
                   tc.constraint_name AS ConstraintName
            FROM information_schema.table_constraints tc
            WHERE tc.table_schema = DATABASE()
              AND tc.constraint_type = 'CHECK'
              AND LOWER(LEFT(tc.constraint_name, CHAR_LENGTH(tc.table_name) + 5)) = LOWER(CONCAT(tc.table_name, '_chk_'))
              AND SUBSTRING(tc.constraint_name, CHAR_LENGTH(tc.table_name) + 6) REGEXP '^[0-9]+$'
            """;
    }
}
