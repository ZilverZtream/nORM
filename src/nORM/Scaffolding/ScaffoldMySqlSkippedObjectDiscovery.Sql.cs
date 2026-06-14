#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlSkippedObjectDiscovery
    {
        private static string GetSkippedObjectSql()
            => string.Join(
                "\nUNION ALL\n",
                ViewSkippedObjectSql,
                RoutineSkippedObjectSql,
                EventSkippedObjectSql)
            + "\nORDER BY ObjectSchema, ObjectName";

        private const string ViewSkippedObjectSql = """
            SELECT NULL AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'MySQL view' AS Detail
            FROM information_schema.views
            WHERE table_schema = DATABASE()
            """;

        private const string RoutineSkippedObjectSql = """
            SELECT NULL, r.routine_name, 'Routine',
                   CONCAT('MySQL ', r.routine_type, '; parameters=',
                          (SELECT COUNT(*)
                           FROM information_schema.parameters p
                           WHERE p.specific_schema = r.routine_schema
                             AND p.specific_name = r.specific_name
                             AND p.parameter_mode IS NOT NULL),
                          '; outputParameters=',
                          (SELECT COUNT(*)
                           FROM information_schema.parameters p
                           WHERE p.specific_schema = r.routine_schema
                             AND p.specific_name = r.specific_name
                             AND p.parameter_mode IN ('OUT', 'INOUT')),
                          '; parameterModes=',
                          COALESCE((SELECT GROUP_CONCAT(CONCAT(
                                        COALESCE(p.parameter_name, 'return'), ':', COALESCE(p.parameter_mode, 'RETURN'), ':',
                                        CASE
                                            WHEN LOWER(COALESCE(p.dtd_identifier, '')) LIKE '%unsigned%' THEN COALESCE(p.dtd_identifier, p.data_type, '')
                                            ELSE COALESCE(p.data_type, '')
                                        END,
                                        CASE
                                            WHEN LOWER(COALESCE(p.dtd_identifier, '')) LIKE '%unsigned%' THEN ''
                                            WHEN p.character_maximum_length IS NOT NULL THEN CONCAT('(', p.character_maximum_length, ')')
                                            WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NULL THEN CONCAT('(', p.numeric_precision, ')')
                                            WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NOT NULL THEN CONCAT('(', p.numeric_precision, ',', p.numeric_scale, ')')
                                            ELSE ''
                                        END) ORDER BY p.ordinal_position SEPARATOR ',')
                                    FROM information_schema.parameters p
                                    WHERE p.specific_schema = r.routine_schema
                                      AND p.specific_name = r.specific_name
                                      AND p.parameter_mode IS NOT NULL), ''),
                          '; callShape=',
                          CASE
                              WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                              ELSE ''
                          END,
                          '; dataType=', COALESCE(r.data_type, ''))
            FROM information_schema.routines r
            WHERE r.routine_schema = DATABASE()
            """;

        private const string EventSkippedObjectSql = """
            SELECT NULL, event_name, 'Event',
                   CONCAT('MySQL event; eventType=', COALESCE(event_type, ''),
                          '; status=', COALESCE(status, ''),
                          '; intervalValue=', COALESCE(interval_value, ''),
                          '; intervalField=', COALESCE(interval_field, ''),
                          '; executeAt=', COALESCE(DATE_FORMAT(execute_at, '%Y-%m-%d %H:%i:%s'), ''),
                          '; starts=', COALESCE(DATE_FORMAT(starts, '%Y-%m-%d %H:%i:%s'), ''),
                          '; ends=', COALESCE(DATE_FORMAT(ends, '%Y-%m-%d %H:%i:%s'), ''))
            FROM information_schema.events
            WHERE event_schema = DATABASE()
            """;
    }
}
