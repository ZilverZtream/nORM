#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresSkippedObjectDiscovery
    {
        private const string RoutineCallShapeSql = """

                   '; callShape=' ||
                   CASE
                       WHEN UPPER(r.routine_type) = 'FUNCTION' AND routine_proc.proretset THEN 'table-valued-function'
                       WHEN UPPER(r.routine_type) = 'FUNCTION' AND LOWER(COALESCE(r.data_type, '')) IN ('record', 'table') THEN 'table-valued-function'
                       WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                       ELSE ''
                   END ||
                   '; dataType=' || COALESCE(r.data_type, '')
            """;
    }
}
