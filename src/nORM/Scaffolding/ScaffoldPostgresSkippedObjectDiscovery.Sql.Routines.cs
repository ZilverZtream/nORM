#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresSkippedObjectDiscovery
    {
        private static string RoutineSkippedObjectSql
            => string.Concat(
                RoutineSkippedObjectHeaderSql,
                RoutineParameterModesSql,
                RoutineCallShapeSql,
                RoutineSkippedObjectSourceSql,
                RoutineSkippedObjectFilterSql);

        private const string RoutineSkippedObjectHeaderSql = """
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
            """;
    }
}
