#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresSkippedObjectDiscovery
    {
        private const string RoutineSkippedObjectSourceSql = """

            FROM information_schema.routines r
            INNER JOIN pg_namespace routine_ns ON routine_ns.nspname = r.specific_schema
            INNER JOIN pg_proc routine_proc ON routine_proc.pronamespace = routine_ns.oid
              AND routine_proc.proname = r.routine_name
              AND RIGHT(r.specific_name, LENGTH(routine_proc.oid::text) + 1) = '_' || routine_proc.oid::text
            """;

        private const string RoutineSkippedObjectFilterSql = """

            WHERE r.routine_schema NOT IN ('pg_catalog', 'information_schema')
              AND routine_proc.prokind IN ('f', 'p')
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_depend extension_dependency
                  WHERE extension_dependency.classid = 'pg_catalog.pg_proc'::regclass
                    AND extension_dependency.objid = routine_proc.oid
                    AND extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
                    AND extension_dependency.deptype = 'e'
              )
            """;
    }
}
