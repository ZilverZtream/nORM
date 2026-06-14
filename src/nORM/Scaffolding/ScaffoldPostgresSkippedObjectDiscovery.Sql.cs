#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresSkippedObjectDiscovery
    {
        private static string GetSkippedObjectSql()
            => string.Join(
                "\nUNION ALL\n",
                ViewSkippedObjectSql,
                SequenceSkippedObjectSql,
                MaterializedViewSkippedObjectSql,
                RoutineSkippedObjectSql)
            + "\nORDER BY ObjectSchema, ObjectName";

        private const string ViewSkippedObjectSql = """
            SELECT table_schema AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'PostgreSQL view' AS Detail
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            """;

        private const string SequenceSkippedObjectSql = """
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
            """;

        private const string MaterializedViewSkippedObjectSql = """
            SELECT schemaname, matviewname, 'MaterializedView', 'PostgreSQL materialized view'
            FROM pg_matviews
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            """;
    }
}
