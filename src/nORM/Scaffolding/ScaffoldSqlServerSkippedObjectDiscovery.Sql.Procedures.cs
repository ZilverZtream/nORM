#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private static string StoredProcedureSkippedObjectSql
            => string.Concat(
                StoredProcedureSkippedObjectHeaderSql,
                StoredProcedureParameterModesSql,
                StoredProcedureResultColumnsSql,
                StoredProcedureSkippedObjectSourceSql,
                StoredProcedureSkippedObjectFilterSql);

        private const string StoredProcedureSkippedObjectHeaderSql = """
            SELECT SCHEMA_NAME(p.schema_id), p.name, 'Routine',
                   CONCAT('SQL Server stored procedure; parameters=',
                          (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = p.object_id),
                          '; outputParameters=',
                          (SELECT COUNT(*) + 1 FROM sys.parameters pa WHERE pa.object_id = p.object_id AND pa.is_output = 1),
                          '; parameterModes=',
            """;
    }
}
