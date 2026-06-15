namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private static string FunctionSkippedObjectSql
            => string.Concat(
                FunctionSkippedObjectHeaderSql,
                FunctionParameterModesSql,
                FunctionDataTypeSql,
                FunctionResultColumnsSql,
                FunctionSkippedObjectSourceSql,
                FunctionSkippedObjectFilterSql);

        private const string FunctionSkippedObjectHeaderSql = """
            SELECT SCHEMA_NAME(o.schema_id), o.name, 'Routine',
                   CONCAT('SQL Server ',
                          CASE
                              WHEN o.type IN ('IF', 'TF') THEN 'table-valued function'
                              ELSE 'scalar function'
                          END,
                          '; parameters=',
                          (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = o.object_id AND pa.parameter_id > 0),
                          '; outputParameters=',
                          CASE WHEN o.type = 'FN' THEN 1 ELSE 0 END,
                          '; callShape=',
                          CASE
                              WHEN o.type IN ('IF', 'TF') THEN 'table-valued-function'
                              ELSE 'scalar-function'
                          END,
                          '; parameterModes=',
            """;
    }
}
