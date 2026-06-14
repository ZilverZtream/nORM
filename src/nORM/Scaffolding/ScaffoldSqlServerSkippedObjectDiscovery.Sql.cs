#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string ViewSkippedObjectSql = """
            SELECT SCHEMA_NAME(v.schema_id) AS ObjectSchema, v.name AS ObjectName, 'View' AS Kind, 'SQL Server view' AS Detail
            FROM sys.views v
            WHERE v.is_ms_shipped = 0
            """;

        private const string SequenceSkippedObjectSql = """
            SELECT SCHEMA_NAME(s.schema_id), s.name, 'Sequence',
                   CONCAT('SQL Server sequence; dataType=', ty.name,
                          CASE
                              WHEN ty.name IN ('decimal', 'numeric') THEN CONCAT('(', s.precision, ',', s.scale, ')')
                              ELSE ''
                          END)
            FROM sys.sequences s
            INNER JOIN sys.types ty ON ty.user_type_id = s.user_type_id
            """;

        private const string SynonymSkippedObjectSql = """
            SELECT SCHEMA_NAME(s.schema_id), s.name, 'Synonym',
                   CONCAT('SQL Server synonym; baseObject=', s.base_object_name,
                          '; baseType=', COALESCE(CONVERT(nvarchar(20), OBJECTPROPERTYEX(OBJECT_ID(s.base_object_name), 'BaseType')), ''))
            FROM sys.synonyms s
            """;
    }
}
