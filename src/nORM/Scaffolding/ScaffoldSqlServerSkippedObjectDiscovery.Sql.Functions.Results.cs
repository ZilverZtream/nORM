namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string FunctionDataTypeSql = """
                          '; dataType=',
                          COALESCE((
                              SELECT TOP (1) ty.name
                              FROM sys.parameters pa
                              INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                              WHERE pa.object_id = o.object_id
                                AND pa.parameter_id = 0
                          ), CASE WHEN o.type IN ('IF', 'TF') THEN 'TABLE' ELSE '' END),
            """;

        private const string FunctionResultColumnsSql = """
                          '; resultColumns=',
                          COALESCE(NULLIF((
                              SELECT STRING_AGG(CONCAT(
                                  COALESCE(rs.name, ''),
                                  ':',
                                  COALESCE(rs.system_type_name, ''),
                                  ':',
                                  CONVERT(varchar(1), COALESCE(rs.is_nullable, 0))), '|') WITHIN GROUP (ORDER BY rs.column_ordinal)
                              FROM sys.dm_exec_describe_first_result_set_for_object(o.object_id, NULL) rs
                              WHERE rs.error_number IS NULL
                                AND rs.is_hidden = 0
                          ), ''), NULLIF((
                              SELECT STRING_AGG(CONCAT(
                                  c.name,
                                  ':',
                                  ty.name,
                                  CASE
                                      WHEN ty.name IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), c.max_length) END, ')')
                                      WHEN ty.name IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), c.max_length / 2) END, ')')
                                      WHEN ty.name IN ('decimal', 'numeric') THEN CONCAT('(', c.precision, ',', c.scale, ')')
                                      ELSE ''
                                  END,
                                  ':',
                                  CONVERT(varchar(1), c.is_nullable)), '|') WITHIN GROUP (ORDER BY c.column_id)
                              FROM sys.columns c
                              INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                              WHERE c.object_id = o.object_id
                                AND c.is_hidden = 0
                                AND o.type IN ('IF', 'TF')
                          ), ''), ''))
            """;
    }
}
