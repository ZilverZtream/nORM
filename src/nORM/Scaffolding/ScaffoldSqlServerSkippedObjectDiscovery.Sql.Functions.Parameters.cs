namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string FunctionParameterModesSql = """
                          COALESCE(STUFF((
                              SELECT ',' + CONCAT(
                                  pa.name, ':',
                                  CASE WHEN pa.parameter_id = 0 THEN 'RETURN' WHEN pa.is_output = 1 THEN 'OUT' ELSE 'IN' END,
                                  ':',
                                  CASE
                                      WHEN ty.is_table_type = 1 THEN CONCAT('table type (', SCHEMA_NAME(ty.schema_id), '.', ty.name, ')')
                                      ELSE COALESCE(base_ty.name, ty.name)
                                  END,
                                  CASE
                                      WHEN ty.is_table_type = 1 THEN ''
                                      WHEN COALESCE(base_ty.name, ty.name) IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length) END, ')')
                                      WHEN COALESCE(base_ty.name, ty.name) IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length / 2) END, ')')
                                      WHEN COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric') THEN CONCAT('(', pa.precision, ',', pa.scale, ')')
                                      ELSE ''
                                  END)
                              FROM sys.parameters pa
                              INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                              LEFT JOIN sys.types base_ty
                                ON ty.is_user_defined = 1
                               AND ty.is_table_type = 0
                               AND base_ty.user_type_id = ty.system_type_id
                               AND base_ty.is_user_defined = 0
                              WHERE pa.object_id = o.object_id
                              ORDER BY pa.parameter_id
                              FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 1, ''), ''),
            """;
    }
}
