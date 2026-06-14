#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerSkippedObjectDiscovery
    {
        private const string StoredProcedureSkippedObjectSql = """
            SELECT SCHEMA_NAME(p.schema_id), p.name, 'Routine',
                   CONCAT('SQL Server stored procedure; parameters=',
                          (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = p.object_id),
                          '; outputParameters=',
                          (SELECT COUNT(*) + 1 FROM sys.parameters pa WHERE pa.object_id = p.object_id AND pa.is_output = 1),
                          '; parameterModes=',
                          COALESCE(NULLIF((
                              SELECT STRING_AGG(CONCAT(
                                  pa.name, ':', CASE WHEN pa.is_output = 1 THEN 'OUT' ELSE 'IN' END, ':',
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
                                  END), ',') WITHIN GROUP (ORDER BY pa.parameter_id)
                              FROM sys.parameters pa
                              INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                              LEFT JOIN sys.types base_ty
                                ON ty.is_user_defined = 1
                               AND ty.is_table_type = 0
                               AND base_ty.user_type_id = ty.system_type_id
                               AND base_ty.is_user_defined = 0
                              WHERE pa.object_id = p.object_id
                          ), '') + ',', '') + 'return:RETURN:int',
                          '; resultColumns=',
                          COALESCE(NULLIF((
                              SELECT STRING_AGG(CONCAT(
                                  COALESCE(rs.name, ''),
                                  ':',
                                  COALESCE(rs.system_type_name, ''),
                                  ':',
                                  CONVERT(varchar(1), COALESCE(rs.is_nullable, 0))), '|') WITHIN GROUP (ORDER BY rs.column_ordinal)
                              FROM sys.dm_exec_describe_first_result_set_for_object(p.object_id, NULL) rs
                              WHERE rs.error_number IS NULL
                                AND rs.is_hidden = 0
                          ), ''), ''))
            FROM sys.procedures p
            WHERE p.is_ms_shipped = 0
            """;
    }
}
