#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldSkippedObjectQuery;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqlServerSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = await QuerySkippedObjectsAsync(connection, """
                SELECT SCHEMA_NAME(v.schema_id) AS ObjectSchema, v.name AS ObjectName, 'View' AS Kind, 'SQL Server view' AS Detail
                FROM sys.views v
                WHERE v.is_ms_shipped = 0
                UNION ALL
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
                UNION ALL
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
                              COALESCE((
                                  SELECT STRING_AGG(CONCAT(
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
                                      END), ',') WITHIN GROUP (ORDER BY pa.parameter_id)
                                  FROM sys.parameters pa
                                  INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                  LEFT JOIN sys.types base_ty
                                    ON ty.is_user_defined = 1
                                   AND ty.is_table_type = 0
                                   AND base_ty.user_type_id = ty.system_type_id
                                   AND base_ty.is_user_defined = 0
                                  WHERE pa.object_id = o.object_id
                              ), ''),
                              '; dataType=',
                              COALESCE((
                                  SELECT TOP (1) ty.name
                                  FROM sys.parameters pa
                                  INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                  WHERE pa.object_id = o.object_id
                                    AND pa.parameter_id = 0
                              ), CASE WHEN o.type IN ('IF', 'TF') THEN 'TABLE' ELSE '' END),
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
                FROM sys.objects o
                WHERE o.is_ms_shipped = 0
                  AND o.type IN ('FN', 'IF', 'TF')
                UNION ALL
                SELECT SCHEMA_NAME(s.schema_id), s.name, 'Sequence',
                       CONCAT('SQL Server sequence; dataType=', ty.name,
                              CASE
                                  WHEN ty.name IN ('decimal', 'numeric') THEN CONCAT('(', s.precision, ',', s.scale, ')')
                                  ELSE ''
                              END)
                FROM sys.sequences s
                INNER JOIN sys.types ty ON ty.user_type_id = s.user_type_id
                UNION ALL
                SELECT SCHEMA_NAME(s.schema_id), s.name, 'Synonym',
                       CONCAT('SQL Server synonym; baseObject=', s.base_object_name,
                              '; baseType=', COALESCE(CONVERT(nvarchar(20), OBJECTPROPERTYEX(OBJECT_ID(s.base_object_name), 'BaseType')), ''))
                FROM sys.synonyms s
                ORDER BY ObjectSchema, ObjectName
                """).ConfigureAwait(false);
            return await AttachSkippedObjectCommentsAsync(connection, provider, objects).ConfigureAwait(false);
        }
    }
}
