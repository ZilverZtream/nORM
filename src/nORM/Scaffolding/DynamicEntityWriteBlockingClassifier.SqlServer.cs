#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityWriteBlockingClassifier
    {
        private static bool HasWriteBlockingSqlServerColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                LEFT JOIN sys.types base_ty
                  ON ty.is_user_defined = 1
                 AND base_ty.user_type_id = ty.system_type_id
                 AND base_ty.is_user_defined = 0
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                  AND (
                      ty.name IN ('geography', 'geometry', 'hierarchyid', 'sql_variant')
                      OR (
                          ty.is_user_defined = 1
                          AND (
                              base_ty.name IS NULL
                              OR base_ty.name NOT IN (
                                  'int', 'bigint', 'smallint', 'tinyint', 'bit',
                                  'decimal', 'numeric', 'money', 'smallmoney',
                                  'float', 'real',
                                  'date', 'time', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset',
                                  'uniqueidentifier', 'sysname',
                                  'char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext', 'xml',
                                  'binary', 'varbinary', 'image'
                              )
                          )
                      )
                  )
                """, schemaName, tableName);
    }
}
