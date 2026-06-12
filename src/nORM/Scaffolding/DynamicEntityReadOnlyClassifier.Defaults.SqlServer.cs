#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool HasSqlServerUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
            => QueryHasUnmodeledDefault(connection, """
                SELECT CONVERT(nvarchar(max), dc.definition) AS DefaultSql
                FROM sys.default_constraints dc
                INNER JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                """, schemaName, tableName);
    }
}
