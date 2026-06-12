#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityComputedColumnReader
    {
        private static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetSqlServerComputedColumns(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryComputedColumnMap(connection, """
                SELECT c.name AS ColumnName,
                       CONVERT(nvarchar(max), cc.definition) AS ComputedSql,
                       CONVERT(bit, cc.is_persisted) AS IsStored
                FROM sys.computed_columns cc
                INNER JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                INNER JOIN sys.tables t ON t.object_id = cc.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                """, schemaName, tableName);
    }
}
