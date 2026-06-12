#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityComputedColumnReader
    {
        private static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetMySqlComputedColumns(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryComputedColumnMap(connection, """
                SELECT column_name AS ColumnName,
                       generation_expression AS ComputedSql,
                       CASE
                           WHEN LOWER(COALESCE(extra, '')) LIKE '%stored generated%' THEN 1
                           ELSE 0
                       END AS IsStored
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND generation_expression IS NOT NULL
                  AND generation_expression <> ''
                """, schemaName, tableName);
    }
}
