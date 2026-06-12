#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityComputedColumnReader
    {
        private static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetPostgresComputedColumns(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryComputedColumnMap(connection, """
                SELECT column_name AS ColumnName,
                       generation_expression AS ComputedSql,
                       1 AS IsStored
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND is_generated <> 'NEVER'
                """, schemaName, tableName);
    }
}
