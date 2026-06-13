#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaResolver
    {
        private static IReadOnlyList<string> GetSqlServerMatchingObjectSchemas(DbConnection connection, string tableName)
            => QuerySchemaNameList(connection, """
                SELECT s.name AS SchemaName
                FROM sys.objects o
                INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE o.name = @tableName
                  AND o.type IN ('U', 'V')
                  AND o.is_ms_shipped = 0
                ORDER BY s.name
                """, tableName);
    }
}
