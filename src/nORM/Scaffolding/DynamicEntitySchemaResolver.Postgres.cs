#nullable enable
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaResolver
    {
        private static IReadOnlyList<string> GetPostgresMatchingObjectSchemas(DbConnection connection, string tableName)
            => QuerySchemaNameList(connection, """
                SELECT table_schema AS SchemaName
                FROM information_schema.tables
                WHERE table_name = @tableName
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_schema
                """, tableName);
    }
}
