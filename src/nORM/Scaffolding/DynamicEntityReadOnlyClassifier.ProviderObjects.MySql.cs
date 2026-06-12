#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool IsMySqlDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM information_schema.views
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                """, schemaName, tableName);

        private static bool HasMySqlProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM information_schema.triggers
                WHERE trigger_schema = COALESCE(@schemaName, DATABASE())
                  AND event_object_table = @tableName
                """, schemaName, tableName);
    }
}
