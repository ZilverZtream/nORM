#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool IsPostgresDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND table_type = 'VIEW'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                UNION ALL
                SELECT 1
                FROM pg_matviews
                WHERE matviewname = @tableName
                  AND (@schemaName IS NULL OR schemaname = @schemaName)
                """, schemaName, tableName);

        private static bool HasPostgresProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM information_schema.triggers
                WHERE event_object_table = @tableName
                  AND (@schemaName IS NULL OR event_object_schema = @schemaName)
                  AND event_object_schema NOT IN ('pg_catalog', 'information_schema')
                """, schemaName, tableName);
    }
}
