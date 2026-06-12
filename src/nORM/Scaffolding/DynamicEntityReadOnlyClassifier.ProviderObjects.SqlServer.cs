#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool IsSqlServerDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM sys.views v
                INNER JOIN sys.schemas s ON s.schema_id = v.schema_id
                WHERE v.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND v.is_ms_shipped = 0
                """, schemaName, tableName);

        private static bool IsSqlServerProviderOwnedSynonym(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM sys.synonyms sy
                INNER JOIN sys.schemas s ON s.schema_id = sy.schema_id
                WHERE sy.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                """, schemaName, tableName);

        private static bool IsSqlServerProviderNativeTemporalTable(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM sys.tables t
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                  AND t.temporal_type <> 0
                """, schemaName, tableName);

        private static bool HasSqlServerProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM sys.triggers tr
                INNER JOIN sys.tables t ON t.object_id = tr.parent_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                """, schemaName, tableName);
    }
}
