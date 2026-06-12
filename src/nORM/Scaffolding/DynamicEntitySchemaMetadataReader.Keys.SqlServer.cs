#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlySet<string> GetSqlServerIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnNameSet(connection, """
                SELECT c.name AS ColumnName
                FROM sys.identity_columns ic
                INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                INNER JOIN sys.tables t ON t.object_id = ic.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                """, schemaName, tableName);

        private static IReadOnlyDictionary<string, int> GetSqlServerPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnOrdinalMap(connection, """
                SELECT c.name AS ColumnName, ic.key_ordinal AS Ordinal
                FROM sys.tables t
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                INNER JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
                INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                """, schemaName, tableName);

        private static IReadOnlySet<string> GetSqlServerRowVersionColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnNameSet(connection, """
                SELECT c.name AS ColumnName
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND ty.name IN ('timestamp', 'rowversion')
                """, schemaName, tableName);
    }
}
