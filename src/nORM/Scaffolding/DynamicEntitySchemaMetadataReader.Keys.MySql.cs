#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlySet<string> GetMySqlIdentityColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnNameSet(connection, """
                SELECT column_name AS ColumnName
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND LOWER(extra) LIKE '%auto_increment%'
                """, schemaName, tableName);

        private static IReadOnlyDictionary<string, int> GetMySqlPrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
            => QueryColumnOrdinalMap(connection, """
                SELECT column_name AS ColumnName, ordinal_position AS Ordinal
                FROM information_schema.key_column_usage
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND constraint_name = 'PRIMARY'
                """, schemaName, tableName);
    }
}
