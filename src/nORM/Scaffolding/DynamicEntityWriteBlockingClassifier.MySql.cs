#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityWriteBlockingClassifier
    {
        private static bool HasWriteBlockingMySqlColumns(DbConnection connection, string? schemaName, string tableName)
            => QueryExists(connection, """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND data_type IN (
                      'geometry',
                      'point',
                      'linestring',
                      'polygon',
                      'multipoint',
                      'multilinestring',
                      'multipolygon',
                      'geometrycollection'
                  )
                """, schemaName, tableName)
                || HasWriteBlockingMySqlSetColumns(connection, schemaName, tableName);

        public static bool HasWriteBlockingMySqlSetColumns(DbConnection connection, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_type AS ColumnType
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND data_type = 'set'
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(Convert.ToString(reader["ColumnType"]), out _))
                    return true;
            }

            return false;
        }
    }
}
