#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlyDictionary<string, string> GetColumnStoreTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteDeclaredColumnTypes(connection, schemaName, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
            {
                return QueryColumnStoreTypeMap(connection, """
                    SELECT c.name AS ColumnName,
                           COALESCE(base_ty.name, ty.name) AS StoreType
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    LEFT JOIN sys.types base_ty
                      ON ty.is_user_defined = 1
                     AND base_ty.user_type_id = ty.system_type_id
                     AND base_ty.is_user_defined = 0
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                      AND t.is_ms_shipped = 0
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsPostgres(connection))
            {
                return QueryColumnStoreTypeMap(connection, """
                    SELECT column_name AS ColumnName,
                           data_type AS StoreType
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND table_schema NOT IN ('pg_catalog', 'information_schema')
                    """, schemaName, tableName);
            }

            if (DynamicEntityConnectionKind.IsMySql(connection))
            {
                return QueryColumnStoreTypeMap(connection, """
                    SELECT column_name AS ColumnName,
                           data_type AS StoreType
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                    """, schemaName, tableName);
            }

            return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
        }

        private static IReadOnlyDictionary<string, string> QueryColumnStoreTypeMap(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                var storeType = Convert.ToString(reader["StoreType"]);
                if (!string.IsNullOrWhiteSpace(columnName) && !string.IsNullOrWhiteSpace(storeType))
                    result[columnName!] = storeType!;
            }

            return result;
        }
    }
}
