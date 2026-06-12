#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityProviderTypeMetadataReader
    {
        public static IReadOnlyDictionary<string, string> GetSqlServerAliasColumnBaseTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (!DynamicEntityConnectionKind.IsSqlServer(connection))
                return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT c.name AS ColumnName,
                       base_ty.name +
                       CASE
                           WHEN base_ty.name IN ('nchar', 'nvarchar') AND c.max_length = -1 THEN '(max)'
                           WHEN base_ty.name IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN '(' + CONVERT(varchar(11), c.max_length / 2) + ')'
                           WHEN base_ty.name IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length = -1 THEN '(max)'
                           WHEN base_ty.name IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length > 0 THEN '(' + CONVERT(varchar(11), c.max_length) + ')'
                           WHEN base_ty.name IN ('decimal', 'numeric') THEN '(' + CONVERT(varchar(10), c.precision) + ',' + CONVERT(varchar(10), c.scale) + ')'
                           ELSE ''
                       END AS BaseType
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                INNER JOIN sys.types base_ty
                  ON ty.is_user_defined = 1
                 AND base_ty.user_type_id = ty.system_type_id
                 AND base_ty.is_user_defined = 0
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                result[columnName] = Convert.ToString(reader["BaseType"]) ?? string.Empty;
            }

            return result;
        }
    }
}
