#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetSqlServerStringBinaryFacets(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryColumnFacetMap(connection, """
                SELECT c.name AS ColumnName,
                       CASE
                           WHEN COALESCE(base_ty.name, ty.name) IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN CONVERT(int, c.max_length / 2)
                           WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length > 0 THEN CONVERT(int, c.max_length)
                           ELSE NULL
                       END AS MaxLength,
                       CASE
                           WHEN COALESCE(base_ty.name, ty.name) IN ('nchar', 'nvarchar') THEN CONVERT(int, 1)
                           WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'varchar') THEN CONVERT(int, 0)
                           ELSE NULL
                       END AS IsUnicode,
                       CASE
                           WHEN COALESCE(base_ty.name, ty.name) IN ('char', 'nchar', 'binary') THEN CONVERT(int, 1)
                           ELSE CONVERT(int, 0)
                       END AS IsFixedLength
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
                  AND COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary')
                """, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetSqlServerDecimalPrecisions(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryDecimalPrecisionMap(connection, """
                SELECT c.name AS ColumnName,
                       CONVERT(int, c.precision) AS DecimalPrecision,
                       CONVERT(int, c.scale) AS DecimalScale
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
                  AND COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric')
                """, schemaName, tableName);
    }
}
