#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetSqlServerStringBinaryFacetsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnFacetMapAsync(connection, tableKeys, """
                SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                       t.name AS TableName,
                       c.name AS ColumnName,
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
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                LEFT JOIN sys.types base_ty
                  ON ty.is_user_defined = 1
                 AND base_ty.user_type_id = ty.system_type_id
                 AND base_ty.is_user_defined = 0
                WHERE t.is_ms_shipped = 0
                  AND COALESCE(base_ty.name, ty.name) IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary')
                """);
    }
}
