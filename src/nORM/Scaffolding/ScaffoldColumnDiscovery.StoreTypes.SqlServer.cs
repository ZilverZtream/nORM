#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetSqlServerColumnStoreTypesAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnStoreTypeMapAsync(connection, tableKeys, """
                SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                       t.name AS TableName,
                       c.name AS ColumnName,
                       COALESCE(base_ty.name, ty.name) AS StoreType
                FROM sys.columns c
                INNER JOIN sys.tables t ON t.object_id = c.object_id
                INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                LEFT JOIN sys.types base_ty
                  ON ty.is_user_defined = 1
                 AND base_ty.user_type_id = ty.system_type_id
                 AND base_ty.is_user_defined = 0
                WHERE t.is_ms_shipped = 0
                """);
    }
}
