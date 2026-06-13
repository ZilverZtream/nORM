#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetPostgresColumnStoreTypesAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnStoreTypeMapAsync(connection, tableKeys, """
                SELECT table_schema AS TableSchema,
                       table_name AS TableName,
                       column_name AS ColumnName,
                       data_type AS StoreType
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                """);
    }
}
