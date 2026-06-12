#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetMySqlStringBinaryFacetsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnFacetMapAsync(connection, tableKeys, """
                SELECT NULL AS TableSchema,
                       table_name AS TableName,
                       column_name AS ColumnName,
                       character_maximum_length AS MaxLength,
                       NULL AS IsUnicode,
                       CASE WHEN data_type IN ('char', 'binary') THEN 1 ELSE 0 END AS IsFixedLength
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND data_type IN ('char', 'varchar', 'binary', 'varbinary')
                """);
    }
}
