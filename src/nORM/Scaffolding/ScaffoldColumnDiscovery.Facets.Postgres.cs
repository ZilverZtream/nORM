#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetPostgresStringBinaryFacetsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryColumnFacetMapAsync(connection, tableKeys, """
                SELECT table_schema AS TableSchema,
                       table_name AS TableName,
                       column_name AS ColumnName,
                       character_maximum_length::int AS MaxLength,
                       NULL::int AS IsUnicode,
                       CASE WHEN data_type = 'character' OR udt_name = 'bpchar' THEN 1 ELSE 0 END AS IsFixedLength
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND (data_type IN ('character varying', 'character') OR udt_name IN ('varchar', 'bpchar'))
                """);
    }
}
