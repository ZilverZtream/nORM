#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetPostgresStringBinaryFacets(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryColumnFacetMap(connection, """
                SELECT column_name AS ColumnName,
                       character_maximum_length::int AS MaxLength,
                       NULL::int AS IsUnicode,
                       CASE WHEN data_type = 'character' OR udt_name = 'bpchar' THEN 1 ELSE 0 END AS IsFixedLength
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND (data_type IN ('character varying', 'character') OR udt_name IN ('varchar', 'bpchar'))
                """, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetPostgresDecimalPrecisions(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryDecimalPrecisionMap(connection, """
                SELECT column_name AS ColumnName,
                       numeric_precision AS DecimalPrecision,
                       numeric_scale AS DecimalScale
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND data_type = 'numeric'
                  AND numeric_precision IS NOT NULL
                """, schemaName, tableName);
    }
}
