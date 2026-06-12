#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetMySqlStringBinaryFacets(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryColumnFacetMap(connection, """
                SELECT column_name AS ColumnName,
                       character_maximum_length AS MaxLength,
                       NULL AS IsUnicode,
                       CASE WHEN data_type IN ('char', 'binary') THEN 1 ELSE 0 END AS IsFixedLength
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = @tableName
                  AND data_type IN ('char', 'varchar', 'binary', 'varbinary')
                """, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetMySqlDecimalPrecisions(
            DbConnection connection,
            string? schemaName,
            string tableName)
            => QueryDecimalPrecisionMap(connection, """
                SELECT column_name AS ColumnName,
                       numeric_precision AS DecimalPrecision,
                       numeric_scale AS DecimalScale
                FROM information_schema.columns
                WHERE table_schema = COALESCE(@schemaName, DATABASE())
                  AND table_name = @tableName
                  AND data_type IN ('decimal', 'numeric')
                  AND numeric_precision IS NOT NULL
                """, schemaName, tableName);
    }
}
