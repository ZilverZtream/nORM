#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        private const string SqlServerIndexSql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS TableSchema,
                t.name AS TableName,
                c.name AS ColumnName,
                i.name AS IndexName,
                i.is_unique AS IsUnique,
                SUM(CASE WHEN ic.is_included_column = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY i.object_id, i.index_id) AS ColumnCount,
                CASE WHEN ic.is_included_column = 1 THEN 2147483647 ELSE ic.key_ordinal - 1 END AS Ordinal,
                ic.is_descending_key AS IsDescending,
                ic.is_included_column AS IsIncluded,
                CASE WHEN kc.is_system_named = 1 THEN 1 ELSE 0 END AS IsSyntheticName,
                i.filter_definition AS FilterSql
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
            LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id AND kc.type = 'UQ'
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.is_hypothetical = 0
              AND i.type IN (1, 2)
              AND i.name IS NOT NULL
            ORDER BY SCHEMA_NAME(t.schema_id), t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
            """;
    }
}
