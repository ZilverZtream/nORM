namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private const string SqlServerPartialIndexFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'PartialIndex', 'SQL Server filtered index'
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.has_filter = 1
              AND i.name IS NOT NULL
            """;

        private const string SqlServerIncludedColumnIndexFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'IncludedColumnIndex', 'SQL Server index with included columns'
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.name IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM sys.index_columns included
                  WHERE included.object_id = i.object_id
                    AND included.index_id = i.index_id
                    AND included.is_included_column = 1
              )
            """;

        private const string SqlServerDescendingIndexFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'DescendingIndex', 'SQL Server descending index key'
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.name IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM sys.index_columns ic
                  WHERE ic.object_id = i.object_id
                    AND ic.index_id = i.index_id
                    AND ic.key_ordinal > 0
                    AND ic.is_descending_key = 1
              )
            """;

        private const string SqlServerProviderSpecificIndexFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'ProviderSpecificIndex',
                CONCAT(
                    'SQL Server provider-specific index; indexType=',
                    CONVERT(nvarchar(128), i.type_desc) COLLATE DATABASE_DEFAULT)
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.is_hypothetical = 0
              AND i.name IS NOT NULL
              AND i.type NOT IN (1, 2)
            """;
    }
}
