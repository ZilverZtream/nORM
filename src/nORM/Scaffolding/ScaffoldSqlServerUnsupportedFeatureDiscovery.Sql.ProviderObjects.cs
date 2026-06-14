namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private const string SqlServerRowVersionFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'RowVersion', ty.name
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE t.is_ms_shipped = 0
              AND ty.name IN ('timestamp', 'rowversion')
            """;

        private const string SqlServerIdentityStrategyFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'IdentityStrategy',
                'IDENTITY(' + CONVERT(varchar(40), ic.seed_value) + ',' + CONVERT(varchar(40), ic.increment_value) + ')'
            FROM sys.identity_columns ic
            INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            INNER JOIN sys.tables t ON t.object_id = ic.object_id
            WHERE t.is_ms_shipped = 0
              AND (CONVERT(decimal(38,0), ic.seed_value) <> 1 OR CONVERT(decimal(38,0), ic.increment_value) <> 1)
            """;

        private const string SqlServerTriggerFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, tr.name, 'Trigger',
                CONCAT(
                    'SQL Server trigger; timing=',
                    CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END,
                    '; isDisabled=',
                    CASE WHEN tr.is_disabled = 1 THEN 'true' ELSE 'false' END,
                    '; isInsteadOf=',
                    CASE WHEN tr.is_instead_of_trigger = 1 THEN 'true' ELSE 'false' END)
            FROM sys.triggers tr
            INNER JOIN sys.tables t ON t.object_id = tr.parent_id
            WHERE t.is_ms_shipped = 0
            """;

        private const string SqlServerTemporalTableFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, t.name, 'TemporalTable',
                CONCAT(
                    CASE t.temporal_type WHEN 1 THEN 'SQL Server temporal history table' ELSE 'SQL Server system-versioned temporal table' END,
                    '; temporalType=',
                    CASE t.temporal_type WHEN 1 THEN 'history' ELSE 'system-versioned' END,
                    CASE
                        WHEN t.history_table_id IS NOT NULL AND t.history_table_id <> 0 AND h.object_id IS NOT NULL
                        THEN CONCAT('; historyTable=', SCHEMA_NAME(h.schema_id), '.', h.name)
                        ELSE ''
                    END)
            FROM sys.tables t
            LEFT JOIN sys.tables h ON h.object_id = t.history_table_id
            WHERE t.is_ms_shipped = 0 AND t.temporal_type <> 0
            """;
    }
}
