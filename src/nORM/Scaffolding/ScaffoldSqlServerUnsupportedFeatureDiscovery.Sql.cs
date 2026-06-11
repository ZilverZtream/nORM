namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private const string SqlServerUnsupportedFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ObjectName, 'Default' AS Kind, dc.definition AS Detail
            FROM sys.default_constraints dc
            INNER JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
            INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
            WHERE t.is_ms_shipped = 0
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Computed',
                CONCAT(cc.definition, CASE WHEN cc.is_persisted = 1 THEN ' PERSISTED' ELSE '' END)
            FROM sys.computed_columns cc
            INNER JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            INNER JOIN sys.tables t ON t.object_id = cc.object_id
            WHERE t.is_ms_shipped = 0
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, cc.name, 'CheckConstraint', cc.definition
            FROM sys.check_constraints cc
            INNER JOIN sys.tables t ON t.object_id = cc.parent_object_id
            WHERE t.is_ms_shipped = 0
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Collation', c.collation_name
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            WHERE t.is_ms_shipped = 0
              AND c.collation_name IS NOT NULL
              AND c.collation_name <> CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation'))
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'ProviderSpecificColumnType',
                CASE
                    WHEN ty.is_user_defined = 1
                    THEN CONCAT(
                        'user-defined type (',
                        SCHEMA_NAME(ty.schema_id),
                        '.',
                        ty.name,
                        CASE
                            WHEN base_ty.name IS NULL THEN ''
                            ELSE CONCAT(
                                ' -> ',
                                base_ty.name,
                                CASE
                                    WHEN base_ty.name IN ('nchar', 'nvarchar') AND c.max_length = -1 THEN '(max)'
                                    WHEN base_ty.name IN ('nchar', 'nvarchar') AND c.max_length > 0 THEN CONCAT('(', c.max_length / 2, ')')
                                    WHEN base_ty.name IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length = -1 THEN '(max)'
                                    WHEN base_ty.name IN ('char', 'varchar', 'binary', 'varbinary') AND c.max_length > 0 THEN CONCAT('(', c.max_length, ')')
                                    WHEN base_ty.name IN ('decimal', 'numeric') THEN CONCAT('(', c.precision, ',', c.scale, ')')
                                    ELSE ''
                                END)
                        END,
                        ')')
                    ELSE ty.name
                END
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            LEFT JOIN sys.types base_ty
              ON ty.is_user_defined = 1
             AND base_ty.user_type_id = ty.system_type_id
             AND base_ty.is_user_defined = 0
            WHERE t.is_ms_shipped = 0
              AND (ty.is_user_defined = 1 OR ty.name IN ('geography', 'geometry', 'hierarchyid', 'sql_variant', 'xml'))
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'PrecisionScale',
                COALESCE(base_ty.name, ty.name) + '(' + CONVERT(varchar(10), c.precision) + ',' + CONVERT(varchar(10), c.scale) + ')'
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            LEFT JOIN sys.types base_ty
              ON ty.is_user_defined = 1
             AND base_ty.user_type_id = ty.system_type_id
             AND base_ty.is_user_defined = 0
            WHERE t.is_ms_shipped = 0
              AND COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric')
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'RowVersion', ty.name
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE t.is_ms_shipped = 0
              AND ty.name IN ('timestamp', 'rowversion')
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'IdentityStrategy',
                'IDENTITY(' + CONVERT(varchar(40), ic.seed_value) + ',' + CONVERT(varchar(40), ic.increment_value) + ')'
            FROM sys.identity_columns ic
            INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            INNER JOIN sys.tables t ON t.object_id = ic.object_id
            WHERE t.is_ms_shipped = 0
              AND (CONVERT(decimal(38,0), ic.seed_value) <> 1 OR CONVERT(decimal(38,0), ic.increment_value) <> 1)
            UNION ALL
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
            UNION ALL
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
            UNION ALL
            SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'PartialIndex', 'SQL Server filtered index'
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.has_filter = 1
              AND i.name IS NOT NULL
            UNION ALL
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
            UNION ALL
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
            UNION ALL
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

        private const string SqlServerSystemNamedCheckConstraintSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, cc.name AS ConstraintName
            FROM sys.check_constraints cc
            INNER JOIN sys.tables t ON t.object_id = cc.parent_object_id
            WHERE t.is_ms_shipped = 0 AND cc.is_system_named = 1
            """;
    }
}
