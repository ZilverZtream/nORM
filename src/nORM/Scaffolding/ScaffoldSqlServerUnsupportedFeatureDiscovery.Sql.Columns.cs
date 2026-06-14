namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private const string SqlServerDefaultFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ObjectName, 'Default' AS Kind, dc.definition AS Detail
            FROM sys.default_constraints dc
            INNER JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
            INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
            WHERE t.is_ms_shipped = 0
            """;

        private const string SqlServerComputedFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Computed',
                CONCAT(cc.definition, CASE WHEN cc.is_persisted = 1 THEN ' PERSISTED' ELSE '' END)
            FROM sys.computed_columns cc
            INNER JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            INNER JOIN sys.tables t ON t.object_id = cc.object_id
            WHERE t.is_ms_shipped = 0
            """;

        private const string SqlServerCheckConstraintFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, cc.name, 'CheckConstraint', cc.definition
            FROM sys.check_constraints cc
            INNER JOIN sys.tables t ON t.object_id = cc.parent_object_id
            WHERE t.is_ms_shipped = 0
            """;

        private const string SqlServerCollationFeatureSql = """
            SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Collation', c.collation_name
            FROM sys.columns c
            INNER JOIN sys.tables t ON t.object_id = c.object_id
            WHERE t.is_ms_shipped = 0
              AND c.collation_name IS NOT NULL
              AND c.collation_name <> CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation'))
            """;

        private const string SqlServerProviderSpecificColumnTypeFeatureSql = """
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
            """;

        private const string SqlServerPrecisionScaleFeatureSql = """
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
            """;
    }
}
