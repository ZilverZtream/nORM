namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private static readonly string SqlServerUnsupportedFeatureSql = string.Join(
            "\nUNION ALL\n",
            SqlServerDefaultFeatureSql,
            SqlServerComputedFeatureSql,
            SqlServerCheckConstraintFeatureSql,
            SqlServerCollationFeatureSql,
            SqlServerProviderSpecificColumnTypeFeatureSql,
            SqlServerPrecisionScaleFeatureSql,
            SqlServerRowVersionFeatureSql,
            SqlServerIdentityStrategyFeatureSql,
            SqlServerTriggerFeatureSql,
            SqlServerTemporalTableFeatureSql,
            SqlServerPartialIndexFeatureSql,
            SqlServerIncludedColumnIndexFeatureSql,
            SqlServerDescendingIndexFeatureSql,
            SqlServerProviderSpecificIndexFeatureSql);

        private const string SqlServerSystemNamedCheckConstraintSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, cc.name AS ConstraintName
            FROM sys.check_constraints cc
            INNER JOIN sys.tables t ON t.object_id = cc.parent_object_id
            WHERE t.is_ms_shipped = 0 AND cc.is_system_named = 1
            """;

        private const string SqlServerNamedDefaultConstraintSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName, dc.name AS ConstraintName
            FROM sys.default_constraints dc
            INNER JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
            INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
            WHERE t.is_ms_shipped = 0 AND dc.is_system_named = 0
            """;
    }
}
