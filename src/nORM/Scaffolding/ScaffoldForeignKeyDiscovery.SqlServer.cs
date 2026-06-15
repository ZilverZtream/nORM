#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private const string SqlServerForeignKeySql = """
            SELECT
                SCHEMA_NAME(dep.schema_id) AS DependentSchema,
                dep.name AS DependentTable,
                dep_col.name AS DependentColumn,
                SCHEMA_NAME(principal.schema_id) AS PrincipalSchema,
                principal.name AS PrincipalTable,
                principal_col.name AS PrincipalColumn,
                fk.name AS ConstraintName,
                fk.is_system_named AS IsSyntheticConstraintName,
                COUNT(*) OVER (PARTITION BY fk.object_id) AS ColumnCount,
                fk.delete_referential_action_desc AS OnDelete,
                fk.update_referential_action_desc +
                CASE WHEN fk.is_not_trusted = 1 THEN ' NOT TRUSTED' ELSE '' END +
                CASE WHEN fk.is_disabled = 1 THEN ' DISABLED' ELSE '' END +
                CASE WHEN fk.is_not_for_replication = 1 THEN ' NOT FOR REPLICATION' ELSE '' END AS OnUpdate
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            INNER JOIN sys.tables dep ON dep.object_id = fk.parent_object_id
            INNER JOIN sys.columns dep_col ON dep_col.object_id = dep.object_id AND dep_col.column_id = fkc.parent_column_id
            INNER JOIN sys.tables principal ON principal.object_id = fk.referenced_object_id
            INNER JOIN sys.columns principal_col ON principal_col.object_id = principal.object_id AND principal_col.column_id = fkc.referenced_column_id
            WHERE dep.is_ms_shipped = 0 AND principal.is_ms_shipped = 0
            ORDER BY SCHEMA_NAME(dep.schema_id), dep.name, fk.name, fkc.constraint_column_id
            """;
    }
}
