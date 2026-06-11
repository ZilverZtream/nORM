#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldKeyDiscovery
    {
        private const string SqlServerPrimaryKeyColumnSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName, ic.key_ordinal AS Ordinal
            FROM sys.tables t
            INNER JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
            INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE t.is_ms_shipped = 0
            ORDER BY SCHEMA_NAME(t.schema_id), t.name, ic.key_ordinal
            """;

        private const string PostgresPrimaryKeyColumnSql = """
            SELECT ns.nspname AS TableSchema, cls.relname AS TableName, att.attname AS ColumnName, keys.ordinality AS Ordinal
            FROM pg_constraint con
            INNER JOIN pg_class cls ON cls.oid = con.conrelid
            INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
            INNER JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = keys.attnum
            WHERE con.contype = 'p'
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY ns.nspname, cls.relname, keys.ordinality
            """;

        private const string MySqlPrimaryKeyColumnSql = """
            SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName, ordinal_position AS Ordinal
            FROM information_schema.key_column_usage
            WHERE table_schema = DATABASE()
              AND constraint_name = 'PRIMARY'
            ORDER BY table_name, ordinal_position
            """;

        private const string SqlServerPrimaryKeyConstraintNameSql = """
            SELECT SCHEMA_NAME(t.schema_id) AS TableSchema,
                   t.name AS TableName,
                   kc.name AS ConstraintName
            FROM sys.tables t
            INNER JOIN sys.key_constraints kc ON kc.parent_object_id = t.object_id AND kc.type = 'PK'
            WHERE t.is_ms_shipped = 0
              AND kc.is_system_named = 0
            ORDER BY SCHEMA_NAME(t.schema_id), t.name
            """;

        private const string PostgresPrimaryKeyConstraintNameSql = """
            SELECT ns.nspname AS TableSchema,
                   cls.relname AS TableName,
                   con.conname AS ConstraintName
            FROM pg_constraint con
            INNER JOIN pg_class cls ON cls.oid = con.conrelid
            INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            WHERE con.contype = 'p'
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
              AND con.conname <> cls.relname || '_pkey'
            ORDER BY ns.nspname, cls.relname
            """;
    }
}
