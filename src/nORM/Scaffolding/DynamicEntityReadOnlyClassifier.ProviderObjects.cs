#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        public static bool IsDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
            {
                var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"""
                    SELECT type, sql
                    FROM {EscapeIdentifier(connection, schema)}.sqlite_master
                    WHERE name = @tableName
                      AND type IN ('table', 'view')
                    LIMIT 1
                    """;
                AddStringParameter(cmd, "@tableName", tableName);
                using var reader = cmd.ExecuteReader();
                if (!reader.Read())
                    return false;

                var type = Convert.ToString(reader["type"]);
                if (string.Equals(type, "view", StringComparison.OrdinalIgnoreCase))
                    return true;

                var sql = Convert.ToString(reader["sql"]);
                return sql?.TrimStart().StartsWith("CREATE VIRTUAL TABLE", StringComparison.OrdinalIgnoreCase) == true;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM sys.views v
                    INNER JOIN sys.schemas s ON s.schema_id = v.schema_id
                    WHERE v.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                      AND v.is_ms_shipped = 0
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND table_type = 'VIEW'
                      AND table_schema NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT 1
                    FROM pg_matviews
                    WHERE matviewname = @tableName
                      AND (@schemaName IS NULL OR schemaname = @schemaName)
                    """, schemaName, tableName);
            }

            if (IsMySqlConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                    """, schemaName, tableName);
            }

            return false;
        }

        public static bool IsProviderOwnedSynonym(DbConnection connection, string? schemaName, string tableName)
        {
            if (!IsSqlServerConnection(connection.GetType().Name))
                return false;

            return QueryExists(connection, """
                SELECT 1
                FROM sys.synonyms sy
                INNER JOIN sys.schemas s ON s.schema_id = sy.schema_id
                WHERE sy.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                """, schemaName, tableName);
        }

        public static bool IsProviderNativeTemporalTable(DbConnection connection, string? schemaName, string tableName)
        {
            if (!IsSqlServerConnection(connection.GetType().Name))
                return false;

            return QueryExists(connection, """
                SELECT 1
                FROM sys.tables t
                INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.name = @tableName
                  AND (@schemaName IS NULL OR s.name = @schemaName)
                  AND t.is_ms_shipped = 0
                  AND t.temporal_type <> 0
                """, schemaName, tableName);
        }

        public static bool HasProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
            {
                var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"""
                    SELECT 1
                    FROM {EscapeIdentifier(connection, schema)}.sqlite_master
                    WHERE type = 'trigger'
                      AND tbl_name = @tableName
                    LIMIT 1
                    """;
                AddStringParameter(cmd, "@tableName", tableName);
                return cmd.ExecuteScalar() is not null;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM sys.triggers tr
                    INNER JOIN sys.tables t ON t.object_id = tr.parent_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                      AND t.is_ms_shipped = 0
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM information_schema.triggers
                    WHERE event_object_table = @tableName
                      AND (@schemaName IS NULL OR event_object_schema = @schemaName)
                      AND event_object_schema NOT IN ('pg_catalog', 'information_schema')
                    """, schemaName, tableName);
            }

            if (IsMySqlConnection(connectionName))
            {
                return QueryExists(connection, """
                    SELECT 1
                    FROM information_schema.triggers
                    WHERE trigger_schema = COALESCE(@schemaName, DATABASE())
                      AND event_object_table = @tableName
                    """, schemaName, tableName);
            }

            return false;
        }

        private static bool QueryExists(DbConnection connection, string sql, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            return cmd.ExecuteScalar() is not null;
        }
    }
}
