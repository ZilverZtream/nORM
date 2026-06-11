#nullable enable
using System;
using System.Data.Common;
using nORM.Core;
using nORM.Migration;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityReadOnlyClassifier
    {
        public static bool IsReadOnlyDynamicObject(DbConnection connection, string? schemaName, string tableName)
            => IsDynamicQueryObject(connection, schemaName, tableName)
               || IsProviderOwnedSynonym(connection, schemaName, tableName)
               || IsProviderNativeTemporalTable(connection, schemaName, tableName)
               || HasProviderOwnedTriggers(connection, schemaName, tableName)
               || HasUnmodeledDefaults(connection, schemaName, tableName)
               || HasWriteBlockingProviderSpecificColumns(connection, schemaName, tableName);

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

        public static bool HasUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
            {
                var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"PRAGMA {EscapeIdentifier(connection, schema)}.table_xinfo({EscapeIdentifier(connection, tableName)})";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    if (!ReaderHasColumn(reader, "dflt_value"))
                        continue;

                    if (HasUnmodeledDefaultSql(Convert.ToString(reader["dflt_value"])))
                        return true;
                }

                return false;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryHasUnmodeledDefault(connection, """
                    SELECT CONVERT(nvarchar(max), dc.definition) AS DefaultSql
                    FROM sys.default_constraints dc
                    INNER JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                    INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                      AND t.is_ms_shipped = 0
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryHasUnmodeledDefault(connection, """
                    SELECT column_default AS DefaultSql
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND column_default IS NOT NULL
                      AND is_identity <> 'YES'
                      AND column_default NOT LIKE 'nextval(%'
                    """, schemaName, tableName);
            }

            if (IsMySqlConnection(connectionName))
            {
                return QueryHasUnmodeledDefault(connection, """
                    SELECT CASE
                               WHEN data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set')
                               THEN QUOTE(column_default)
                               WHEN data_type IN ('date', 'datetime', 'timestamp', 'time')
                                    AND LOWER(column_default) NOT LIKE 'current_timestamp%'
                                    AND LOWER(column_default) NOT LIKE 'current_time%'
                                    AND LOWER(column_default) NOT LIKE 'localtime%'
                                    AND LOWER(column_default) NOT LIKE 'localtimestamp%'
                                    AND LOWER(column_default) NOT LIKE 'now(%'
                                    AND LOWER(column_default) NOT LIKE 'utc_timestamp(%'
                                    AND LOWER(column_default) NOT LIKE 'sysdate(%'
                                    AND LOWER(column_default) NOT LIKE 'current_date%'
                                    AND column_default NOT LIKE '%()'
                               THEN QUOTE(column_default)
                               ELSE column_default
                           END AS DefaultSql
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND column_default IS NOT NULL
                    """, schemaName, tableName);
            }

            return false;
        }

        public static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityWriteBlockingClassifier.HasWriteBlockingProviderSpecificColumns(connection, schemaName, tableName);

        public static bool HasWriteBlockingMySqlSetColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityWriteBlockingClassifier.HasWriteBlockingMySqlSetColumns(connection, schemaName, tableName);

        public static bool IsWriteBlockingSqliteDeclaredType(string? declaredType)
            => DynamicEntityWriteBlockingClassifier.IsWriteBlockingSqliteDeclaredType(declaredType);

        public static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => DynamicEntityWriteBlockingClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalizedDeclaredType);

        public static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => DynamicEntityWriteBlockingClassifier.ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, token);

        public static bool TryNormalizeDynamicDefaultSql(string? raw, out string defaultValueSql)
        {
            defaultValueSql = string.Empty;
            if (string.IsNullOrWhiteSpace(raw))
                return false;

            var candidate = raw.Trim();
            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            try
            {
                var validated = DefaultValueValidator.Validate(candidate);
                if (string.IsNullOrWhiteSpace(validated))
                    return false;

                defaultValueSql = validated;
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
            => DynamicEntityWriteBlockingClassifier.IsSqliteUuidDeclaredType(declaredType);

        private static bool QueryHasUnmodeledDefault(DbConnection connection, string sql, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (HasUnmodeledDefaultSql(Convert.ToString(reader["DefaultSql"])))
                    return true;
            }

            return false;
        }

        private static bool HasUnmodeledDefaultSql(string? raw)
            => !string.IsNullOrWhiteSpace(raw)
               && !TryNormalizeDynamicDefaultSql(raw, out _);

        private static bool QueryExists(DbConnection connection, string sql, string? schemaName, string tableName)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            return cmd.ExecuteScalar() is not null;
        }

        private static bool HasBalancedOuterParentheses(string value)
        {
            var depth = 0;
            var inString = false;
            for (var i = 0; i < value.Length; i++)
            {
                var ch = value[i];
                if (ch == '\'')
                {
                    inString = !inString;
                    if (inString && i + 1 < value.Length && value[i + 1] == '\'')
                        i++;
                    continue;
                }

                if (inString)
                    continue;

                if (ch == '(')
                    depth++;
                else if (ch == ')')
                    depth--;

                if (depth == 0 && i < value.Length - 1)
                    return false;
                if (depth < 0)
                    return false;
            }

            return depth == 0;
        }
    }
}
