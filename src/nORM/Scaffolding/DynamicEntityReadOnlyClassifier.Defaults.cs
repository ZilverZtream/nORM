#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        public static bool HasUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
            {
                var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"PRAGMA {DynamicEntityConnectionKind.EscapeIdentifier(connection, schema)}.table_xinfo({DynamicEntityConnectionKind.EscapeIdentifier(connection, tableName)})";
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

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
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

            if (DynamicEntityConnectionKind.IsPostgres(connection))
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

            if (DynamicEntityConnectionKind.IsMySql(connection))
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

        public static bool TryNormalizeDynamicDefaultSql(string? raw, out string defaultValueSql)
            => ScaffoldSqlMetadataParser.TryNormalizeScaffoldDefaultSql(raw, out defaultValueSql);

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
    }
}
