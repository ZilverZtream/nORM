#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityComputedColumnReader
    {
        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetComputedColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var connectionName = connection.GetType().Name;

            if (IsSqliteConnection(connectionName))
            {
                var result = new Dictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
                foreach (var (columnName, computed) in ExtractSqliteGeneratedColumns(GetSqliteCreateTableSql(connection, schemaName, tableName)))
                    result[columnName] = computed;

                using var cmd = connection.CreateCommand();
                var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                    ? string.Empty
                    : EscapeIdentifier(connection, schemaName!) + ".";
                cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({EscapeIdentifier(connection, tableName)})";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    if (!ReaderHasColumn(reader, "hidden")
                        || !ReaderHasColumn(reader, "name")
                        || Convert.ToInt32(reader["hidden"], System.Globalization.CultureInfo.InvariantCulture) is not (2 or 3))
                    {
                        continue;
                    }

                    var name = Convert.ToString(reader["name"]);
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        if (!result.ContainsKey(name))
                            result[name] = new DynamicEntityTypeGenerator.ScaffoldComputedColumn(string.Empty, Stored: false);
                    }
                }

                return result;
            }

            if (IsSqlServerConnection(connectionName))
            {
                return QueryComputedColumnMap(connection, """
                    SELECT c.name AS ColumnName,
                           CONVERT(nvarchar(max), cc.definition) AS ComputedSql,
                           CONVERT(bit, cc.is_persisted) AS IsStored
                    FROM sys.computed_columns cc
                    INNER JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                    INNER JOIN sys.tables t ON t.object_id = cc.object_id
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE t.name = @tableName
                      AND (@schemaName IS NULL OR s.name = @schemaName)
                    """, schemaName, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QueryComputedColumnMap(connection, """
                    SELECT column_name AS ColumnName,
                           generation_expression AS ComputedSql,
                           1 AS IsStored
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                      AND (@schemaName IS NULL OR table_schema = @schemaName)
                      AND is_generated <> 'NEVER'
                    """, schemaName, tableName);
            }

            if (IsMySqlConnection(connectionName))
            {
                return QueryComputedColumnMap(connection, """
                    SELECT column_name AS ColumnName,
                           generation_expression AS ComputedSql,
                           CASE
                               WHEN LOWER(COALESCE(extra, '')) LIKE '%stored generated%' THEN 1
                               ELSE 0
                           END AS IsStored
                    FROM information_schema.columns
                    WHERE table_schema = COALESCE(@schemaName, DATABASE())
                      AND table_name = @tableName
                      AND generation_expression IS NOT NULL
                      AND generation_expression <> ''
                    """, schemaName, tableName);
            }

            return new Dictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn>(0, StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> QueryComputedColumnMap(
            DbConnection connection,
            string sql,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            AddStringParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? null : schemaName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                var rawSql = Convert.ToString(reader["ComputedSql"]) ?? string.Empty;
                var isStored = ReaderHasColumn(reader, "IsStored")
                               && reader["IsStored"] != DBNull.Value
                               && ConvertMetadataBoolean(reader["IsStored"]);
                var (computedSql, stored) = NormalizeComputedColumnSql(rawSql + (isStored ? " STORED" : string.Empty));
                result[columnName] = new DynamicEntityTypeGenerator.ScaffoldComputedColumn(computedSql, stored);
            }

            return result;
        }

        public static string? GetSqliteCreateTableSql(DbConnection connection, string? schemaName, string tableName)
        {
            using var command = connection.CreateCommand();
            var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                ? string.Empty
                : EscapeIdentifier(connection, schemaName!) + ".";
            command.CommandText = $"SELECT sql FROM {schemaPrefix}sqlite_master WHERE type = 'table' AND name = @tableName";
            AddStringParameter(command, "@tableName", tableName);
            return Convert.ToString(command.ExecuteScalar());
        }

        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> ExtractSqliteGeneratedColumns(string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractGeneratedColumns(createTableSql)
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => new DynamicEntityTypeGenerator.ScaffoldComputedColumn(pair.Value.Sql, pair.Value.Stored),
                    StringComparer.OrdinalIgnoreCase);

        public static (string Sql, bool Stored) NormalizeComputedColumnSql(string raw)
            => ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(raw);

        private static bool ConvertMetadataBoolean(object value)
            => value switch
            {
                bool b => b,
                byte b => b != 0,
                short s => s != 0,
                int i => i != 0,
                long l => l != 0,
                string s => s.Equals("true", StringComparison.OrdinalIgnoreCase)
                            || s.Equals("yes", StringComparison.OrdinalIgnoreCase)
                            || s.Equals("1", StringComparison.OrdinalIgnoreCase),
                _ => Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture) != 0
            };
    }
}
