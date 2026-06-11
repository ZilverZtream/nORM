#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static class DynamicEntitySchemaResolver
    {
        public static (string? Schema, string Table) SplitSchema(string identifier)
        {
            var idx = identifier.IndexOf('.');
            if (idx > 0 && idx < identifier.Length - 1)
                return (identifier[..idx], identifier[(idx + 1)..]);
            return (null, identifier);
        }

        public static (string? Schema, string Table, TColumns Columns, bool IsReadOnlyEntity) ResolveTableSchema<TColumns>(
            DbConnection connection,
            string tableName,
            Func<DbConnection, string?, string, TColumns> getTableSchema,
            Func<TColumns> emptyColumnsFactory,
            Func<DbConnection, string?, string, bool> isReadOnlyDynamicObject)
        {
            if (tableName.Contains('.', StringComparison.Ordinal))
            {
                var exactFound = TryGetTableSchema(connection, null, tableName, getTableSchema, emptyColumnsFactory, out var exactColumns);
                var (schemaName, bareTable) = SplitSchema(tableName);
                var schemaFound = TryGetTableSchema(connection, schemaName, bareTable, getTableSchema, emptyColumnsFactory, out var schemaColumns);

                if (exactFound && schemaFound)
                {
                    throw new NormConfigurationException(
                        $"Dynamic scaffolding table name '{tableName}' is ambiguous: it matches both a literal table name and a schema-qualified table. " +
                        "Use a typed model or remove the naming collision before using Query(string).");
                }

                if (exactFound)
                    return (null, tableName, exactColumns, isReadOnlyDynamicObject(connection, null, tableName));

                if (schemaFound)
                    return (schemaName, bareTable, schemaColumns, isReadOnlyDynamicObject(connection, schemaName, bareTable));
            }

            var (fallbackSchemaName, fallbackBareTable) = SplitSchema(tableName);
            if (fallbackSchemaName is null)
                fallbackSchemaName = ResolveUniqueUnqualifiedSchema(connection, fallbackBareTable);

            var columns = getTableSchema(connection, fallbackSchemaName, fallbackBareTable);
            return (fallbackSchemaName, fallbackBareTable, columns, isReadOnlyDynamicObject(connection, fallbackSchemaName, fallbackBareTable));
        }

        public static bool TryGetTableSchema<TColumns>(
            DbConnection connection,
            string? schemaName,
            string tableName,
            Func<DbConnection, string?, string, TColumns> getTableSchema,
            Func<TColumns> emptyColumnsFactory,
            out TColumns columns)
        {
            try
            {
                columns = getTableSchema(connection, schemaName, tableName);
                return true;
            }
            catch (DbException)
            {
                columns = emptyColumnsFactory();
                return false;
            }
        }

        public static string? ResolveUniqueUnqualifiedSchema(DbConnection connection, string tableName)
        {
            var schemas = GetMatchingObjectSchemas(connection, tableName);
            if (schemas.Count > 1)
            {
                throw new NormConfigurationException(
                    $"Dynamic scaffolding table name '{tableName}' is ambiguous because it exists in multiple schemas/catalogs: " +
                    string.Join(", ", schemas.Select(schema => "'" + schema + "'")) +
                    ". Use a schema-qualified table name before using Query(string).");
            }

            if (schemas.Count == 1)
            {
                var connectionName = connection.GetType().Name;
                if (IsSqlServerConnection(connectionName) || IsPostgresConnection(connectionName))
                    return schemas[0];
            }

            return null;
        }

        public static IReadOnlyList<string> GetMatchingObjectSchemas(DbConnection connection, string tableName)
        {
            var connectionName = connection.GetType().Name;
            if (IsSqliteConnection(connectionName))
                return GetSqliteMatchingObjectSchemas(connection, tableName);

            if (IsSqlServerConnection(connectionName))
            {
                return QuerySchemaNameList(connection, """
                    SELECT s.name AS SchemaName
                    FROM sys.objects o
                    INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
                    WHERE o.name = @tableName
                      AND o.type IN ('U', 'V')
                      AND o.is_ms_shipped = 0
                    ORDER BY s.name
                    """, tableName);
            }

            if (IsPostgresConnection(connectionName))
            {
                return QuerySchemaNameList(connection, """
                    SELECT table_schema AS SchemaName
                    FROM information_schema.tables
                    WHERE table_name = @tableName
                      AND table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_schema
                    """, tableName);
            }

            return Array.Empty<string>();
        }

        public static IReadOnlyList<string> GetSqliteMatchingObjectSchemas(DbConnection connection, string tableName)
        {
            var schemas = new List<string>();
            using (var schemaCommand = connection.CreateCommand())
            {
                schemaCommand.CommandText = "PRAGMA database_list";
                using var reader = schemaCommand.ExecuteReader();
                while (reader.Read())
                {
                    var schemaName = ReaderHasColumn(reader, "name")
                        ? Convert.ToString(reader["name"])
                        : reader.FieldCount > 1 ? Convert.ToString(reader[1]) : null;
                    if (!string.IsNullOrWhiteSpace(schemaName))
                        schemas.Add(schemaName);
                }
            }

            var matches = new List<string>();
            foreach (var schema in schemas.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT name FROM {EscapeIdentifier(connection, schema)}.sqlite_master WHERE type IN ('table', 'view') AND name = @tableName LIMIT 1";
                AddStringParameter(command, "@tableName", tableName);
                if (command.ExecuteScalar() is not null)
                    matches.Add(schema);
            }

            return matches;
        }

        public static IReadOnlyList<string> QuerySchemaNameList(DbConnection connection, string sql, string tableName)
        {
            var result = new List<string>();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            AddStringParameter(cmd, "@tableName", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var schemaName = Convert.ToString(reader["SchemaName"]);
                if (!string.IsNullOrWhiteSpace(schemaName))
                    result.Add(schemaName);
            }

            return result
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(schema => schema, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        public static string EscapeQualified(DbConnection connection, string? schema, string table)
            => string.IsNullOrEmpty(schema)
                ? EscapeIdentifier(connection, table)
                : $"{EscapeIdentifier(connection, schema!)}.{EscapeIdentifier(connection, table)}";

        public static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        public static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            var name = connection.GetType().Name.ToLowerInvariant();
            return name switch
            {
                var n when n.Contains("sqlite") => $"\"{identifier.Replace("\"", "\"\"")}\"",
                var n when n.Contains("npgsql") => $"\"{identifier.Replace("\"", "\"\"")}\"",
                var n when n.Contains("mysql") => $"`{identifier.Replace("`", "``")}`",
                var n when n.Contains("sqlconnection") => $"[{identifier.Replace("]", "]]")}]",
                _ => $"\"{identifier.Replace("\"", "\"\"")}\""
            };
        }

        private static void AddStringParameter(DbCommand command, string name, string? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = DbType.String;
            parameter.Value = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
            command.Parameters.Add(parameter);
        }

        public static bool IsSqliteConnection(string connectionName)
            => connectionName.Contains("Sqlite", StringComparison.OrdinalIgnoreCase);

        public static bool IsPostgresConnection(string connectionName)
            => connectionName.Contains("Npgsql", StringComparison.OrdinalIgnoreCase);

        public static bool IsMySqlConnection(string connectionName)
            => connectionName.Contains("MySql", StringComparison.OrdinalIgnoreCase);

        public static bool IsSqlServerConnection(string connectionName)
            => connectionName.Contains("SqlConnection", StringComparison.OrdinalIgnoreCase)
               && !IsPostgresConnection(connectionName)
               && !IsMySqlConnection(connectionName)
               && !IsSqliteConnection(connectionName);
    }
}
