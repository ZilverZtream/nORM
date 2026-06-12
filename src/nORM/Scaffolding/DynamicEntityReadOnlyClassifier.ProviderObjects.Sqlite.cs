#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool IsSqliteDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
        {
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"""
                SELECT type, sql
                FROM {DynamicEntityConnectionKind.EscapeIdentifier(connection, schema)}.sqlite_master
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

        private static bool HasSqliteProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
        {
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"""
                SELECT 1
                FROM {DynamicEntityConnectionKind.EscapeIdentifier(connection, schema)}.sqlite_master
                WHERE type = 'trigger'
                  AND tbl_name = @tableName
                LIMIT 1
                """;
            AddStringParameter(cmd, "@tableName", tableName);
            return cmd.ExecuteScalar() is not null;
        }
    }
}
