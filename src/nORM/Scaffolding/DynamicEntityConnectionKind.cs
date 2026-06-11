#nullable enable
using System;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static class DynamicEntityConnectionKind
    {
        public static bool IsSqlite(DbConnection connection)
            => IsSqlite(connection.GetType().Name);

        public static bool IsSqlServer(DbConnection connection)
            => IsSqlServer(connection.GetType().Name);

        public static bool IsPostgres(DbConnection connection)
            => IsPostgres(connection.GetType().Name);

        public static bool IsMySql(DbConnection connection)
            => IsMySql(connection.GetType().Name);

        public static string EscapeQualified(DbConnection connection, string? schema, string table)
            => string.IsNullOrEmpty(schema)
                ? EscapeIdentifier(connection, table)
                : $"{EscapeIdentifier(connection, schema!)}.{EscapeIdentifier(connection, table)}";

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

        private static bool IsSqlite(string connectionName)
            => connectionName.Contains("Sqlite", StringComparison.OrdinalIgnoreCase);

        private static bool IsPostgres(string connectionName)
            => connectionName.Contains("Npgsql", StringComparison.OrdinalIgnoreCase);

        private static bool IsMySql(string connectionName)
            => connectionName.Contains("MySql", StringComparison.OrdinalIgnoreCase);

        private static bool IsSqlServer(string connectionName)
            => connectionName.Contains("SqlConnection", StringComparison.OrdinalIgnoreCase)
               && !IsPostgres(connectionName)
               && !IsMySql(connectionName)
               && !IsSqlite(connectionName);
    }
}
