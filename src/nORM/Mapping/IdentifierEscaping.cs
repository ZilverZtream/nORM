using nORM.Providers;

#nullable enable

namespace nORM.Mapping
{
    internal static class IdentifierEscaping
    {
        internal static string EscapeSingle(DatabaseProvider provider, string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                return identifier;

            return provider switch
            {
                SqlServerProvider => $"[{identifier.Replace("]", "]]")}]",
                MySqlProvider => $"`{identifier.Replace("`", "``")}`",
                SqliteProvider => $"\"{identifier.Replace("\"", "\"\"")}\"",
                PostgresProvider => $"\"{identifier.Replace("\"", "\"\"")}\"",
                _ => provider.Escape(identifier)
            };
        }

        internal static string EscapeTable(DatabaseProvider provider, string tableName, string? schemaName)
            => string.IsNullOrWhiteSpace(schemaName)
                ? EscapeSingle(provider, tableName)
                : $"{provider.Escape(schemaName!)}.{EscapeSingle(provider, tableName)}";
    }
}
