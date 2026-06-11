#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        /// <summary>
        /// Converts a database object name to PascalCase by removing separators and capitalizing
        /// the first letter of each segment.
        /// </summary>
        private static string ToPascalCase(string name)
            => ScaffoldNameHelper.ToPascalCase(name);

        private static string ToScaffoldClrName(string databaseName, bool useDatabaseNames)
            => ScaffoldNameHelper.ToScaffoldClrName(databaseName, useDatabaseNames);

        private static string ToScaffoldClrNamePart(string databaseName, bool useDatabaseNames)
            => ScaffoldNameHelper.ToScaffoldClrNamePart(databaseName, useDatabaseNames);

        private static string ToNavigationName(string generatedName)
            => ScaffoldNameHelper.ToNavigationName(generatedName);

        /// <summary>
        /// Returns the last segment of a dot-delimited identifier.
        /// </summary>
        internal static string GetUnqualifiedName(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            return idx >= 0 ? identifier[(idx + 1)..] : identifier;
        }

        /// <summary>
        /// Returns the schema segment (before the first dot) or null if no dot or empty schema.
        /// </summary>
        internal static string? GetSchemaNameOrNull(string identifier)
        {
            var idx = identifier.IndexOf('.');
            return idx > 0 ? identifier[..idx] : null;
        }

        /// <summary>
        /// Combines schema and table with a dot separator when schema is present.
        /// No SQL escaping is applied.
        /// </summary>
        internal static string EscapeQualifiedIfNeeded(string? schema, string table)
            => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";

        /// <summary>
        /// Escapes an identifier using the appropriate provider for the given connection.
        /// For schema-qualified names (containing a dot), both parts are escaped.
        /// </summary>
        internal static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            DatabaseProvider provider = connection switch
            {
                Microsoft.Data.Sqlite.SqliteConnection => new SqliteProvider(),
                _ => new SqliteProvider()
            };
            var dot = identifier.IndexOf('.');
            if (dot >= 0)
            {
                var schema = identifier[..dot];
                var table = identifier[(dot + 1)..];
                return $"{provider.Escape(schema)}.{provider.Escape(table)}";
            }
            return provider.Escape(identifier);
        }

        /// <summary>
        /// Combines schema and table with a dot separator. No SQL escaping.
        /// </summary>
        internal static string EscapeQualified(string schema, string table)
            => $"{schema}.{table}";

        /// <summary>
        /// Escapes schema and table identifiers using the given provider's rules.
        /// </summary>
        private static string EscapeQualified(DatabaseProvider provider, string? schema, string table)
            => IdentifierEscaping.EscapeTable(provider, table, schema);

        /// <summary>
        /// Escapes an identifier so it is valid in generated C# code. Reserved keywords are
        /// prefixed with <c>@</c>; other invalid characters are replaced with underscores.
        /// </summary>
        private static string EscapeCSharpIdentifier(string identifier)
            => ScaffoldNameHelper.EscapeCSharpIdentifier(identifier);

        private static bool IsValidNamespaceName(string namespaceName)
            => ScaffoldNameHelper.IsValidNamespaceName(namespaceName);

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;

        private static string TrimIdSuffix(string name)
        {
            if (name.EndsWith("Id", StringComparison.Ordinal) && name.Length > 2)
                return name[..^2];

            if (name.EndsWith("_id", StringComparison.OrdinalIgnoreCase) && name.Length > 3)
                return name[..^3];

            return name;
        }

        private static string MakeUnique(string baseName, HashSet<string> existingNames)
            => ScaffoldNameHelper.MakeUnique(baseName, existingNames);

        private static string MakeUniqueContextName(string contextName, IEnumerable<string> entityNames)
            => ScaffoldNameHelper.MakeUniqueContextName(contextName, entityNames);

        private static string Pluralize(string name)
            => ScaffoldNameHelper.Pluralize(name);
    }
}
