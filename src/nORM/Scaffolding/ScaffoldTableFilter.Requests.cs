#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        public static string[] GetRequestedTableFilters(ScaffoldOptions options)
        {
            if (options.Tables is null)
                return Array.Empty<string>();

            return options.Tables
                .Where(table => !string.IsNullOrWhiteSpace(table))
                .Select(table => table.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        public static string[] GetRequestedSchemaFilters(ScaffoldOptions options)
        {
            if (options.Schemas is null)
                return Array.Empty<string>();

            return options.Schemas
                .Where(schema => !string.IsNullOrWhiteSpace(schema))
                .Select(schema => schema.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        public static bool MatchesSchemaFilter(DatabaseProvider provider, string? schemaName, string requested, string? filterCatalog)
            => !string.IsNullOrWhiteSpace(schemaName)
                   ? string.Equals(schemaName, requested, StringComparison.OrdinalIgnoreCase)
                   : ScaffoldProviderKind.IsSqlite(provider) && string.Equals(requested, "main", StringComparison.OrdinalIgnoreCase)
                     || ScaffoldProviderKind.IsMySql(provider) && !string.IsNullOrWhiteSpace(filterCatalog) && string.Equals(filterCatalog, requested, StringComparison.OrdinalIgnoreCase);

        public static bool MatchesSkippedObjectFilter(DatabaseProvider provider, ScaffoldSkippedObjectInfo obj, string requested, string? filterCatalog = null)
            => MatchesSkippedObjectFilter(obj, requested)
               || IsDefaultSqliteSchemaQualifiedFilter(provider, obj.Schema, obj.Name, requested)
               || IsDefaultMySqlCatalogQualifiedFilter(provider, obj.Schema, obj.Name, requested, filterCatalog);

        private static ScaffoldTableInfo ApplyRequestedTableCasing(
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            IReadOnlyList<string> requested,
            string? filterCatalog)
        {
            var request = requested.FirstOrDefault(filter => MatchesTableFilter(provider, table, filter, filterCatalog));
            if (string.IsNullOrWhiteSpace(request))
                return table;

            var requestedSchema = GetSchemaNameOrNull(request);
            if (!string.IsNullOrWhiteSpace(requestedSchema)
                && string.Equals(requestedSchema, table.Schema, StringComparison.OrdinalIgnoreCase))
            {
                return new ScaffoldTableInfo(GetUnqualifiedName(request), requestedSchema);
            }

            if (!string.IsNullOrWhiteSpace(requestedSchema)
                && IsDefaultSqliteSchemaQualifiedFilter(provider, table.Schema, table.Name, request))
            {
                return new ScaffoldTableInfo(GetUnqualifiedName(request), table.Schema);
            }

            if (!string.IsNullOrWhiteSpace(requestedSchema)
                && IsDefaultMySqlCatalogQualifiedFilter(provider, table.Schema, table.Name, request, filterCatalog))
            {
                return new ScaffoldTableInfo(GetUnqualifiedName(request), table.Schema);
            }

            if (requestedSchema is null && string.Equals(request, table.Name, StringComparison.OrdinalIgnoreCase))
                return new ScaffoldTableInfo(request, table.Schema);

            return table;
        }

        private static bool MatchesTableFilter(DatabaseProvider provider, ScaffoldTableInfo table, string requested, string? filterCatalog)
            => MatchesTableFilter(table, requested)
               || IsDefaultSqliteSchemaQualifiedFilter(provider, table.Schema, table.Name, requested)
               || IsDefaultMySqlCatalogQualifiedFilter(provider, table.Schema, table.Name, requested, filterCatalog);

        private static bool MatchesTableFilter(ScaffoldTableInfo table, string requested)
            => string.Equals(table.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(table.Schema, table.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static bool MatchesSkippedObjectFilter(ScaffoldSkippedObjectInfo obj, string requested)
            => string.Equals(obj.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(obj.Schema, obj.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static bool IsDefaultSqliteSchemaQualifiedFilter(DatabaseProvider provider, string? schemaName, string objectName, string requested)
            => ScaffoldProviderKind.IsSqlite(provider)
               && string.IsNullOrWhiteSpace(schemaName)
               && string.Equals(GetSchemaNameOrNull(requested), "main", StringComparison.OrdinalIgnoreCase)
               && string.Equals(GetUnqualifiedName(requested), objectName, StringComparison.OrdinalIgnoreCase);

        private static bool IsDefaultMySqlCatalogQualifiedFilter(DatabaseProvider provider, string? schemaName, string objectName, string requested, string? filterCatalog)
            => ScaffoldProviderKind.IsMySql(provider)
               && string.IsNullOrWhiteSpace(schemaName)
               && !string.IsNullOrWhiteSpace(filterCatalog)
               && string.Equals(GetSchemaNameOrNull(requested), filterCatalog, StringComparison.OrdinalIgnoreCase)
               && string.Equals(GetUnqualifiedName(requested), objectName, StringComparison.OrdinalIgnoreCase);

        private static string DisplayTableMatch(ScaffoldTableInfo table)
            => string.IsNullOrWhiteSpace(table.Schema)
                ? "<default>." + table.Name
                : TableKey(table.Schema, table.Name);

        private static string TableKey(string? schema, string table)
            => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";

        private static string GetUnqualifiedName(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            return idx >= 0 ? identifier[(idx + 1)..] : identifier;
        }

        private static string? GetSchemaNameOrNull(string identifier)
        {
            var idx = identifier.IndexOf('.');
            return idx > 0 ? identifier[..idx] : null;
        }
    }
}
