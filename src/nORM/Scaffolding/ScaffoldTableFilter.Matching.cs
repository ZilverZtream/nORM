#nullable enable
using System;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        public static bool MatchesSchemaFilter(DatabaseProvider provider, string? schemaName, string requested, string? filterCatalog)
            => !string.IsNullOrWhiteSpace(schemaName)
                   ? string.Equals(schemaName, requested, StringComparison.OrdinalIgnoreCase)
                   : ScaffoldProviderKind.IsSqlite(provider) && string.Equals(requested, "main", StringComparison.OrdinalIgnoreCase)
                     || ScaffoldProviderKind.IsMySql(provider) && !string.IsNullOrWhiteSpace(filterCatalog) && string.Equals(filterCatalog, requested, StringComparison.OrdinalIgnoreCase);

        public static bool MatchesSkippedObjectFilter(DatabaseProvider provider, ScaffoldSkippedObjectInfo obj, string requested, string? filterCatalog = null)
            => MatchesSkippedObjectFilter(obj, requested)
               || IsDefaultSqliteSchemaQualifiedFilter(provider, obj.Schema, obj.Name, requested)
               || IsDefaultMySqlCatalogQualifiedFilter(provider, obj.Schema, obj.Name, requested, filterCatalog);

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
    }
}
