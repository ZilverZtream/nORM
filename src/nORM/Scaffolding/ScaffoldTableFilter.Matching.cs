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
        {
            var filter = ParseObjectFilterRequest(requested);
            return MatchesSkippedObjectKindSelector(filter.KindSelector, obj)
                   && (MatchesSkippedObjectFilter(obj, filter.Identifier)
                       || IsDefaultSqliteSchemaQualifiedFilter(provider, obj.Schema, obj.Name, filter.Identifier)
                       || IsDefaultMySqlCatalogQualifiedFilter(provider, obj.Schema, obj.Name, filter.Identifier, filterCatalog));
        }

        private static bool MatchesTableFilter(DatabaseProvider provider, ScaffoldTableInfo table, string requested, string? filterCatalog)
        {
            var filter = ParseObjectFilterRequest(requested);
            return MatchesTableKindSelector(filter.KindSelector, table)
                   && (MatchesTableFilter(table, filter.Identifier)
                       || IsDefaultSqliteSchemaQualifiedFilter(provider, table.Schema, table.Name, filter.Identifier)
                       || IsDefaultMySqlCatalogQualifiedFilter(provider, table.Schema, table.Name, filter.Identifier, filterCatalog));
        }

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

        private static bool MatchesTableKindSelector(string? kindSelector, ScaffoldTableInfo table)
            => kindSelector is null
               || string.Equals(kindSelector, "QueryArtifact", StringComparison.OrdinalIgnoreCase) && IsQueryArtifactKind(table.Kind)
               || string.Equals(table.Kind, kindSelector, StringComparison.OrdinalIgnoreCase);

        private static bool MatchesSkippedObjectKindSelector(string? kindSelector, ScaffoldSkippedObjectInfo obj)
            => kindSelector is null
               || string.Equals(kindSelector, "QueryArtifact", StringComparison.OrdinalIgnoreCase) && IsQueryArtifactObject(obj)
               || string.Equals(obj.Kind, kindSelector, StringComparison.OrdinalIgnoreCase);

        private static bool IsQueryArtifactKind(string kind)
            => string.Equals(kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
               || string.Equals(kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
               || string.Equals(kind, "Synonym", StringComparison.OrdinalIgnoreCase);
    }
}
