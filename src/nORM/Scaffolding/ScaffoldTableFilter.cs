#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Core;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldTableFilter
    {
        public static IReadOnlyList<ScaffoldTableInfo> FilterTables(
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            var requested = GetRequestedTableFilters(options);
            var requestedSchemas = GetRequestedSchemaFilters(options);
            if (requested.Length == 0 && requestedSchemas.Length == 0)
                return tables;

            var ambiguousRequests = requested
                .Select(request => new
                {
                    Request = request,
                    Matches = tables
                        .Where(table => MatchesTableFilter(provider, table, request))
                        .GroupBy(table => (table.Schema ?? string.Empty) + "\u001f" + table.Name, StringComparer.OrdinalIgnoreCase)
                        .Select(group => DisplayTableMatch(group.First()))
                        .OrderBy(value => value, StringComparer.Ordinal)
                        .ToArray()
                })
                .Where(match => match.Matches.Length > 1)
                .ToArray();

            if (ambiguousRequests.Length > 0)
            {
                throw new NormConfigurationException(
                    "Scaffolding table filter is ambiguous because it matches multiple discovered tables: " +
                    string.Join("; ", ambiguousRequests.Select(match => $"{match.Request} matched {string.Join(", ", match.Matches)}")) +
                    ". Use schema-qualified table filters when the ambiguity is across schemas; literal dotted table names that collide with schema-qualified names must be scaffolded without a table filter.");
            }

            var selected = tables
                .Where(table =>
                    requested.Any(request => MatchesTableFilter(provider, table, request))
                    || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, table.Schema, schema, filterCatalog)))
                .Select(table => ApplyRequestedTableCasing(provider, table, requested))
                .ToArray();

            var missing = requested
                .Where(request => !tables.Any(table => MatchesTableFilter(provider, table, request)))
                .ToArray();

            if (missing.Length > 0)
            {
                var skippedMatches = skippedObjects
                    .Where(obj => missing.Any(request => MatchesSkippedObjectFilter(provider, obj, request)))
                    .Select(obj => $"{obj.Kind} {TableKey(obj.Schema, obj.Name)}")
                    .OrderBy(value => value, StringComparer.Ordinal)
                    .ToArray();

                if (skippedMatches.Length > 0)
                {
                    throw new NormConfigurationException(
                        "Scaffolding table filter matched database object(s) that v1 scaffolding does not emit as entity classes: " +
                        string.Join(", ", skippedMatches) +
                        ". Scaffold base tables or create a provider-neutral entity manually.");
                }

                throw new NormConfigurationException(
                    "Scaffolding table filter did not match discovered table(s): " +
                    string.Join(", ", missing));
            }

            var missingSchemas = requestedSchemas
                .Where(schema =>
                    !tables.Any(table => MatchesSchemaFilter(provider, table.Schema, schema, filterCatalog))
                    && !skippedObjects.Any(obj => MatchesSchemaFilter(provider, obj.Schema, schema, filterCatalog)))
                .ToArray();

            if (missingSchemas.Length > 0)
            {
                throw new NormConfigurationException(
                    "Scaffolding schema filter did not match discovered schema(s): " +
                    string.Join(", ", missingSchemas));
            }

            return selected;
        }

        public static void EnsureNoTableKeyCollisions(IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var collisions = tables
                .GroupBy(table => TableKey(table.Schema, table.Name), StringComparer.OrdinalIgnoreCase)
                .Select(group => new
                {
                    DisplayKey = group.Key,
                    Matches = group
                        .GroupBy(table => (table.Schema ?? string.Empty) + "\u001f" + table.Name, StringComparer.OrdinalIgnoreCase)
                        .Select(inner => DisplayTableMatch(inner.First()))
                        .OrderBy(value => value, StringComparer.Ordinal)
                        .ToArray()
                })
                .Where(group => group.Matches.Length > 1)
                .ToArray();

            if (collisions.Length == 0)
                return;

            throw new NormConfigurationException(
                "Scaffolding discovered tables whose display names collide with schema-qualified names: " +
                string.Join("; ", collisions.Select(c => $"{c.DisplayKey} matched {string.Join(", ", c.Matches)}")) +
                ". Rename one table or scaffold a provider-specific model manually; v1 table filters cannot disambiguate literal dotted table names from schema-qualified table names.");
        }

        public static IReadOnlyList<ScaffoldSkippedObjectInfo> FilterSkippedObjects(
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog,
            IReadOnlyList<ScaffoldSkippedObjectInfo>? emittedQueryArtifacts = null)
        {
            var requested = GetRequestedTableFilters(options);
            var requestedSchemas = GetRequestedSchemaFilters(options);
            if (requested.Length == 0 && requestedSchemas.Length == 0)
                return skippedObjects;

            var emittedVirtualTables = (emittedQueryArtifacts ?? Array.Empty<ScaffoldSkippedObjectInfo>())
                .Where(obj => string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase))
                .ToArray();

            return skippedObjects
                .Where(obj => requested.Any(request => MatchesSkippedObjectFilter(provider, obj, request))
                              || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, obj.Schema, schema, filterCatalog))
                              || IsShadowOfEmittedVirtualTable(obj, emittedVirtualTables))
                .ToArray();
        }

        public static bool IsQueryArtifactObject(ScaffoldSkippedObjectInfo obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
               || (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase) && ScaffoldSkippedObjectMetadataBuilder.IsTableLikeSqlServerSynonym(obj.Detail));

        public static bool ShouldEmitQueryArtifactObject(
            ScaffoldSkippedObjectInfo obj,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            if (!IsQueryArtifactObject(obj))
                return false;

            if (options.EmitViewEntities || options.EmitQueryArtifacts)
                return true;

            var requested = GetRequestedTableFilters(options);
            var requestedSchemas = GetRequestedSchemaFilters(options);
            if (requested.Length == 0 && requestedSchemas.Length == 0)
                return IsDefaultQueryArtifactObject(obj);

            return requested.Any(request => MatchesSkippedObjectFilter(provider, obj, request))
                   || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, obj.Schema, schema, filterCatalog));
        }

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
                   : provider is SqliteProvider && string.Equals(requested, "main", StringComparison.OrdinalIgnoreCase)
                     || IsMySqlProvider(provider) && !string.IsNullOrWhiteSpace(filterCatalog) && string.Equals(filterCatalog, requested, StringComparison.OrdinalIgnoreCase);

        public static bool MatchesSkippedObjectFilter(DatabaseProvider provider, ScaffoldSkippedObjectInfo obj, string requested)
            => MatchesSkippedObjectFilter(obj, requested)
               || IsDefaultSqliteSchemaQualifiedFilter(provider, obj.Schema, obj.Name, requested);

        private static ScaffoldTableInfo ApplyRequestedTableCasing(DatabaseProvider provider, ScaffoldTableInfo table, IReadOnlyList<string> requested)
        {
            var request = requested.FirstOrDefault(filter => MatchesTableFilter(provider, table, filter));
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

            if (requestedSchema is null && string.Equals(request, table.Name, StringComparison.OrdinalIgnoreCase))
                return new ScaffoldTableInfo(request, table.Schema);

            return table;
        }

        private static bool MatchesTableFilter(DatabaseProvider provider, ScaffoldTableInfo table, string requested)
            => MatchesTableFilter(table, requested)
               || IsDefaultSqliteSchemaQualifiedFilter(provider, table.Schema, table.Name, requested);

        private static bool MatchesTableFilter(ScaffoldTableInfo table, string requested)
            => string.Equals(table.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(table.Schema, table.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static bool MatchesSkippedObjectFilter(ScaffoldSkippedObjectInfo obj, string requested)
            => string.Equals(obj.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(obj.Schema, obj.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static bool IsShadowOfEmittedVirtualTable(
            ScaffoldSkippedObjectInfo obj,
            IReadOnlyList<ScaffoldSkippedObjectInfo> emittedVirtualTables)
            => string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase)
               && emittedVirtualTables.Any(vt =>
                   string.Equals(vt.Schema ?? string.Empty, obj.Schema ?? string.Empty, StringComparison.OrdinalIgnoreCase)
                   && obj.Name.StartsWith(vt.Name + "_", StringComparison.OrdinalIgnoreCase));

        private static bool IsDefaultQueryArtifactObject(ScaffoldSkippedObjectInfo obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase);

        private static bool IsDefaultSqliteSchemaQualifiedFilter(DatabaseProvider provider, string? schemaName, string objectName, string requested)
            => provider is SqliteProvider
               && string.IsNullOrWhiteSpace(schemaName)
               && string.Equals(GetSchemaNameOrNull(requested), "main", StringComparison.OrdinalIgnoreCase)
               && string.Equals(GetUnqualifiedName(requested), objectName, StringComparison.OrdinalIgnoreCase);

        private static string DisplayTableMatch(ScaffoldTableInfo table)
            => string.IsNullOrWhiteSpace(table.Schema)
                ? "<default>." + table.Name
                : TableKey(table.Schema, table.Name);

        private static bool IsMySqlProvider(DatabaseProvider provider)
            => provider.GetType().Name.Contains("MySql", StringComparison.OrdinalIgnoreCase);

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
