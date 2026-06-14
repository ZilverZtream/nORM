#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Core;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
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
                        .Where(table => MatchesTableFilter(provider, table, request, filterCatalog))
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
                    requested.Any(request => MatchesTableFilter(provider, table, request, filterCatalog))
                    || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, table.Schema, schema, filterCatalog)))
                .Select(table => ApplyRequestedTableCasing(provider, table, requested, filterCatalog))
                .ToArray();

            var missing = requested
                .Where(request => !tables.Any(table => MatchesTableFilter(provider, table, request, filterCatalog)))
                .ToArray();

            if (missing.Length > 0)
            {
                var skippedMatches = skippedObjects
                    .Where(obj => missing.Any(request => MatchesSkippedObjectFilter(provider, obj, request, filterCatalog)))
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
                .Where(obj => requested.Any(request => MatchesSkippedObjectFilter(provider, obj, request, filterCatalog))
                              || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, obj.Schema, schema, filterCatalog))
                              || IsShadowOfEmittedVirtualTable(obj, emittedVirtualTables))
                .ToArray();
        }
    }
}
