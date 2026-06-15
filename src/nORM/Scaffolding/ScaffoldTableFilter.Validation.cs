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
        private static void ValidateRequestedTableFilters(
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            IReadOnlyList<string> requested,
            string? filterCatalog)
        {
            ThrowIfAmbiguousTableFilterRequest(tables, skippedObjects, options, provider, requested, filterCatalog);
            ThrowIfMissingTableFilterRequest(tables, skippedObjects, options, provider, requested, filterCatalog);
        }

        private static void ThrowIfAmbiguousTableFilterRequest(
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            IReadOnlyList<string> requested,
            string? filterCatalog)
        {
            var ambiguousRequests = requested
                .Select(request => new
                {
                    Request = request,
                    Matches = GetSelectableTableFilterMatches(tables, skippedObjects, options, provider, request, filterCatalog)
                })
                .Where(match => match.Matches.Length > 1)
                .ToArray();

            if (ambiguousRequests.Length == 0)
                return;

            throw new NormConfigurationException(
                "Scaffolding table filter is ambiguous because it matches multiple selectable database objects: " +
                string.Join("; ", ambiguousRequests.Select(match => $"{match.Request} matched {string.Join(", ", match.Matches)}")) +
                ". Use schema-qualified table filters when the ambiguity is across schemas; use object-kind selectors such as table:, view:, query:, routine:, or sequence: for same-schema object-kind collisions; use literal-name selectors such as name: or table:name: for literal dotted table names.");
        }

        private static void ThrowIfMissingTableFilterRequest(
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            IReadOnlyList<string> requested,
            string? filterCatalog)
        {
            var missing = requested
                .Where(request =>
                    !tables.Any(table => MatchesTableFilter(provider, table, request, filterCatalog))
                    && !MatchesSelectableSkippedObjectFilter(skippedObjects, options, provider, request, filterCatalog))
                .ToArray();

            if (missing.Length == 0)
                return;

            var skippedMatches = skippedObjects
                .Where(obj => missing.Any(request => MatchesSkippedObjectFilter(provider, obj, request, filterCatalog)))
                .Select(DisplaySkippedObjectMatch)
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

        private static void ValidateRequestedSchemaFilters(
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            DatabaseProvider provider,
            IReadOnlyList<string> requestedSchemas,
            string? filterCatalog)
        {
            var missingSchemas = requestedSchemas
                .Where(schema =>
                    !tables.Any(table => MatchesSchemaFilter(provider, table.Schema, schema, filterCatalog))
                    && !skippedObjects.Any(obj => MatchesSchemaFilter(provider, obj.Schema, schema, filterCatalog)))
                .ToArray();

            if (missingSchemas.Length == 0)
                return;

            throw new NormConfigurationException(
                "Scaffolding schema filter did not match discovered schema(s): " +
                string.Join(", ", missingSchemas));
        }
    }
}
