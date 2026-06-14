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
                ". Use schema-qualified table filters when the ambiguity is across schemas; literal dotted table names that collide with schema-qualified names must be scaffolded without a table filter; same-schema object-kind collisions must be scaffolded in separate runs.");
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

        private static bool MatchesSelectableSkippedObjectFilter(
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string requested,
            string? filterCatalog)
            => skippedObjects.Any(obj =>
                MatchesSkippedObjectFilter(provider, obj, requested, filterCatalog)
                && IsSelectableProviderStubObject(obj, options));

        private static bool IsSelectableProviderStubObject(ScaffoldSkippedObjectInfo obj, ScaffoldOptions options)
            => (string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase) && options.EmitRoutineStubs)
               || (string.Equals(obj.Kind, "Sequence", StringComparison.OrdinalIgnoreCase) && options.EmitSequenceStubs);

        private static string[] GetSelectableTableFilterMatches(
            IReadOnlyList<ScaffoldTableInfo> tables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string requested,
            string? filterCatalog)
            => tables
                .Where(table => MatchesTableFilter(provider, table, requested, filterCatalog))
                .GroupBy(table => table.Kind + "\u001f" + (table.Schema ?? string.Empty) + "\u001f" + table.Name, StringComparer.OrdinalIgnoreCase)
                .Select(group => DisplaySelectableTableMatch(group.First()))
                .Concat(skippedObjects
                    .Where(obj =>
                        MatchesSkippedObjectFilter(provider, obj, requested, filterCatalog)
                        && IsSelectableProviderStubObject(obj, options))
                    .GroupBy(obj => obj.Kind + "\u001f" + (obj.Schema ?? string.Empty) + "\u001f" + obj.Name, StringComparer.OrdinalIgnoreCase)
                    .Select(group => DisplaySkippedObjectMatch(group.First())))
                .OrderBy(value => value, StringComparer.Ordinal)
                .ToArray();

        private static string DisplaySkippedObjectMatch(ScaffoldSkippedObjectInfo obj)
            => $"{obj.Kind} {TableKey(obj.Schema, obj.Name)}";
    }
}
