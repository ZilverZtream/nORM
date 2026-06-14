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

            ValidateRequestedTableFilters(tables, skippedObjects, options, provider, requested, filterCatalog);

            var selected = tables
                .Where(table =>
                    requested.Any(request => MatchesTableFilter(provider, table, request, filterCatalog))
                    || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, table.Schema, schema, filterCatalog)))
                .Select(table => ApplyRequestedTableCasing(provider, table, requested, filterCatalog))
                .ToArray();

            ValidateRequestedSchemaFilters(tables, skippedObjects, provider, requestedSchemas, filterCatalog);

            return selected;
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
