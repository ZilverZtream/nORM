#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldObjectSelectionBuilder
    {
        public static ScaffoldObjectSelectionInfo BuildSelection(
            IReadOnlyList<ScaffoldTableInfo> discoveredTables,
            IReadOnlyList<ScaffoldSkippedObjectInfo> discoveredSkippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            var candidateQueryArtifacts = discoveredSkippedObjects
                .Where(obj => ScaffoldTableFilter.ShouldEmitQueryArtifactObject(obj, options, provider, filterCatalog))
                .ToArray();
            var discoveredTablesAndViews = discoveredTables
                .Concat(candidateQueryArtifacts.Select(obj => new ScaffoldTableInfo(obj.Name, obj.Schema, obj.Kind)))
                .ToArray();
            var tables = ScaffoldTableFilter.FilterTables(
                discoveredTablesAndViews,
                discoveredSkippedObjects,
                options,
                provider,
                filterCatalog);
            var selectedTableKeys = tables
                .Select(table => TableKey(table.Schema, table.Name))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var emittedQueryArtifacts = candidateQueryArtifacts
                .Where(obj => selectedTableKeys.Contains(TableKey(obj.Schema, obj.Name)))
                .ToArray();
            var queryArtifactTableKeys = emittedQueryArtifacts
                .Select(obj => TableKey(obj.Schema, obj.Name))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            ScaffoldTableFilter.EnsureNoTableKeyCollisions(tables);

            var skippedObjects = ScaffoldTableFilter.FilterSkippedObjects(
                discoveredSkippedObjects.Where(obj => !emittedQueryArtifacts.Contains(obj)).ToArray(),
                options,
                provider,
                filterCatalog,
                emittedQueryArtifacts);

            return new ScaffoldObjectSelectionInfo(tables, skippedObjects, queryArtifactTableKeys);
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";
    }

    internal readonly record struct ScaffoldObjectSelectionInfo(
        IReadOnlyList<ScaffoldTableInfo> Tables,
        IReadOnlyList<ScaffoldSkippedObjectInfo> SkippedObjects,
        IReadOnlySet<string> QueryArtifactTableKeys);
}
