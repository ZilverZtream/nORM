#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
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
