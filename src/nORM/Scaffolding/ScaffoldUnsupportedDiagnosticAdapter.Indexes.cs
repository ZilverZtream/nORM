#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedDiagnosticAdapter
    {
        public static void RemoveSupportedDescendingIndexDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var supportedDescending = indexes
                .Where(static index => index.IsDescending)
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "DescendingIndex", StringComparison.OrdinalIgnoreCase)
                && supportedDescending.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        public static void RemoveSupportedIncludedColumnIndexDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var supportedIncluded = indexes
                .Where(static index => index.IsIncluded)
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "IncludedColumnIndex", StringComparison.OrdinalIgnoreCase)
                && supportedIncluded.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        public static void RemoveSupportedPartialIndexDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var supportedPartial = indexes
                .Where(static index => !string.IsNullOrWhiteSpace(index.FilterSql))
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "PartialIndex", StringComparison.OrdinalIgnoreCase)
                && supportedPartial.Contains(feature.TableKey + "\u001f" + feature.Name));
        }
    }
}
