#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSemicolonParser
    {
        private static List<SemicolonValueMarker> SelectBestOrderedSemicolonValueMarkers(
            string detail,
            IReadOnlyList<SemicolonValueMarker> markers,
            IReadOnlyList<IReadOnlyList<string>> keyOrders,
            Func<string, string, bool>? isCompleteValue)
        {
            var best = new List<SemicolonValueMarker>();
            foreach (var keyOrder in keyOrders)
            {
                var candidate = SelectOrderedSemicolonValueMarkers(detail, markers, keyOrder, isCompleteValue);
                if (candidate.Count > best.Count)
                    best = candidate;
            }

            return best;
        }

        private static List<SemicolonValueMarker> SelectOrderedSemicolonValueMarkers(
            string detail,
            IReadOnlyList<SemicolonValueMarker> markers,
            IReadOnlyList<string> keyOrder,
            Func<string, string, bool>? isCompleteValue)
        {
            var result = new List<SemicolonValueMarker>();
            var expectedIndex = 0;
            foreach (var marker in markers)
            {
                var matchIndex = IndexOfSemicolonValueKey(keyOrder, marker.Key, expectedIndex);
                if (matchIndex < 0)
                    continue;

                if (result.Count > 0)
                {
                    var previous = result[^1];
                    var value = detail.Substring(previous.ValueStart, marker.SemicolonIndex - previous.ValueStart);
                    if (isCompleteValue is not null && !isCompleteValue(previous.Key, value))
                        continue;
                }

                result.Add(marker);
                expectedIndex = matchIndex + 1;
            }

            return result;
        }

        private static int IndexOfSemicolonValueKey(IReadOnlyList<string> keyOrder, string key, int start)
        {
            for (var i = start; i < keyOrder.Count; i++)
            {
                if (keyOrder[i].Equals(key, StringComparison.OrdinalIgnoreCase))
                    return i;
            }

            return -1;
        }
    }
}
