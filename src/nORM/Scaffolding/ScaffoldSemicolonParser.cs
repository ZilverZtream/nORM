#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSemicolonParser
    {
        public static IReadOnlyDictionary<string, string> Parse(string detail, out string header)
            => Parse(detail, out header, keyOrders: null, isCompleteValue: null);

        public static IReadOnlyDictionary<string, string> ParseRoutine(
            string detail,
            out string header,
            Func<string, string, bool> isCompleteValue)
            => Parse(detail, out header, GetRoutineSemicolonValueKeyOrders(detail), isCompleteValue);

        private static IReadOnlyDictionary<string, string> Parse(
            string detail,
            out string header,
            IReadOnlyList<IReadOnlyList<string>>? keyOrders,
            Func<string, string, bool>? isCompleteValue)
        {
            var markers = FindSemicolonValueMarkers(detail);

            if (keyOrders is { Count: > 0 })
                markers = SelectBestOrderedSemicolonValueMarkers(detail, markers, keyOrders, isCompleteValue);

            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (markers.Count == 0)
            {
                header = detail.Trim();
                return values;
            }

            header = detail[..markers[0].SemicolonIndex].Trim();
            for (var i = 0; i < markers.Count; i++)
            {
                var marker = markers[i];
                var valueEnd = i + 1 < markers.Count ? markers[i + 1].SemicolonIndex : detail.Length;
                values[marker.Key] = detail.Substring(marker.ValueStart, valueEnd - marker.ValueStart).Trim();
            }

            return values;
        }

        private readonly record struct SemicolonValueMarker(
            int SemicolonIndex,
            string Key,
            int ValueStart);
    }
}
