#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSemicolonParser
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
            var markers = new List<SemicolonValueMarker>();
            for (var i = 0; i < detail.Length; i++)
            {
                if (detail[i] == ';' && TryReadSemicolonValueMarker(detail, i, out var key, out var valueStart))
                    markers.Add(new SemicolonValueMarker(i, key, valueStart));
            }

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

        private static IReadOnlyList<IReadOnlyList<string>> GetRoutineSemicolonValueKeyOrders(string detail)
        {
            var firstMarker = detail.IndexOf(';');
            var header = firstMarker < 0 ? detail.Trim() : detail[..firstMarker].Trim();
            if (header.StartsWith("SQL Server stored procedure", StringComparison.OrdinalIgnoreCase))
            {
                return new[]
                {
                    new[] { "parameters", "outputParameters", "parameterModes", "resultColumns" }
                };
            }

            if (header.StartsWith("SQL Server ", StringComparison.OrdinalIgnoreCase))
            {
                return new[]
                {
                    new[] { "parameters", "outputParameters", "callShape", "parameterModes", "dataType", "resultColumns" },
                    new[] { "parameters", "outputParameters", "parameterModes", "callShape", "dataType", "resultColumns" }
                };
            }

            if (header.StartsWith("PostgreSQL ", StringComparison.OrdinalIgnoreCase)
                || header.StartsWith("MySQL ", StringComparison.OrdinalIgnoreCase))
            {
                return new[]
                {
                    new[] { "parameters", "outputParameters", "parameterModes", "callShape", "dataType" },
                    new[] { "parameters", "outputParameters", "callShape", "parameterModes", "dataType" }
                };
            }

            return Array.Empty<IReadOnlyList<string>>();
        }

        private static bool TryReadSemicolonValueMarker(
            string detail,
            int semicolonIndex,
            out string key,
            out int valueStart)
        {
            key = string.Empty;
            valueStart = 0;

            var index = semicolonIndex + 1;
            var sawWhitespace = false;
            while (index < detail.Length && char.IsWhiteSpace(detail[index]))
            {
                sawWhitespace = true;
                index++;
            }

            if (!sawWhitespace || index >= detail.Length || !char.IsLetter(detail[index]))
                return false;

            var keyStart = index;
            while (index < detail.Length && char.IsLetterOrDigit(detail[index]))
                index++;

            var keyEnd = index;
            while (index < detail.Length && char.IsWhiteSpace(detail[index]))
                index++;

            if (index >= detail.Length || detail[index] != '=')
                return false;

            key = detail.Substring(keyStart, keyEnd - keyStart);
            if (!IsKnownSemicolonValueKey(key))
                return false;

            valueStart = index + 1;
            return true;
        }

        private static bool IsKnownSemicolonValueKey(string key)
            => key.Equals("parameters", StringComparison.OrdinalIgnoreCase)
               || key.Equals("outputParameters", StringComparison.OrdinalIgnoreCase)
               || key.Equals("parameterModes", StringComparison.OrdinalIgnoreCase)
               || key.Equals("resultColumns", StringComparison.OrdinalIgnoreCase)
               || key.Equals("callShape", StringComparison.OrdinalIgnoreCase)
               || key.Equals("dataType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("baseObject", StringComparison.OrdinalIgnoreCase)
               || key.Equals("baseType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("eventType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("status", StringComparison.OrdinalIgnoreCase)
               || key.Equals("intervalValue", StringComparison.OrdinalIgnoreCase)
               || key.Equals("intervalField", StringComparison.OrdinalIgnoreCase)
               || key.Equals("executeAt", StringComparison.OrdinalIgnoreCase)
               || key.Equals("starts", StringComparison.OrdinalIgnoreCase)
               || key.Equals("ends", StringComparison.OrdinalIgnoreCase)
               || key.Equals("expression", StringComparison.OrdinalIgnoreCase)
               || key.Equals("isUnique", StringComparison.OrdinalIgnoreCase)
               || key.Equals("filterSql", StringComparison.OrdinalIgnoreCase)
               || key.Equals("indexSql", StringComparison.OrdinalIgnoreCase)
               || key.Equals("indexType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("accessMethod", StringComparison.OrdinalIgnoreCase)
               || key.Equals("prefixColumns", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasNullsNotDistinct", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasNonDefaultOperatorClass", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasIndexCollation", StringComparison.OrdinalIgnoreCase)
               || key.Equals("hasNonDefaultNullOrdering", StringComparison.OrdinalIgnoreCase)
               || key.Equals("timing", StringComparison.OrdinalIgnoreCase)
               || key.Equals("event", StringComparison.OrdinalIgnoreCase)
               || key.Equals("orientation", StringComparison.OrdinalIgnoreCase)
               || key.Equals("triggerSql", StringComparison.OrdinalIgnoreCase)
               || key.Equals("isDisabled", StringComparison.OrdinalIgnoreCase)
               || key.Equals("isInsteadOf", StringComparison.OrdinalIgnoreCase)
               || key.Equals("temporalType", StringComparison.OrdinalIgnoreCase)
               || key.Equals("historyTable", StringComparison.OrdinalIgnoreCase);

        private readonly record struct SemicolonValueMarker(
            int SemicolonIndex,
            string Key,
            int ValueStart);
    }
}
