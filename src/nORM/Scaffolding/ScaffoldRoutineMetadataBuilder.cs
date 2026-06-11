#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildMetadata(string detail)
        {
            var values = ParseRoutineSemicolonValues(detail, out var header);
            var provider = ParseRoutineProvider(header);
            var routineType = ParseRoutineType(header);
            values.TryGetValue("dataType", out var dataType);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = provider,
                ["routineType"] = routineType,
                ["parameterCount"] = ParseNullableInt(values.TryGetValue("parameters", out var parameterCount) ? parameterCount : null),
                ["outputParameterCount"] = ParseNullableInt(values.TryGetValue("outputParameters", out var outputParameterCount) ? outputParameterCount : null)
            };

            if (!string.IsNullOrWhiteSpace(dataType))
                metadata["dataType"] = dataType;

            if (values.TryGetValue("callShape", out var callShape) && !string.IsNullOrWhiteSpace(callShape))
                metadata["callShape"] = callShape;
            else
            {
                var inferredCallShape = InferRoutineCallShape(provider, routineType, dataType);
                if (!string.IsNullOrWhiteSpace(inferredCallShape))
                    metadata["callShape"] = inferredCallShape;
            }

            if (values.TryGetValue("parameterModes", out var parameterModes))
                metadata["parameters"] = ParseRoutineParameters(parameterModes);

            if (values.TryGetValue("resultColumns", out var resultColumns))
                metadata["resultColumns"] = ParseRoutineResultColumns(resultColumns);

            return metadata;
        }

        private static string InferRoutineCallShape(string provider, string routineType, string? dataType)
        {
            if (!routineType.Contains("function", StringComparison.OrdinalIgnoreCase))
                return string.Empty;

            if (provider.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase)
                && (string.Equals(dataType, "record", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(dataType, "table", StringComparison.OrdinalIgnoreCase)))
            {
                return "table-valued-function";
            }

            return "scalar-function";
        }

        private static string ParseRoutineProvider(string header)
        {
            if (header.StartsWith("SQL Server ", StringComparison.OrdinalIgnoreCase)) return "SQL Server";
            if (header.StartsWith("PostgreSQL ", StringComparison.OrdinalIgnoreCase)) return "PostgreSQL";
            if (header.StartsWith("MySQL ", StringComparison.OrdinalIgnoreCase)) return "MySQL";
            var space = header.IndexOf(' ');
            return space > 0 ? header[..space] : header;
        }

        private static string ParseRoutineType(string header)
        {
            foreach (var provider in new[] { "SQL Server ", "PostgreSQL ", "MySQL " })
            {
                if (header.StartsWith(provider, StringComparison.OrdinalIgnoreCase))
                    return header[provider.Length..];
            }

            var space = header.IndexOf(' ');
            return space > 0 ? header[(space + 1)..] : header;
        }

        private static IReadOnlyDictionary<string, string> ParseRoutineSemicolonValues(string detail, out string header)
            => ScaffoldSemicolonParser.ParseRoutine(detail, out header, IsCompleteSemicolonValue);

        private static bool IsCompleteSemicolonValue(string key, string value)
        {
            if (key.Equals("parameters", StringComparison.OrdinalIgnoreCase)
                || key.Equals("outputParameters", StringComparison.OrdinalIgnoreCase))
            {
                return int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out _);
            }

            if (key.Equals("parameterModes", StringComparison.OrdinalIgnoreCase))
                return IsCompleteRoutineParameterModes(value);

            if (key.Equals("resultColumns", StringComparison.OrdinalIgnoreCase))
                return IsCompleteRoutineResultColumns(value);

            return true;
        }

        private static bool IsCompleteRoutineParameterModes(string parameterModes)
        {
            if (string.IsNullOrWhiteSpace(parameterModes))
                return true;

            var parts = SplitRoutineParameterModes(parameterModes);
            return parts.Count > 0 && parts.All(part => TryParseRoutineParameterMode(part, out _, out _, out _));
        }

        private static bool IsCompleteRoutineResultColumns(string resultColumns)
        {
            if (string.IsNullOrWhiteSpace(resultColumns))
                return true;

            var parts = SplitRoutineResultColumns(resultColumns);
            return parts.Count > 0 && parts.All(part => TryParseRoutineResultColumnParts(part, out _, out _, out _));
        }

        private static int? ParseNullableInt(string? value)
            => int.TryParse(value, out var parsed) ? parsed : null;

        private static void AddRoutineDelimitedPart(List<string> parts, string value)
        {
            var trimmed = value.Trim();
            if (!string.IsNullOrWhiteSpace(trimmed))
                parts.Add(trimmed);
        }
    }
}
