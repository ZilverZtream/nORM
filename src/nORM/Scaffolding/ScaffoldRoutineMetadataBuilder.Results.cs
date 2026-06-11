#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataBuilder
    {
        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParseRoutineResultColumns(string resultColumns)
        {
            if (string.IsNullOrWhiteSpace(resultColumns))
                return Array.Empty<IReadOnlyDictionary<string, object?>>();

            return SplitRoutineResultColumns(resultColumns)
                .Select(ParseRoutineResultColumn)
                .ToArray();
        }

        private static IReadOnlyDictionary<string, object?> ParseRoutineResultColumn(string raw)
        {
            string name;
            string dataType;
            bool? isNullable = null;
            if (TryParseRoutineResultColumnParts(raw, out name, out dataType, out var nullable))
            {
                isNullable = nullable;
            }
            else
            {
                var parts = raw.Split(':', 3);
                name = parts.Length > 0 ? parts[0].Trim() : string.Empty;
                dataType = parts.Length > 1 ? parts[1].Trim() : string.Empty;
                if (parts.Length > 2 && int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNullable))
                    isNullable = parsedNullable != 0;
            }

            var column = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["name"] = name,
                ["dataType"] = dataType
            };
            if (isNullable.HasValue)
                column["nullable"] = isNullable.Value;
            return column;
        }

        private static bool TryParseRoutineResultColumnParts(
            string raw,
            out string name,
            out string dataType,
            out bool nullable)
        {
            name = string.Empty;
            dataType = string.Empty;
            nullable = false;

            var trimmed = raw.Trim();
            var nullableSeparator = trimmed.LastIndexOf(':');
            if (nullableSeparator <= 0
                || !int.TryParse(trimmed[(nullableSeparator + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedNullable))
            {
                return false;
            }

            var typeSeparator = trimmed.LastIndexOf(':', nullableSeparator - 1);
            if (typeSeparator < 0)
                return false;

            name = trimmed[..typeSeparator].Trim();
            dataType = trimmed.Substring(typeSeparator + 1, nullableSeparator - typeSeparator - 1).Trim();
            nullable = parsedNullable != 0;
            return true;
        }

        private static IReadOnlyList<string> SplitRoutineResultColumns(string resultColumns)
        {
            var parts = new List<string>();
            var start = 0;
            for (var i = 0; i < resultColumns.Length; i++)
            {
                if (resultColumns[i] != '|')
                    continue;

                var candidate = resultColumns[start..i];
                if (TryParseRoutineResultColumnParts(candidate, out _, out _, out _))
                {
                    AddRoutineDelimitedPart(parts, candidate);
                    start = i + 1;
                }
            }

            AddRoutineDelimitedPart(parts, resultColumns[start..]);
            return parts;
        }
    }
}
