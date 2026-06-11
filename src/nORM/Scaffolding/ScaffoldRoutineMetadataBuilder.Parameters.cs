#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataBuilder
    {
        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParseRoutineParameters(string parameterModes)
        {
            if (string.IsNullOrWhiteSpace(parameterModes))
                return Array.Empty<IReadOnlyDictionary<string, object?>>();

            return SplitRoutineParameterModes(parameterModes)
                .Select(ParseRoutineParameter)
                .ToArray();
        }

        private static IReadOnlyDictionary<string, object?> ParseRoutineParameter(string raw)
        {
            if (!TryParseRoutineParameterMode(raw, out var name, out var mode, out var dataType))
            {
                var parts = raw.Split(':', 3);
                name = parts.Length > 0 ? parts[0].Trim() : string.Empty;
                mode = parts.Length > 1 ? parts[1].Trim() : string.Empty;
                dataType = parts.Length > 2 ? parts[2].Trim() : string.Empty;
            }

            var parameter = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["name"] = name,
                ["mode"] = mode
            };
            if (!string.IsNullOrWhiteSpace(dataType))
            {
                parameter["dataType"] = dataType;
                parameter["clrType"] = ScaffoldRoutineTypeMapper.GetRoutineParameterTypeName(dataType);
                parameter["dbType"] = ScaffoldRoutineTypeMapper.GetRoutineParameterDbTypeName(dataType);
            }

            return parameter;
        }

        private static bool TryParseRoutineParameterMode(
            string raw,
            out string name,
            out string mode,
            out string dataType)
        {
            name = string.Empty;
            mode = string.Empty;
            dataType = string.Empty;

            var trimmed = raw.Trim();
            if (trimmed.Length == 0)
                return false;

            for (var separator = trimmed.Length - 1; separator >= 0; separator--)
            {
                if (trimmed[separator] != ':')
                    continue;

                var modeStart = separator + 1;
                var modeEnd = trimmed.IndexOf(':', modeStart);
                var candidate = modeEnd >= 0
                    ? trimmed.Substring(modeStart, modeEnd - modeStart).Trim()
                    : trimmed[modeStart..].Trim();
                if (!IsRoutineParameterMode(candidate))
                    continue;

                name = trimmed[..separator].Trim();
                mode = candidate;
                dataType = modeEnd >= 0 ? trimmed[(modeEnd + 1)..].Trim() : string.Empty;
                return true;
            }

            return false;
        }

        private static bool IsRoutineParameterMode(string mode)
            => mode.Equals("IN", StringComparison.OrdinalIgnoreCase)
               || mode.Equals("OUT", StringComparison.OrdinalIgnoreCase)
               || mode.Equals("INOUT", StringComparison.OrdinalIgnoreCase)
               || mode.Equals("RETURN", StringComparison.OrdinalIgnoreCase);

        private static IReadOnlyList<string> SplitRoutineParameterModes(string parameterModes)
        {
            var parts = new List<string>();
            var start = 0;
            var depth = 0;
            for (var i = 0; i < parameterModes.Length; i++)
            {
                var ch = parameterModes[i];
                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')' && depth > 0)
                {
                    depth--;
                }
                else if (ch == ',' && depth == 0)
                {
                    var candidate = parameterModes[start..i];
                    if (TryParseRoutineParameterMode(candidate, out _, out _, out _))
                    {
                        AddRoutineDelimitedPart(parts, candidate);
                        start = i + 1;
                    }
                }
            }

            AddRoutineDelimitedPart(parts, parameterModes[start..]);
            return parts;
        }
    }
}
