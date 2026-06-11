#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataReader
    {
        public static IReadOnlyList<RoutineStubParameter> GetRoutineInputParameters(
            IReadOnlyDictionary<string, object?> metadata,
            bool useNullableReferenceTypes)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return Array.Empty<RoutineStubParameter>();
            }

            var names = new List<RoutineStubParameter>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var rawName = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null);
                var normalized = NormalizeRoutineParameterName(rawName);
                if (string.IsNullOrWhiteSpace(normalized))
                    return Array.Empty<RoutineStubParameter>();

                var escaped = ScaffoldNameHelper.EscapeCSharpIdentifier(normalized);
                if (!string.Equals(escaped.TrimStart('@'), normalized, StringComparison.Ordinal))
                    return Array.Empty<RoutineStubParameter>();

                if (!usedNames.Add(escaped))
                    return Array.Empty<RoutineStubParameter>();

                var dataType = Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null);
                names.Add(new RoutineStubParameter(escaped, ScaffoldRoutineTypeMapper.GetRoutineParameterTypeName(dataType, useNullableReferenceTypes), dataType ?? string.Empty));
            }

            return names.ToArray();
        }

        public static IReadOnlyList<string> GetRoutineInputParameterDataTypes(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return Array.Empty<string>();
            }

            var dataTypes = new List<string>();
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                dataTypes.Add(Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null) ?? string.Empty);
            }

            return dataTypes.ToArray();
        }

        public static int GetRoutineInputParameterCount(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return 0;
            }

            var count = 0;
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (!string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    count++;
                }
            }

            return count;
        }

        public static IReadOnlyList<RoutineOutputParameter> GetRoutineOutputParameters(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return Array.Empty<RoutineOutputParameter>();
            }

            var names = new List<RoutineOutputParameter>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (!string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(mode, "INOUT", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var rawName = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null);
                var normalized = NormalizeRoutineParameterName(rawName);
                if (string.IsNullOrWhiteSpace(normalized))
                    return Array.Empty<RoutineOutputParameter>();

                var escaped = ScaffoldNameHelper.EscapeCSharpIdentifier(normalized);
                if (!string.Equals(escaped.TrimStart('@'), normalized, StringComparison.Ordinal))
                    return Array.Empty<RoutineOutputParameter>();

                if (!usedNames.Add(normalized))
                    return Array.Empty<RoutineOutputParameter>();

                var dataType = Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null);
                var (precision, scale) = ScaffoldRoutineTypeMapper.GetRoutineParameterPrecisionScale(dataType);
                names.Add(new RoutineOutputParameter(
                    normalized,
                    ScaffoldRoutineTypeMapper.GetRoutineParameterDbTypeName(dataType),
                    ScaffoldRoutineTypeMapper.GetRoutineParameterSize(dataType),
                    precision,
                    scale,
                    ScaffoldRoutineTypeMapper.GetRoutineParameterDirection(mode)));
            }

            return names.ToArray();
        }

        private static string NormalizeRoutineParameterName(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return string.Empty;

            return name.Trim().TrimStart('@', ':', '?');
        }
    }
}
