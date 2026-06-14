#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataReader
    {
        public static IReadOnlyList<RoutineStubParameter> GetRoutineInputParameters(
            IReadOnlyDictionary<string, object?> metadata,
            bool useNullableReferenceTypes)
        {
            var names = new List<RoutineStubParameter>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in GetRoutineParameterMetadata(metadata))
            {
                if (!IsRoutineInputParameter(parameter))
                    continue;

                if (!TryGetSafeRoutineParameterName(parameter, out _, out var escaped))
                    return Array.Empty<RoutineStubParameter>();

                if (!usedNames.Add(escaped))
                    return Array.Empty<RoutineStubParameter>();

                var dataType = GetRoutineParameterDataType(parameter);
                names.Add(new RoutineStubParameter(escaped, ScaffoldRoutineTypeMapper.GetRoutineParameterTypeName(dataType, useNullableReferenceTypes), dataType ?? string.Empty));
            }

            return names.ToArray();
        }

        public static IReadOnlyList<string> GetRoutineInputParameterDataTypes(IReadOnlyDictionary<string, object?> metadata)
            => GetRoutineParameterMetadata(metadata)
                .Where(IsRoutineInputParameter)
                .Select(parameter => GetRoutineParameterDataType(parameter) ?? string.Empty)
                .ToArray();

        public static int GetRoutineInputParameterCount(IReadOnlyDictionary<string, object?> metadata)
            => GetRoutineParameterMetadata(metadata).Count(IsRoutineInputParameter);

        public static IReadOnlyList<RoutineOutputParameter> GetRoutineOutputParameters(IReadOnlyDictionary<string, object?> metadata)
        {
            var names = new List<RoutineOutputParameter>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in GetRoutineParameterMetadata(metadata))
            {
                var mode = GetRoutineParameterMode(parameter);
                if (!IsRoutineOutputMode(mode))
                    continue;

                if (!TryGetSafeRoutineParameterName(parameter, out var normalized, out _))
                    return Array.Empty<RoutineOutputParameter>();

                if (!usedNames.Add(normalized))
                    return Array.Empty<RoutineOutputParameter>();

                var dataType = GetRoutineParameterDataType(parameter);
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

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> GetRoutineParameterMetadata(
            IReadOnlyDictionary<string, object?> metadata)
            => metadata.TryGetValue("parameters", out var parametersValue)
               && parametersValue is IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                ? parameters
                : Array.Empty<IReadOnlyDictionary<string, object?>>();

        private static bool IsRoutineInputParameter(IReadOnlyDictionary<string, object?> parameter)
            => IsRoutineInputMode(GetRoutineParameterMode(parameter));

        private static bool IsRoutineInputMode(string? mode)
            => !string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
               && !string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase);

        private static bool IsRoutineOutputMode(string? mode)
            => string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
               || string.Equals(mode, "INOUT", StringComparison.OrdinalIgnoreCase)
               || string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase);

        private static string? GetRoutineParameterDataType(IReadOnlyDictionary<string, object?> parameter)
            => Convert.ToString(parameter.TryGetValue("dataType", out var dataType) ? dataType : null);

        private static string? GetRoutineParameterMode(IReadOnlyDictionary<string, object?> parameter)
            => Convert.ToString(parameter.TryGetValue("mode", out var mode) ? mode : null);

        private static bool TryGetSafeRoutineParameterName(
            IReadOnlyDictionary<string, object?> parameter,
            out string normalized,
            out string escaped)
        {
            var rawName = Convert.ToString(parameter.TryGetValue("name", out var name) ? name : null);
            normalized = NormalizeRoutineParameterName(rawName);
            if (string.IsNullOrWhiteSpace(normalized))
            {
                escaped = string.Empty;
                return false;
            }

            escaped = ScaffoldNameHelper.EscapeCSharpIdentifier(normalized);
            return string.Equals(escaped.TrimStart('@'), normalized, StringComparison.Ordinal);
        }

        private static string NormalizeRoutineParameterName(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return string.Empty;

            return name.Trim().TrimStart('@', ':', '?');
        }
    }
}
