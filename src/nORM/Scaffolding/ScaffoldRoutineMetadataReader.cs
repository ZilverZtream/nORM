#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataReader
    {
        public static string FormatRoutineParameterSummary(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return string.Empty;
            }

            return string.Join(", ", parameters.Select(parameter =>
            {
                var name = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null) ?? "parameter";
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                var dataType = Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null);
                return string.Join(" ", new[] { name, mode, dataType }.Where(part => !string.IsNullOrWhiteSpace(part)));
            }));
        }
    }

    internal readonly record struct RoutineStubParameter(
        string Name,
        string TypeName,
        string DataType);

    internal readonly record struct RoutineOutputParameter(
        string Name,
        string DbType,
        int? Size,
        byte? Precision,
        byte? Scale,
        string Direction);

    internal readonly record struct RoutineResultColumn(
        string Name,
        string TypeName);
}
