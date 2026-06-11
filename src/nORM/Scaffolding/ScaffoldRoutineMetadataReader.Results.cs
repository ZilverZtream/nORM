#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineMetadataReader
    {
        public static IReadOnlyList<RoutineResultColumn> GetRoutineResultColumns(
            IReadOnlyDictionary<string, object?> metadata,
            bool useNullableReferenceTypes,
            bool useDatabaseNames)
        {
            if (!TryGetRoutineResultColumnMetadata(metadata, out var columns))
            {
                return Array.Empty<RoutineResultColumn>();
            }

            var result = new List<RoutineResultColumn>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                var rawName = Convert.ToString(column.TryGetValue("name", out var n) ? n : null);
                var escaped = string.IsNullOrWhiteSpace(rawName)
                    ? ScaffoldNameHelper.EscapeCSharpIdentifier("Column" + (i + 1).ToString(CultureInfo.InvariantCulture))
                    : ScaffoldNameHelper.ToScaffoldClrName(rawName, useDatabaseNames);
                var unique = escaped;
                var suffix = 2;
                while (!usedNames.Add(unique))
                    unique = escaped + suffix++.ToString(CultureInfo.InvariantCulture);

                var dataType = Convert.ToString(column.TryGetValue("dataType", out var d) ? d : null);
                var nullable = column.TryGetValue("nullable", out var isNullable) && isNullable is true;
                result.Add(new RoutineResultColumn(unique, ScaffoldRoutineTypeMapper.GetRoutineResultColumnTypeName(dataType, nullable, useNullableReferenceTypes)));
            }

            return result.ToArray();
        }

        public static bool TryGetScalarSetReturningRoutineResultColumn(
            IReadOnlyDictionary<string, object?> metadata,
            bool useNullableReferenceTypes,
            out RoutineResultColumn column)
        {
            column = default;
            var callShape = Convert.ToString(metadata.TryGetValue("callShape", out var shape) ? shape : null);
            if (!string.Equals(callShape, "table-valued-function", StringComparison.OrdinalIgnoreCase))
                return false;

            if (TryGetRoutineResultColumnMetadata(metadata, out var discoveredColumns)
                && discoveredColumns.Count > 0)
            {
                return false;
            }

            var dataType = Convert.ToString(metadata.TryGetValue("dataType", out var d) ? d : null);
            if (string.IsNullOrWhiteSpace(dataType))
                return false;

            if (ScaffoldRoutineTypeMapper.IsNonScalarRoutineResultDataType(dataType))
                return false;

            var typeName = ScaffoldRoutineTypeMapper.GetRoutineResultColumnTypeName(dataType, nullable: false, useNullableReferenceTypes);
            if (string.Equals(typeName, useNullableReferenceTypes ? "object?" : "object", StringComparison.Ordinal))
                return false;

            column = new RoutineResultColumn("Value", typeName);
            return true;
        }

        private static bool TryGetRoutineResultColumnMetadata(
            IReadOnlyDictionary<string, object?> metadata,
            out IReadOnlyList<IReadOnlyDictionary<string, object?>> columns)
        {
            if (metadata.TryGetValue("resultColumns", out var columnsValue)
                && columnsValue is IReadOnlyList<IReadOnlyDictionary<string, object?>> resultColumns
                && resultColumns.Count > 0)
            {
                columns = resultColumns;
                return true;
            }

            var callShape = Convert.ToString(metadata.TryGetValue("callShape", out var shape) ? shape : null);
            if (!string.Equals(callShape, "table-valued-function", StringComparison.OrdinalIgnoreCase)
                || !metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                columns = Array.Empty<IReadOnlyDictionary<string, object?>>();
                return false;
            }

            columns = parameters
                .Where(parameter =>
                {
                    var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                    var name = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null);
                    return !string.IsNullOrWhiteSpace(name)
                           && !string.Equals(name, "return", StringComparison.OrdinalIgnoreCase)
                           && (string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                               || string.Equals(mode, "INOUT", StringComparison.OrdinalIgnoreCase));
                })
                .ToArray();
            return columns.Count > 0;
        }
    }
}
