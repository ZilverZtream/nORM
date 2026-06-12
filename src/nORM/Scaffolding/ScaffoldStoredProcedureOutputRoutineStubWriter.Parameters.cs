#nullable enable
using System;
using System.Data;
using System.Globalization;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoredProcedureOutputRoutineStubWriter
    {
        private static string FormatRoutineOutputParameterCreation(RoutineOutputParameter parameter)
        {
            var baseCall = $"new OutputParameter(\"{EscapeStringLiteral(parameter.Name)}\", System.Data.DbType.{parameter.DbType}";
            if (parameter.Precision.HasValue && parameter.Scale.HasValue)
            {
                baseCall += $", (byte){parameter.Precision.Value.ToString(CultureInfo.InvariantCulture)}, (byte){parameter.Scale.Value.ToString(CultureInfo.InvariantCulture)}";
                var hasNonDefaultDecimalDirection = !string.Equals(parameter.Direction, nameof(ParameterDirection.Output), StringComparison.Ordinal);
                return hasNonDefaultDecimalDirection
                    ? baseCall + ", System.Data.ParameterDirection." + parameter.Direction + ")"
                    : baseCall + ")";
            }

            if (parameter.Precision.HasValue)
            {
                return baseCall
                    + $", null, (byte){parameter.Precision.Value.ToString(CultureInfo.InvariantCulture)}, null, System.Data.ParameterDirection.{parameter.Direction}, null)";
            }

            var hasNonDefaultDirection = !string.Equals(parameter.Direction, nameof(ParameterDirection.Output), StringComparison.Ordinal);
            if (!parameter.Size.HasValue && !hasNonDefaultDirection)
                return baseCall + ")";

            var sizeArgument = parameter.Size.HasValue
                ? parameter.Size.Value.ToString(CultureInfo.InvariantCulture)
                : "null";
            if (!hasNonDefaultDirection)
                return baseCall + ", " + sizeArgument + ")";

            return baseCall + ", " + sizeArgument + ", System.Data.ParameterDirection." + parameter.Direction + ")";
        }
    }
}
