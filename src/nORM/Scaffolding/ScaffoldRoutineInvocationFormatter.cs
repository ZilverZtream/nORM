#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static class ScaffoldRoutineInvocationFormatter
    {
        public static bool IsFunctionCallShape(string? callShape)
            => string.Equals(callShape, "table-valued-function", StringComparison.OrdinalIgnoreCase)
               || string.Equals(callShape, "scalar-function", StringComparison.OrdinalIgnoreCase);

        public static string FormatProviderEscapedRoutineName(ScaffoldRoutineStubInfo routine)
        {
            var name = EscapeStringLiteral(routine.Name);
            if (string.IsNullOrWhiteSpace(routine.Schema))
                return $"Provider.Escape(\"{name}\")";

            var schema = EscapeStringLiteral(routine.Schema!);
            return $"Provider.Escape(\"{schema}\") + \".\" + Provider.Escape(\"{name}\")";
        }

        public static string QualifiedRoutineName(ScaffoldRoutineStubInfo routine)
            => string.IsNullOrWhiteSpace(routine.Schema) ? routine.Name : routine.Schema + "." + routine.Name;

        public static string FormatStoredProcedureParameterArgument(ScaffoldRoutineStubInfo routine, int expectedInputParameterCount)
        {
            if (expectedInputParameterCount <= 0)
                return "parameters";

            return $"RequireScaffoldedRoutineParameters(parameters, {expectedInputParameterCount.ToString(CultureInfo.InvariantCulture)}, {FormatProviderEscapedRoutineName(routine)})";
        }

        public static string FormatRoutineArgumentArray(
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            bool usePositionalArguments = false,
            bool useNullableReferenceTypes = true)
        {
            if (usePositionalArguments)
                return "        var args = arguments is null ? System.Array.Empty<object>() : System.Array.ConvertAll(arguments, value => (object)(value ?? System.DBNull.Value));";

            if (parameterType is null || inputParameters.Count == 0)
                return "        var args = System.Array.Empty<object>();";

            var objectCast = useNullableReferenceTypes ? "object?" : "object";
            var values = inputParameters
                .Select(parameter => $"({objectCast})parameters.{parameter.Name} ?? System.DBNull.Value")
                .ToArray();
            return "        var args = parameters is null ? System.Array.Empty<object>() : new object[] { " + string.Join(", ", values) + " };";
        }

        public static void AppendFunctionArgumentCountGuard(
            StringBuilder sb,
            ScaffoldRoutineStubInfo routine,
            int expectedArgumentCount)
        {
            if (expectedArgumentCount <= 0)
                return;

            sb.AppendLine($"        if (args.Length != {expectedArgumentCount.ToString(CultureInfo.InvariantCulture)})");
            sb.AppendLine($"            throw new NormConfigurationException(\"Function `{EscapeStringLiteral(QualifiedRoutineName(routine))}` was scaffolded with {expectedArgumentCount.ToString(CultureInfo.InvariantCulture)} input parameters; pass exactly {expectedArgumentCount.ToString(CultureInfo.InvariantCulture)} arguments in scaffolded order.\");");
        }

        public static void AppendFunctionPlaceholderLine(
            StringBuilder sb,
            ScaffoldRoutineStubInfo routine,
            IReadOnlyList<string> inputParameterDataTypes,
            int expectedArgumentCount)
        {
            var postgresCastTypes = ScaffoldRoutineTypeMapper.GetPostgresFunctionArgumentCastTypes(routine, inputParameterDataTypes, expectedArgumentCount);
            if (postgresCastTypes.Count == 0)
            {
                sb.AppendLine("        var placeholders = string.Join(\", \", System.Linq.Enumerable.Range(0, args.Length).Select(i => Provider.ParamPrefix + \"p\" + i));");
                return;
            }

            sb.AppendLine("        var casts = new[] { " + string.Join(", ", postgresCastTypes.Select(type => $"\"{EscapeStringLiteral(type)}\"")) + " };");
            sb.AppendLine("        var placeholders = string.Join(\", \", System.Linq.Enumerable.Range(0, args.Length).Select(i => Provider.ParamPrefix + \"p\" + i + \"::\" + casts[i]));");
        }
    }
}
