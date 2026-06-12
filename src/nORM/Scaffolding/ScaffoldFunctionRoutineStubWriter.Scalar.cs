#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFunctionRoutineStubWriter
    {
        private static void AppendScalarFunctionValueMethod(
            StringBuilder sb,
            string? scalarValueMethod,
            string? scalarValueType,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            bool scalar,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool useNullableReferenceTypes)
        {
            if (!scalar || scalarValueType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    private sealed class {scalarValueType}<TValue>");
            sb.AppendLine("    {");
            sb.AppendLine(useNullableReferenceTypes
                ? "        public TValue? Value { get; set; }"
                : "        public TValue Value { get; set; }");
            sb.AppendLine("    }");
            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Executes provider-bound scalar function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` and returns its single value.</summary>");
            sb.AppendLine("    /// <remarks>The routine body is provider-owned and is not translated by nORM.</remarks>");
            sb.AppendLine($"    public async Task<TValue{(useNullableReferenceTypes ? "?" : string.Empty)}> {scalarValueMethod}<TValue>({parameterSignature}, CancellationToken ct = default)");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            sb.AppendLine($"        var rows = await QueryUnchangedAsync<{scalarValueType}<TValue>>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args).ConfigureAwait(false);");
            sb.AppendLine("        return rows.Count == 0 ? default : rows[0].Value;");
            sb.AppendLine("    }");
        }
    }
}
