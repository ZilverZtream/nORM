#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFunctionRoutineStubWriter
    {
        private static void AppendGenericFunctionQueryMethod(
            StringBuilder sb,
            string methodBase,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            bool scalar,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue,
            bool useNullableReferenceTypes)
        {
            sb.AppendLine($"    public Task<List<TResult>> {methodBase}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalar || scalarSetReturnsValue)
                sb.AppendLine("        return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine("        return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("    }");
        }

        private static void AppendTypedFunctionQueryMethod(
            StringBuilder sb,
            string methodBase,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            string? resultType,
            bool scalar,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue,
            bool useNullableReferenceTypes)
        {
            if (scalar || resultType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Executes provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` and materializes the scaffold-discovered result shape.</summary>");
            sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
            sb.AppendLine($"    public Task<List<{resultType}>> {methodBase}({parameterSignature}, CancellationToken ct = default)");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalarSetReturnsValue)
                sb.AppendLine($"        return QueryUnchangedAsync<{resultType}>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine($"        return QueryUnchangedAsync<{resultType}>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("    }");
        }
    }
}
