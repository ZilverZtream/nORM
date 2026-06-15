#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFunctionRoutineStubWriter
    {
        private static void AppendGenericFunctionStreamMethod(
            StringBuilder sb,
            string streamMethod,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue,
            bool useNullableReferenceTypes)
        {
            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Streams provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows without buffering the full result set.</summary>");
            sb.AppendLine("    /// <remarks>Routine bodies are provider-owned and are not translated by nORM.</remarks>");
            sb.AppendLine($"    public async IAsyncEnumerable<TResult> {streamMethod}<TResult>({parameterSignature}, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalarSetReturnsValue)
                sb.AppendLine("        var rows = QueryUnchangedStreamAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine("        var rows = QueryUnchangedStreamAsync<TResult>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("        await foreach (var row in rows.ConfigureAwait(false))");
            sb.AppendLine("            yield return row;");
            sb.AppendLine("    }");
        }
    }
}
