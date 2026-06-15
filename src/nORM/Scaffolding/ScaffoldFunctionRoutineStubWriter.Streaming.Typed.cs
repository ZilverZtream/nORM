#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFunctionRoutineStubWriter
    {
        private static void AppendTypedFunctionStreamMethod(
            StringBuilder sb,
            string streamMethod,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            string? resultType,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue,
            bool useNullableReferenceTypes)
        {
            if (resultType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Streams provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows using the scaffold-discovered result shape.</summary>");
            sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
            sb.AppendLine($"    public async IAsyncEnumerable<{resultType}> {streamMethod}({parameterSignature}, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalarSetReturnsValue)
                sb.AppendLine($"        var rows = QueryUnchangedStreamAsync<{resultType}>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine($"        var rows = QueryUnchangedStreamAsync<{resultType}>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("        await foreach (var row in rows.ConfigureAwait(false))");
            sb.AppendLine("            yield return row;");
            sb.AppendLine("    }");
        }
    }
}
